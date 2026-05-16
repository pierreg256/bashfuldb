use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use argon2::{
    Argon2,
    password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
};
use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use bashfuldb_clock::Clock;
use bashfuldb_storage::{ColumnFamily, StorageEngine, WriteBatch};
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, encode};
use rand::rngs::OsRng;
use rsa::pkcs8::{EncodePrivateKey, EncodePublicKey};
use rsa::traits::PublicKeyParts;
use rsa::RsaPrivateKey;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;
use zeroize::{Zeroize, Zeroizing};

use crate::{
    AuthError, Authenticator, BOOTSTRAP_ADMIN_USER, Claims, Credentials, DEFAULT_ACCESS_TTL,
    DEFAULT_REFRESH_TTL, Result, Role, TokenPair,
};

const USER_KEY_PREFIX: &str = "auth/users/";
const ROLE_KEY_PREFIX: &str = "auth/roles/";
const DEFAULT_BOOTSTRAP_ENV: &str = "BASHFULDB_BOOTSTRAP_ADMIN_PASSWORD";
type ScanIter =
    Box<dyn Iterator<Item = bashfuldb_storage::Result<bashfuldb_storage::ScanItem>> + Send + 'static>;

/// JWT mode for signing and verification.
#[derive(Debug, Clone)]
pub enum JwtMode {
    /// Production mode using RS256 with generated RSA-2048 key pair.
    Rs256,
    /// Development fallback using HS256 and a 32-byte secret.
    Hs256 { secret: [u8; 32] },
}

/// Auth subsystem runtime configuration.
#[derive(Debug, Clone)]
pub struct AuthConfig {
    /// Token signing mode.
    pub jwt_mode: JwtMode,
    /// Access token lifetime.
    pub access_ttl: Duration,
    /// Refresh token lifetime.
    pub refresh_ttl: Duration,
    /// Bootstrap admin password (optional when users already exist).
    pub bootstrap_admin_password: Option<String>,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            jwt_mode: JwtMode::Rs256,
            access_ttl: DEFAULT_ACCESS_TTL,
            refresh_ttl: DEFAULT_REFRESH_TTL,
            bootstrap_admin_password: std::env::var(DEFAULT_BOOTSTRAP_ENV).ok(),
        }
    }
}

impl Drop for AuthConfig {
    fn drop(&mut self) {
        if let Some(password) = &mut self.bootstrap_admin_password {
            password.zeroize();
        }
        if let JwtMode::Hs256 { secret } = &mut self.jwt_mode {
            secret.zeroize();
        }
    }
}

/// User create request used by bootstrap and tests.
#[derive(Debug, Clone)]
pub struct UserCreateRequest {
    /// Optional explicit user ID. If omitted, a UUID is generated.
    pub user_id: Option<String>,
    /// Login username.
    pub username: String,
    /// Tenant for this user (`None` for `ServerAdmin` users).
    pub tenant: Option<String>,
    /// Plaintext password. It is hashed with Argon2id before storage.
    pub password: String,
    /// Assigned roles.
    pub roles: Vec<Role>,
}

impl Drop for UserCreateRequest {
    fn drop(&mut self) {
        self.password.zeroize();
    }
}

/// Concrete authenticator implementation with in-memory token revocation.
pub struct AuthService {
    storage: Arc<dyn StorageEngine>,
    clock: Arc<dyn Clock>,
    signer: Signer,
    access_ttl: Duration,
    refresh_ttl: Duration,
    revoked: Mutex<HashSet<String>>,
}

impl AuthService {
    /// Creates a new auth service and performs bootstrap-admin initialization.
    pub async fn new(
        storage: Arc<dyn StorageEngine>,
        clock: Arc<dyn Clock>,
        mut config: AuthConfig,
    ) -> Result<Self> {
        let signer = Signer::new(config.jwt_mode.clone())?;
        let service = Self {
            storage,
            clock,
            signer,
            access_ttl: config.access_ttl,
            refresh_ttl: config.refresh_ttl,
            revoked: Mutex::new(HashSet::new()),
        };

        service
            .bootstrap_if_needed(config.bootstrap_admin_password.take())
            .await?;
        Ok(service)
    }

    /// Creates and persists a user and role assignment.
    pub async fn create_user(&self, mut request: UserCreateRequest) -> Result<String> {
        if request.roles.is_empty() {
            return Err(AuthError::Internal {
                message: "user must have at least one role".to_string(),
            });
        }

        if request.password.len() < 8 {
            return Err(AuthError::WeakPassword {
                reason: "password must be at least 8 characters".to_string(),
            });
        }

        if request
            .roles
            .iter()
            .any(|role| matches!(role, Role::ServerAdmin))
        {
            request.tenant = None;
        } else if request.tenant.is_none() {
            return Err(AuthError::Internal {
                message: "tenant-scoped users must provide tenant".to_string(),
            });
        }

        if let Some(tenant) = request.tenant.as_deref()
            && self.non_server_admin_exists_for_tenant(tenant).await?
        {
            return Err(AuthError::UserAlreadyExists {
                username: format!("tenant:{tenant}"),
            });
        }

        if self.find_user_by_username(&request.username).await?.is_some() {
            return Err(AuthError::UserAlreadyExists {
                username: request.username.clone(),
            });
        }

        let user_id = request
            .user_id
            .clone()
            .unwrap_or_else(|| Uuid::new_v4().to_string());

        let password_hash = hash_password(&request.password)?;
        let user = StoredUser {
            user_id: user_id.clone(),
            username: request.username.clone(),
            tenant: request.tenant.clone(),
            password_hash,
        };
        let roles = StoredRoles {
            roles: request.roles.clone(),
        };

        self.persist_user_and_roles(&user, &roles).await?;
        Ok(user_id)
    }

    /// Returns the JWKS object for public key distribution.
    pub fn jwks(&self) -> serde_json::Value {
        self.signer.jwks()
    }

    async fn bootstrap_if_needed(&self, bootstrap_password: Option<String>) -> Result<()> {
        if self.any_user_exists()? {
            return Ok(());
        }

        let password = bootstrap_password.ok_or_else(|| AuthError::Internal {
            message: format!(
                "bootstrap password required via env/config ({DEFAULT_BOOTSTRAP_ENV})"
            ),
        })?;

        let req = UserCreateRequest {
            user_id: Some(BOOTSTRAP_ADMIN_USER.to_string()),
            username: BOOTSTRAP_ADMIN_USER.to_string(),
            tenant: None,
            password,
            roles: vec![Role::ServerAdmin],
        };
        let _ = self.create_user(req).await?;
        Ok(())
    }

    fn any_user_exists(&self) -> Result<bool> {
        let mut iter = self.scan_prefix(USER_KEY_PREFIX)?;
        if let Some(item) = iter.next() {
            let _ = item.map_err(map_storage_error)?;
            return Ok(true);
        }
        Ok(false)
    }

    async fn persist_user_and_roles(&self, user: &StoredUser, roles: &StoredRoles) -> Result<()> {
        let user_value = serde_json::to_vec(user).map_err(|err| AuthError::Internal {
            message: format!("failed to serialize user: {err}"),
        })?;
        let role_value = serde_json::to_vec(roles).map_err(|err| AuthError::Internal {
            message: format!("failed to serialize roles: {err}"),
        })?;

        let mut batch = WriteBatch::new();
        batch.put(
            ColumnFamily::SYSTEM,
            user_key(&user.user_id).into_bytes(),
            user_value,
        );
        batch.put(
            ColumnFamily::SYSTEM,
            role_key(&user.user_id).into_bytes(),
            role_value,
        );
        self.storage
            .write_batch(batch)
            .await
            .map_err(map_storage_error)
    }

    async fn find_user_by_id(&self, user_id: &str) -> Result<Option<StoredUser>> {
        let key = user_key(user_id);
        let bytes = self
            .storage
            .get(ColumnFamily::SYSTEM, key.as_bytes())
            .await
            .map_err(map_storage_error)?;
        match bytes {
            Some(value) => deserialize_json::<StoredUser>(&value, "stored user").map(Some),
            None => Ok(None),
        }
    }

    async fn find_user_by_username(&self, username: &str) -> Result<Option<StoredUser>> {
        let iter = self.scan_prefix(USER_KEY_PREFIX)?;
        for item in iter {
            let (_, value) = item.map_err(map_storage_error)?;
            let user = deserialize_json::<StoredUser>(&value, "stored user")?;
            if user.username == username {
                return Ok(Some(user));
            }
        }
        Ok(None)
    }

    async fn get_roles(&self, user_id: &str) -> Result<Vec<Role>> {
        let key = role_key(user_id);
        let bytes = self
            .storage
            .get(ColumnFamily::SYSTEM, key.as_bytes())
            .await
            .map_err(map_storage_error)?;
        let value = bytes.ok_or_else(|| AuthError::UserNotFound {
            username: user_id.to_string(),
        })?;
        let stored = deserialize_json::<StoredRoles>(&value, "stored roles")?;
        Ok(stored.roles)
    }

    async fn non_server_admin_exists_for_tenant(&self, tenant: &str) -> Result<bool> {
        let iter = self.scan_prefix(USER_KEY_PREFIX)?;
        for item in iter {
            let (_, value) = item.map_err(map_storage_error)?;
            let user = deserialize_json::<StoredUser>(&value, "stored user")?;
            if user.tenant.as_deref() != Some(tenant) {
                continue;
            }
            let roles = self.get_roles(&user.user_id).await?;
            if !roles.iter().any(|role| matches!(role, Role::ServerAdmin)) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn now_secs(&self) -> Result<u64> {
        self.clock
            .tick()
            .map(|hlc| hlc.physical_ms() / 1_000)
            .map_err(|err| AuthError::Internal {
                message: format!("clock error: {err}"),
            })
    }

    fn issue_token_pair(&self, claims: &Claims) -> Result<TokenPair> {
        let now = self.now_secs()?;
        let access_claims = JwtClaims::from_claims(
            Claims::new(
                claims.sub().to_string(),
                claims.tenant().map(ToOwned::to_owned),
                claims.roles().to_vec(),
                now,
                now.saturating_add(self.access_ttl.as_secs()),
            ),
            TokenKind::Access,
        );
        let refresh_claims = JwtClaims::from_claims(
            Claims::new(
                claims.sub().to_string(),
                claims.tenant().map(ToOwned::to_owned),
                claims.roles().to_vec(),
                now,
                now.saturating_add(self.refresh_ttl.as_secs()),
            ),
            TokenKind::Refresh,
        );

        let access_token = self.signer.encode(&access_claims)?;
        let refresh_token = self.signer.encode(&refresh_claims)?;
        Ok(TokenPair {
            access_token,
            refresh_token,
            access_ttl: self.access_ttl,
            refresh_ttl: self.refresh_ttl,
        })
    }

    fn check_revoked(&self, token: &str) -> Result<()> {
        let digest = token_digest(token);
        let revoked = self
            .revoked
            .lock()
            .map_err(|_| AuthError::Internal {
                message: "revocation lock poisoned".to_string(),
            })?;
        if revoked.contains(&digest) {
            return Err(AuthError::TokenRevoked);
        }
        Ok(())
    }

    fn revoke(&self, token: &str) -> Result<()> {
        let digest = token_digest(token);
        let mut revoked = self
            .revoked
            .lock()
            .map_err(|_| AuthError::Internal {
                message: "revocation lock poisoned".to_string(),
            })?;
        revoked.insert(digest);
        Ok(())
    }

    fn decode_token(&self, token: &str, expected: TokenKind) -> Result<Claims> {
        self.check_revoked(token)?;
        let decoded = self.signer.decode(token)?;
        if decoded.token_kind != expected {
            return Err(AuthError::InvalidToken {
                reason: "unexpected token kind".to_string(),
            });
        }
        let claims = decoded.into_claims();
        if claims.is_expired(self.now_secs()?) {
            return Err(AuthError::TokenExpired);
        }
        Ok(claims)
    }

    fn scan_prefix(&self, prefix: &str) -> Result<ScanIter> {
        let start = prefix.as_bytes();
        let end = prefix_end(prefix.as_bytes());
        self.storage
            .scan(ColumnFamily::SYSTEM, start, &end)
            .map_err(map_storage_error)
    }
}

#[async_trait]
impl Authenticator for AuthService {
    async fn login(&self, credentials: &Credentials) -> Result<TokenPair> {
        let user = self
            .find_user_by_username(&credentials.username)
            .await?
            .ok_or(AuthError::InvalidCredentials)?;

        let password = Zeroizing::new(credentials.password.clone());
        let parsed_hash = PasswordHash::new(&user.password_hash).map_err(|_| AuthError::InvalidCredentials)?;
        Argon2::default()
            .verify_password(password.as_bytes(), &parsed_hash)
            .map_err(|_| AuthError::InvalidCredentials)?;

        let roles = self.get_roles(&user.user_id).await?;
        let now = self.now_secs()?;
        let claims = Claims::new(user.user_id, user.tenant, roles, now, now);
        self.issue_token_pair(&claims)
    }

    async fn refresh(&self, refresh_token: &str) -> Result<TokenPair> {
        let claims = self.decode_token(refresh_token, TokenKind::Refresh)?;
        let user = self
            .find_user_by_id(claims.sub())
            .await?
            .ok_or_else(|| AuthError::UserNotFound {
                username: claims.sub().to_string(),
            })?;
        let roles = self.get_roles(&user.user_id).await?;
        let new_claims = Claims::new(
            user.user_id,
            user.tenant,
            roles,
            self.now_secs()?,
            self.now_secs()?,
        );
        self.issue_token_pair(&new_claims)
    }

    async fn verify(&self, access_token: &str) -> Result<Claims> {
        self.decode_token(access_token, TokenKind::Access)
    }

    async fn logout(&self, access_token: &str) -> Result<()> {
        self.revoke(access_token)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum TokenKind {
    Access,
    Refresh,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct JwtClaims {
    sub: String,
    tenant: Option<String>,
    roles: Vec<Role>,
    iat: u64,
    exp: u64,
    token_kind: TokenKind,
}

impl JwtClaims {
    fn from_claims(claims: Claims, token_kind: TokenKind) -> Self {
        Self {
            sub: claims.sub().to_string(),
            tenant: claims.tenant().map(ToOwned::to_owned),
            roles: claims.roles().to_vec(),
            iat: claims.iat(),
            exp: claims.exp(),
            token_kind,
        }
    }

    fn into_claims(self) -> Claims {
        Claims::new(self.sub, self.tenant, self.roles, self.iat, self.exp)
    }
}

enum Signer {
    Rs256 {
        encoding_key: EncodingKey,
        decoding_key: DecodingKey,
        kid: String,
        n: String,
        e: String,
    },
    Hs256 {
        secret: [u8; 32],
    },
}

impl Signer {
    fn new(mode: JwtMode) -> Result<Self> {
        match mode {
            JwtMode::Rs256 => {
                let mut rng = OsRng;
                let key = RsaPrivateKey::new(&mut rng, 2048).map_err(|err| AuthError::Internal {
                    message: format!("failed to generate RSA keypair: {err}"),
                })?;
                let public = key.to_public_key();

                let private_der = key.to_pkcs8_der().map_err(|err| AuthError::Internal {
                    message: format!("failed to serialize RSA private key: {err}"),
                })?;
                let public_der = public
                    .to_public_key_der()
                    .map_err(|err| AuthError::Internal {
                        message: format!("failed to serialize RSA public key: {err}"),
                    })?;

                let n = URL_SAFE_NO_PAD.encode(public.n().to_bytes_be());
                let e = URL_SAFE_NO_PAD.encode(public.e().to_bytes_be());
                let mut hasher = Sha256::new();
                hasher.update(n.as_bytes());
                hasher.update(e.as_bytes());
                let kid = URL_SAFE_NO_PAD.encode(hasher.finalize());

                let encoding_key = EncodingKey::from_rsa_der(private_der.as_bytes());
                let decoding_key = DecodingKey::from_rsa_der(public_der.as_bytes());

                Ok(Self::Rs256 {
                    encoding_key,
                    decoding_key,
                    kid,
                    n,
                    e,
                })
            }
            JwtMode::Hs256 { secret } => Ok(Self::Hs256 { secret }),
        }
    }

    fn encode(&self, claims: &JwtClaims) -> Result<String> {
        let mut header = Header::new(match self {
            Self::Rs256 { .. } => Algorithm::RS256,
            Self::Hs256 { .. } => Algorithm::HS256,
        });
        if let Self::Rs256 { kid, .. } = self {
            header.kid = Some(kid.clone());
        }

        match self {
            Self::Rs256 { encoding_key, .. } => {
                encode(&header, claims, encoding_key).map_err(|err| AuthError::InvalidToken {
                    reason: err.to_string(),
                })
            }
            Self::Hs256 { secret } => {
                let key = EncodingKey::from_secret(secret.as_slice());
                encode(&header, claims, &key).map_err(|err| AuthError::InvalidToken {
                    reason: err.to_string(),
                })
            }
        }
    }

    fn decode(&self, token: &str) -> Result<JwtClaims> {
        let mut validation = Validation::new(match self {
            Self::Rs256 { .. } => Algorithm::RS256,
            Self::Hs256 { .. } => Algorithm::HS256,
        });
        validation.validate_exp = false;

        let result = match self {
            Self::Rs256 { decoding_key, .. } => decode::<JwtClaims>(token, decoding_key, &validation),
            Self::Hs256 { secret } => {
                let key = DecodingKey::from_secret(secret.as_slice());
                decode::<JwtClaims>(token, &key, &validation)
            }
        };

        result
            .map(|token_data| token_data.claims)
            .map_err(|err| AuthError::InvalidToken {
                reason: err.to_string(),
            })
    }

    fn jwks(&self) -> serde_json::Value {
        match self {
            Self::Rs256 { kid, n, e, .. } => serde_json::json!({
                "keys": [
                    {
                        "kty": "RSA",
                        "alg": "RS256",
                        "use": "sig",
                        "kid": kid,
                        "n": n,
                        "e": e
                    }
                ]
            }),
            Self::Hs256 { .. } => serde_json::json!({ "keys": [] }),
        }
    }
}

impl Drop for Signer {
    fn drop(&mut self) {
        if let Self::Hs256 { secret } = self {
            secret.zeroize();
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredUser {
    user_id: String,
    username: String,
    tenant: Option<String>,
    password_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredRoles {
    roles: Vec<Role>,
}

fn user_key(user_id: &str) -> String {
    format!("{USER_KEY_PREFIX}{user_id}")
}

fn role_key(user_id: &str) -> String {
    format!("{ROLE_KEY_PREFIX}{user_id}")
}

fn hash_password(password: &str) -> Result<String> {
    let mut rng = OsRng;
    let salt = SaltString::generate(&mut rng);
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map(|hash| hash.to_string())
        .map_err(|err| AuthError::Internal {
            message: format!("failed to hash password: {err}"),
        })
}

fn deserialize_json<T: for<'de> Deserialize<'de>>(bytes: &[u8], subject: &str) -> Result<T> {
    serde_json::from_slice(bytes).map_err(|err| AuthError::Internal {
        message: format!("failed to decode {subject}: {err}"),
    })
}

fn map_storage_error(err: bashfuldb_storage::StorageError) -> AuthError {
    AuthError::Internal {
        message: format!("storage error: {err}"),
    }
}

fn token_digest(token: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(token.as_bytes());
    URL_SAFE_NO_PAD.encode(hasher.finalize())
}

fn prefix_end(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    for idx in (0..end.len()).rev() {
        if end[idx] != u8::MAX {
            end[idx] = end[idx].saturating_add(1);
            end.truncate(idx + 1);
            return end;
        }
    }
    Vec::new()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, HashMap};
    use std::path::Path;
    use std::sync::RwLock;

    use bashfuldb_clock::ManualClock;
    use bashfuldb_storage::{BatchOp, ScanItem, StorageSnapshot};

    type MemCfMap = HashMap<String, BTreeMap<Vec<u8>, Vec<u8>>>;

    struct MemEngine {
        cfs: RwLock<MemCfMap>,
    }

    impl MemEngine {
        fn new() -> Self {
            let mut cfs = HashMap::new();
            for cf in bashfuldb_storage::ColumnFamily::all() {
                cfs.insert((*cf).to_string(), BTreeMap::new());
            }
            Self {
                cfs: RwLock::new(cfs),
            }
        }
    }

    struct MemSnapshot {
        data: MemCfMap,
    }

    #[async_trait]
    impl StorageEngine for MemEngine {
        async fn put(&self, cf: &str, key: &[u8], value: &[u8]) -> bashfuldb_storage::Result<()> {
            let mut guard = self.cfs.write().map_err(|_| bashfuldb_storage::StorageError::Engine {
                message: "rwlock poisoned".to_string(),
            })?;
            let cf_map = guard.get_mut(cf).ok_or_else(|| bashfuldb_storage::StorageError::UnknownColumnFamily {
                name: cf.to_string(),
            })?;
            cf_map.insert(key.to_vec(), value.to_vec());
            Ok(())
        }

        async fn get(&self, cf: &str, key: &[u8]) -> bashfuldb_storage::Result<Option<Vec<u8>>> {
            let guard = self.cfs.read().map_err(|_| bashfuldb_storage::StorageError::Engine {
                message: "rwlock poisoned".to_string(),
            })?;
            let cf_map = guard.get(cf).ok_or_else(|| bashfuldb_storage::StorageError::UnknownColumnFamily {
                name: cf.to_string(),
            })?;
            Ok(cf_map.get(key).cloned())
        }

        async fn delete(&self, cf: &str, key: &[u8]) -> bashfuldb_storage::Result<()> {
            let mut guard = self.cfs.write().map_err(|_| bashfuldb_storage::StorageError::Engine {
                message: "rwlock poisoned".to_string(),
            })?;
            let cf_map = guard.get_mut(cf).ok_or_else(|| bashfuldb_storage::StorageError::UnknownColumnFamily {
                name: cf.to_string(),
            })?;
            cf_map.remove(key);
            Ok(())
        }

        fn scan(
            &self,
            cf: &str,
            start: &[u8],
            end: &[u8],
        ) -> bashfuldb_storage::Result<
            Box<dyn Iterator<Item = bashfuldb_storage::Result<ScanItem>> + Send + 'static>,
        > {
            let guard = self.cfs.read().map_err(|_| bashfuldb_storage::StorageError::Engine {
                message: "rwlock poisoned".to_string(),
            })?;
            let cf_map = guard.get(cf).ok_or_else(|| bashfuldb_storage::StorageError::UnknownColumnFamily {
                name: cf.to_string(),
            })?;
            let items: Vec<_> = if end.is_empty() {
                cf_map
                    .range(start.to_vec()..)
                    .map(|(k, v)| Ok::<ScanItem, _>((k.clone(), v.clone())))
                    .collect()
            } else {
                cf_map
                    .range(start.to_vec()..end.to_vec())
                    .map(|(k, v)| Ok::<ScanItem, _>((k.clone(), v.clone())))
                    .collect()
            };
            Ok(Box::new(items.into_iter()))
        }

        async fn write_batch(&self, batch: WriteBatch) -> bashfuldb_storage::Result<()> {
            for op in batch.into_ops() {
                match op {
                    BatchOp::Put { cf, key, value } => self.put(&cf, &key, &value).await?,
                    BatchOp::Delete { cf, key } => self.delete(&cf, &key).await?,
                }
            }
            Ok(())
        }

        fn snapshot(&self) -> bashfuldb_storage::Result<Arc<dyn StorageSnapshot>> {
            let guard = self.cfs.read().map_err(|_| bashfuldb_storage::StorageError::Engine {
                message: "rwlock poisoned".to_string(),
            })?;
            Ok(Arc::new(MemSnapshot {
                data: guard.clone(),
            }))
        }

        async fn flush(&self) -> bashfuldb_storage::Result<()> {
            Ok(())
        }

        async fn checkpoint(&self, _path: &Path) -> bashfuldb_storage::Result<()> {
            Ok(())
        }

        fn has_column_family(&self, cf: &str) -> bool {
            if let Ok(guard) = self.cfs.read() {
                return guard.contains_key(cf);
            }
            false
        }
    }

    impl StorageSnapshot for MemSnapshot {
        fn get(&self, cf: &str, key: &[u8]) -> bashfuldb_storage::Result<Option<Vec<u8>>> {
            let cf_map = self.data.get(cf).ok_or_else(|| bashfuldb_storage::StorageError::UnknownColumnFamily {
                name: cf.to_string(),
            })?;
            Ok(cf_map.get(key).cloned())
        }

        fn scan(
            &self,
            cf: &str,
            start: &[u8],
            end: &[u8],
        ) -> bashfuldb_storage::Result<
            Box<dyn Iterator<Item = bashfuldb_storage::Result<ScanItem>> + Send + 'static>,
        > {
            let cf_map = self.data.get(cf).ok_or_else(|| bashfuldb_storage::StorageError::UnknownColumnFamily {
                name: cf.to_string(),
            })?;
            let items: Vec<_> = if end.is_empty() {
                cf_map
                    .range(start.to_vec()..)
                    .map(|(k, v)| Ok::<ScanItem, _>((k.clone(), v.clone())))
                    .collect()
            } else {
                cf_map
                    .range(start.to_vec()..end.to_vec())
                    .map(|(k, v)| Ok::<ScanItem, _>((k.clone(), v.clone())))
                    .collect()
            };
            Ok(Box::new(items.into_iter()))
        }
    }

    #[tokio::test]
    async fn login_refresh_verify_logout_roundtrip() {
        let storage = Arc::new(MemEngine::new());
        let clock = Arc::new(ManualClock::new(1_000_000));
        let service = AuthService::new(
            storage,
            clock.clone(),
            AuthConfig {
                jwt_mode: JwtMode::Hs256 { secret: [7u8; 32] },
                access_ttl: Duration::from_secs(60),
                refresh_ttl: Duration::from_secs(120),
                bootstrap_admin_password: Some("supersecret".to_string()),
            },
        )
        .await
        .expect("service should initialize");

        let credentials = Credentials {
            username: BOOTSTRAP_ADMIN_USER.to_string(),
            password: "supersecret".to_string(),
        };

        let tokens = service.login(&credentials).await.expect("login should succeed");
        let claims = service
            .verify(&tokens.access_token)
            .await
            .expect("verify should succeed");
        assert_eq!(claims.sub(), BOOTSTRAP_ADMIN_USER);

        clock.set_time(1_050_000);
        let refreshed = service
            .refresh(&tokens.refresh_token)
            .await
            .expect("refresh should succeed");
        assert!(!refreshed.access_token.is_empty());

        service
            .logout(&tokens.access_token)
            .await
            .expect("logout should succeed");
        let verify_after_logout = service.verify(&tokens.access_token).await;
        assert!(matches!(verify_after_logout, Err(AuthError::TokenRevoked)));
    }

    #[tokio::test]
    async fn access_token_expiration_is_enforced() {
        let storage = Arc::new(MemEngine::new());
        let clock = Arc::new(ManualClock::new(2_000_000));
        let service = AuthService::new(
            storage,
            clock.clone(),
            AuthConfig {
                jwt_mode: JwtMode::Hs256 { secret: [9u8; 32] },
                access_ttl: Duration::from_secs(1),
                refresh_ttl: Duration::from_secs(100),
                bootstrap_admin_password: Some("supersecret".to_string()),
            },
        )
        .await
        .expect("service should initialize");

        let tokens = service
            .login(&Credentials {
                username: BOOTSTRAP_ADMIN_USER.to_string(),
                password: "supersecret".to_string(),
            })
            .await
            .expect("login should succeed");

        clock.set_time(2_002_000);
        let verify = service.verify(&tokens.access_token).await;
        assert!(matches!(verify, Err(AuthError::TokenExpired)));
    }

    #[tokio::test]
    async fn rs256_mode_exposes_jwks() {
        let storage = Arc::new(MemEngine::new());
        let clock = Arc::new(ManualClock::new(3_000_000));
        let service = AuthService::new(
            storage,
            clock,
            AuthConfig {
                jwt_mode: JwtMode::Rs256,
                access_ttl: Duration::from_secs(60),
                refresh_ttl: Duration::from_secs(120),
                bootstrap_admin_password: Some("supersecret".to_string()),
            },
        )
        .await
        .expect("service should initialize");

        let jwks = service.jwks();
        let keys_len = jwks["keys"]
            .as_array()
            .map(|keys| keys.len())
            .unwrap_or_default();
        assert_eq!(keys_len, 1);
    }

    #[tokio::test]
    async fn rejects_second_non_server_admin_for_same_tenant() {
        let storage = Arc::new(MemEngine::new());
        let clock = Arc::new(ManualClock::new(4_000_000));
        let service = AuthService::new(
            storage,
            clock,
            AuthConfig {
                jwt_mode: JwtMode::Hs256 { secret: [11u8; 32] },
                access_ttl: Duration::from_secs(60),
                refresh_ttl: Duration::from_secs(120),
                bootstrap_admin_password: Some("supersecret".to_string()),
            },
        )
        .await
        .expect("service should initialize");

        service
            .create_user(UserCreateRequest {
                user_id: None,
                username: "tenant-user-1".to_string(),
                tenant: Some("tenant-a".to_string()),
                password: "password-1".to_string(),
                roles: vec![Role::ReadWrite],
            })
            .await
            .expect("first user should be created");

        let second = service
            .create_user(UserCreateRequest {
                user_id: None,
                username: "tenant-user-2".to_string(),
                tenant: Some("tenant-a".to_string()),
                password: "password-2".to_string(),
                roles: vec![Role::ReadOnly],
            })
            .await;

        assert!(matches!(second, Err(AuthError::UserAlreadyExists { .. })));
    }

    #[test]
    fn prefix_end_works_for_ascii_prefixes() {
        assert_eq!(prefix_end(b"auth/users/"), b"auth/users0".to_vec());
    }

    #[test]
    fn prefix_end_handles_trailing_0xff() {
        assert_eq!(prefix_end(&[b'a', 0xFF]), vec![b'b']);
    }

    #[test]
    fn prefix_end_handles_all_0xff() {
        assert!(prefix_end(&[0xFF, 0xFF]).is_empty());
    }

    #[test]
    fn prefix_end_handles_empty_prefix() {
        assert!(prefix_end(b"").is_empty());
    }
}
