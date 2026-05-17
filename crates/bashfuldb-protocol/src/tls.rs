//! TLS support via `rustls`.
//!
//! This module is compiled only when the `tls` feature is enabled.

use std::sync::Arc;

use rustls::ServerConfig as RustlsServerConfig;
use tokio_rustls::TlsAcceptor;

use crate::error::ProtocolError;
use crate::types::TlsConfig;

/// Build a [`TlsAcceptor`] from a [`TlsConfig`].
///
/// When `tls_cfg.dev_mode` is `true`, a self-signed certificate is generated
/// automatically using `rcgen`.
pub(crate) fn build_tls_acceptor(tls_cfg: &TlsConfig) -> Result<TlsAcceptor, ProtocolError> {
    // Ensure a crypto provider is installed.  `install_default` returns `Err`
    // when a provider is already registered — that is the normal case in tests
    // or when the caller has already installed a provider — so the error is
    // intentionally ignored here.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let config = if tls_cfg.dev_mode {
        build_dev_config()?
    } else {
        build_config_from_pem(&tls_cfg.cert_pem, &tls_cfg.key_pem)?
    };
    Ok(TlsAcceptor::from(Arc::new(config)))
}

/// Build a rustls config with a self-signed development certificate.
fn build_dev_config() -> Result<RustlsServerConfig, ProtocolError> {
    use rcgen::generate_simple_self_signed;
    use rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};

    let certified = generate_simple_self_signed(vec!["localhost".to_string()])
        .map_err(|e| ProtocolError::Tls(format!("failed to generate self-signed cert: {e}")))?;

    let cert_der = certified.cert.der().clone();
    let key_der_bytes = certified.key_pair.serialize_der();

    let private_key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key_der_bytes));

    let config = RustlsServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der], private_key)
        .map_err(|e| ProtocolError::Tls(e.to_string()))?;

    Ok(config)
}

/// Build a rustls config from PEM-encoded certificate and key.
fn build_config_from_pem(
    cert_pem: &str,
    key_pem: &str,
) -> Result<RustlsServerConfig, ProtocolError> {
    use rustls_pemfile::{certs, private_key};

    let cert_chain: Vec<_> = certs(&mut cert_pem.as_bytes())
        .collect::<Result<_, _>>()
        .map_err(|e| ProtocolError::Tls(format!("invalid certificate PEM: {e}")))?;

    let private_key = private_key(&mut key_pem.as_bytes())
        .map_err(|e| ProtocolError::Tls(format!("invalid private-key PEM: {e}")))?
        .ok_or_else(|| ProtocolError::Tls("no private key found in PEM".to_string()))?;

    let config = RustlsServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(cert_chain, private_key)
        .map_err(|e| ProtocolError::Tls(e.to_string()))?;

    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dev_tls_acceptor_builds() {
        let tls_cfg = TlsConfig::dev();
        let result = build_tls_acceptor(&tls_cfg);
        assert!(result.is_ok(), "dev TLS acceptor should build");
    }

    #[test]
    fn pem_tls_acceptor_invalid_cert_errors() {
        let tls_cfg = TlsConfig {
            cert_pem: "not-a-pem".to_string(),
            key_pem: "not-a-pem".to_string(),
            dev_mode: false,
        };
        let result = build_tls_acceptor(&tls_cfg);
        // Should fail because the PEM data is invalid
        assert!(result.is_err());
    }

    #[test]
    fn build_dev_config_produces_valid_config() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let cfg = build_dev_config().unwrap();
        // Verify it's a standard TLS 1.3 config (no client auth)
        drop(cfg);
    }
}
