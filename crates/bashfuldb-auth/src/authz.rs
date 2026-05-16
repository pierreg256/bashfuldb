use async_trait::async_trait;

use crate::{Action, AuthError, Authorizer, Claims, Resource, Result, Role};

/// Default RBAC authorizer implementation.
#[derive(Debug, Default, Clone)]
pub struct RbacAuthorizer;

#[async_trait]
impl Authorizer for RbacAuthorizer {
    async fn check(&self, claims: &Claims, action: Action, resource: &Resource) -> Result<()> {
        let is_server_admin = claims.roles().iter().any(|r| matches!(r, Role::ServerAdmin));

        if !is_server_admin {
            let claims_tenant = claims.tenant().ok_or_else(|| AuthError::PermissionDenied {
                action,
                resource: resource.to_string(),
            })?;

            let resource_tenant = resource.tenant.as_deref().ok_or_else(|| AuthError::PermissionDenied {
                action,
                resource: resource.to_string(),
            })?;

            if claims_tenant != resource_tenant {
                return Err(AuthError::PermissionDenied {
                    action,
                    resource: resource.to_string(),
                });
            }
        }

        if claims.roles().iter().any(|role| role.permits(action)) {
            return Ok(());
        }

        Err(AuthError::PermissionDenied {
            action,
            resource: resource.to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Claims, Role};

    #[tokio::test]
    async fn server_admin_can_access_cluster_resource() {
        let authz = RbacAuthorizer;
        let claims = Claims::new(
            "u1".into(),
            None,
            vec![Role::ServerAdmin],
            1,
            10,
        );
        assert!(authz
            .check(&claims, Action::ManageCluster, &Resource::cluster())
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn tenant_scope_isolation_is_enforced() {
        let authz = RbacAuthorizer;
        let claims = Claims::new(
            "u1".into(),
            Some("tenant-a".into()),
            vec![Role::TenantAdmin],
            1,
            10,
        );
        let result = authz
            .check(&claims, Action::Read, &Resource::tenant("tenant-b"))
            .await;
        assert!(matches!(result, Err(AuthError::PermissionDenied { .. })));
    }
}
