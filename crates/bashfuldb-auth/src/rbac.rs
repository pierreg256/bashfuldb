use serde::{Deserialize, Serialize};
use std::fmt;

/// RBAC roles with decreasing privilege levels.
///
/// There is **no automatic scope inheritance**: a `TenantAdmin` on tenant A
/// has zero access to tenant B.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Role {
    /// Full cluster access. Can manage all tenants, nodes, and configuration.
    ServerAdmin,
    /// Full access to a specific tenant (databases, collections, users within that tenant).
    TenantAdmin,
    /// Can create/drop databases and collections within assigned scope.
    DatabaseAdmin,
    /// Can read and write documents within assigned scope.
    ReadWrite,
    /// Can only read documents within assigned scope.
    ReadOnly,
}

impl Role {
    /// Returns true if this role can perform the given action.
    pub fn permits(&self, action: Action) -> bool {
        match self {
            Role::ServerAdmin => true,
            Role::TenantAdmin => !matches!(action, Action::ManageCluster),
            Role::DatabaseAdmin => matches!(
                action,
                Action::Read
                    | Action::Create
                    | Action::Update
                    | Action::Delete
                    | Action::CreateIndex
                    | Action::DropIndex
                    | Action::CreateCollection
                    | Action::DropCollection
                    | Action::CreateDatabase
                    | Action::DropDatabase
            ),
            Role::ReadWrite => matches!(
                action,
                Action::Read | Action::Create | Action::Update | Action::Delete
            ),
            Role::ReadOnly => matches!(action, Action::Read),
        }
    }
}

impl fmt::Display for Role {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// Actions that can be performed on resources.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Action {
    /// Read a document or list documents.
    Read,
    /// Create a new document.
    Create,
    /// Update an existing document.
    Update,
    /// Delete a document.
    Delete,
    /// Create a secondary index on a collection.
    CreateIndex,
    /// Drop a secondary index.
    DropIndex,
    /// Create a new database within a tenant.
    CreateDatabase,
    /// Drop a database.
    DropDatabase,
    /// Create a new collection within a database.
    CreateCollection,
    /// Drop a collection.
    DropCollection,
    /// Manage users within a tenant.
    ManageUsers,
    /// Manage cluster-wide configuration and nodes.
    ManageCluster,
}

/// A scoped resource that an action is performed on.
///
/// Scope narrows from tenant → database → collection. A `None` field means
/// "all resources at that level".
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Resource {
    /// The tenant scope (None = cluster-wide).
    pub tenant: Option<String>,
    /// The database scope (None = all databases in tenant).
    pub database: Option<String>,
    /// The collection scope (None = all collections in database).
    pub collection: Option<String>,
}

impl Resource {
    /// Creates a cluster-wide resource (no tenant scope).
    pub fn cluster() -> Self {
        Self {
            tenant: None,
            database: None,
            collection: None,
        }
    }

    /// Creates a tenant-scoped resource.
    pub fn tenant(tenant: impl Into<String>) -> Self {
        Self {
            tenant: Some(tenant.into()),
            database: None,
            collection: None,
        }
    }

    /// Creates a database-scoped resource.
    pub fn database(tenant: impl Into<String>, db: impl Into<String>) -> Self {
        Self {
            tenant: Some(tenant.into()),
            database: Some(db.into()),
            collection: None,
        }
    }

    /// Creates a collection-scoped resource.
    pub fn collection(
        tenant: impl Into<String>,
        db: impl Into<String>,
        coll: impl Into<String>,
    ) -> Self {
        Self {
            tenant: Some(tenant.into()),
            database: Some(db.into()),
            collection: Some(coll.into()),
        }
    }
}

impl fmt::Display for Resource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (&self.tenant, &self.database, &self.collection) {
            (None, _, _) => write!(f, "cluster"),
            (Some(t), None, _) => write!(f, "tenant:{t}"),
            (Some(t), Some(d), None) => write!(f, "tenant:{t}/db:{d}"),
            (Some(t), Some(d), Some(c)) => write!(f, "tenant:{t}/db:{d}/coll:{c}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_admin_permits_everything() {
        assert!(Role::ServerAdmin.permits(Action::ManageCluster));
        assert!(Role::ServerAdmin.permits(Action::Delete));
        assert!(Role::ServerAdmin.permits(Action::CreateDatabase));
    }

    #[test]
    fn read_only_permits_only_read() {
        assert!(Role::ReadOnly.permits(Action::Read));
        assert!(!Role::ReadOnly.permits(Action::Create));
        assert!(!Role::ReadOnly.permits(Action::Delete));
        assert!(!Role::ReadOnly.permits(Action::CreateIndex));
    }

    #[test]
    fn read_write_permits_crud_only() {
        assert!(Role::ReadWrite.permits(Action::Read));
        assert!(Role::ReadWrite.permits(Action::Create));
        assert!(Role::ReadWrite.permits(Action::Update));
        assert!(Role::ReadWrite.permits(Action::Delete));
        assert!(!Role::ReadWrite.permits(Action::CreateIndex));
        assert!(!Role::ReadWrite.permits(Action::ManageUsers));
    }

    #[test]
    fn tenant_admin_cannot_manage_cluster() {
        assert!(Role::TenantAdmin.permits(Action::ManageUsers));
        assert!(!Role::TenantAdmin.permits(Action::ManageCluster));
    }

    #[test]
    fn resource_display() {
        assert_eq!(Resource::cluster().to_string(), "cluster");
        assert_eq!(Resource::tenant("acme").to_string(), "tenant:acme");
        assert_eq!(
            Resource::collection("acme", "main", "users").to_string(),
            "tenant:acme/db:main/coll:users"
        );
    }
}
