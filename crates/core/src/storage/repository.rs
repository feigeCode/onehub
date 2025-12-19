use anyhow::Result;
use async_trait::async_trait;
use gpui::{App, SharedString};
use sqlx::{Row, SqlitePool};
use crate::gpui_tokio::Tokio;
use crate::storage::{traits::Repository, StoredConnection};
use crate::storage::query_repository::QueryRepository;

/// Repository for StoredConnection
#[derive(Clone)]
pub struct ConnectionRepository;

impl Default for ConnectionRepository {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectionRepository {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Repository for ConnectionRepository {
    type Entity = StoredConnection;

    fn entity_type(&self) -> SharedString {
       SharedString::from("Connection")
    }

    async fn create_table(&self, pool: &SqlitePool) -> Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS connections (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                connection_type TEXT NOT NULL,
                params TEXT NOT NULL,
                workspace_id INTEGER,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            )
            "#,
        )
        .execute(pool)
        .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_connections_name ON connections(name)")
            .execute(pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_connections_workspace ON connections(workspace_id)")
            .execute(pool)
            .await?;

        Ok(())
    }

    async fn insert(&self, pool: &SqlitePool, item: &mut Self::Entity) -> Result<i64> {
        let now = now();
        let result = sqlx::query(
            r#"
            INSERT INTO connections (name, connection_type, params, workspace_id, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(&item.name)
        .bind(format!("{:?}", item.connection_type))
        .bind(&item.params)
        .bind(item.workspace_id)
        .bind(now)
        .bind(now)
        .execute(pool)
        .await?;

        let id = result.last_insert_rowid();
        item.id = Some(id);
        item.created_at = Some(now);
        item.updated_at = Some(now);

        Ok(id)
    }

    async fn update(&self, pool: &SqlitePool, item: &Self::Entity) -> Result<()> {
        let id = item.id.ok_or_else(|| anyhow::anyhow!("Cannot update without ID"))?;
        let now = now();
        sqlx::query(
            r#"
            UPDATE connections 
            SET name = ?, connection_type = ?, params = ?, workspace_id = ?, updated_at = ?
            WHERE id = ?
            "#,
        )
        .bind(&item.name)
        .bind(format!("{:?}", item.connection_type))
        .bind(&item.params)
        .bind(item.workspace_id)
        .bind(now)
        .bind(id)
        .execute(pool)
        .await?;

        Ok(())
    }

    async fn delete(&self, pool: &SqlitePool, id: i64) -> Result<()> {
        sqlx::query("DELETE FROM connections WHERE id = ?")
            .bind(id)
            .execute(pool)
            .await?;

        Ok(())
    }

    async fn get(&self, pool: &SqlitePool, id: i64) -> Result<Option<Self::Entity>> {
        let row = sqlx::query(
            r#"
            SELECT id, name, connection_type, params, workspace_id, created_at, updated_at
            FROM connections
            WHERE id = ?
            "#,
        )
        .bind(id)
        .fetch_optional(pool)
        .await?;

        Ok(row.map(|r| Self::row_to_entity(&r)))
    }

    async fn list(&self, pool: &SqlitePool) -> Result<Vec<Self::Entity>> {
        let rows = sqlx::query(
            r#"
            SELECT id, name, connection_type, params, workspace_id, created_at, updated_at
            FROM connections
            ORDER BY updated_at DESC
            "#,
        )
        .fetch_all(pool)
        .await?;

        Ok(rows.iter().map(Self::row_to_entity).collect())
    }

    async fn count(&self, pool: &SqlitePool) -> Result<i64> {
        let row = sqlx::query("SELECT COUNT(*) as count FROM connections")
            .fetch_one(pool)
            .await?;

        Ok(row.get("count"))
    }

    async fn exists(&self, pool: &SqlitePool, id: i64) -> Result<bool> {
        let row = sqlx::query("SELECT 1 FROM connections WHERE id = ? LIMIT 1")
            .bind(id)
            .fetch_optional(pool)
            .await?;

        Ok(row.is_some())
    }
}

impl ConnectionRepository {
    fn row_to_entity(row: &sqlx::sqlite::SqliteRow) -> StoredConnection {
        let conn_type_str: String = row.get("connection_type");

        StoredConnection {
            id: Some(row.get("id")),
            name: row.get("name"),
            connection_type: parse_connection_type(&conn_type_str),
            params: row.get("params"),
            workspace_id: row.get("workspace_id"),
            created_at: Some(row.get("created_at")),
            updated_at: Some(row.get("updated_at")),
        }
    }

    pub async fn list_by_workspace(&self, pool: &SqlitePool, workspace_id: Option<i64>) -> Result<Vec<StoredConnection>> {
        let rows = sqlx::query(
            r#"
            SELECT id, name, connection_type, params, workspace_id, created_at, updated_at
            FROM connections
            WHERE workspace_id IS ? OR (? IS NULL AND workspace_id IS NULL)
            ORDER BY updated_at DESC
            "#,
        )
        .bind(workspace_id)
        .bind(workspace_id)
        .fetch_all(pool)
        .await?;

        Ok(rows.iter().map(Self::row_to_entity).collect())
    }
}

use crate::storage::ConnectionType;
use crate::storage::manager::{now, GlobalStorageState};
use crate::storage::Workspace;

fn parse_connection_type(s: &str) -> ConnectionType {
    match s {
        "Database" => ConnectionType::Database,
        "SshSftp" => ConnectionType::SshSftp,
        "Redis" => ConnectionType::Redis,
        "MongoDB" => ConnectionType::MongoDB,
        _ => ConnectionType::Database,
    }
}

/// Repository for Workspace
#[derive(Clone)]
pub struct WorkspaceRepository;

impl Default for WorkspaceRepository {
    fn default() -> Self {
        Self::new()
    }
}

impl WorkspaceRepository {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Repository for WorkspaceRepository {
    type Entity = Workspace;

    fn entity_type(&self) -> SharedString {
        SharedString::from("Workspace")
    }

    async fn create_table(&self, pool: &SqlitePool) -> Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS workspaces (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                color TEXT,
                icon TEXT,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            )
            "#,
        )
        .execute(pool)
        .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_workspaces_name ON workspaces(name)")
            .execute(pool)
            .await?;

        Ok(())
    }

    async fn insert(&self, pool: &SqlitePool, item: &mut Self::Entity) -> Result<i64> {
        let now = now();
        let result = sqlx::query(
            r#"
            INSERT INTO workspaces (name, color, icon, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?)
            "#,
        )
        .bind(&item.name)
        .bind(&item.color)
        .bind(&item.icon)
        .bind(now)
        .bind(now)
        .execute(pool)
        .await?;

        let id = result.last_insert_rowid();
        item.id = Some(id);
        item.created_at = Some(now);
        item.updated_at = Some(now);

        Ok(id)
    }

    async fn update(&self, pool: &SqlitePool, item: &Self::Entity) -> Result<()> {
        let id = item.id.ok_or_else(|| anyhow::anyhow!("Cannot update without ID"))?;
        let now = now();
        sqlx::query(
            r#"
            UPDATE workspaces 
            SET name = ?, color = ?, icon = ?, updated_at = ?
            WHERE id = ?
            "#,
        )
        .bind(&item.name)
        .bind(&item.color)
        .bind(&item.icon)
        .bind(now)
        .bind(id)
        .execute(pool)
        .await?;

        Ok(())
    }

    async fn delete(&self, pool: &SqlitePool, id: i64) -> Result<()> {
        // 删除工作区时，将关联的连接的 workspace_id 设为 NULL
        sqlx::query("UPDATE connections SET workspace_id = NULL WHERE workspace_id = ?")
            .bind(id)
            .execute(pool)
            .await?;

        sqlx::query("DELETE FROM workspaces WHERE id = ?")
            .bind(id)
            .execute(pool)
            .await?;

        Ok(())
    }

    async fn get(&self, pool: &SqlitePool, id: i64) -> Result<Option<Self::Entity>> {
        let row = sqlx::query(
            r#"
            SELECT id, name, color, icon, created_at, updated_at
            FROM workspaces
            WHERE id = ?
            "#,
        )
        .bind(id)
        .fetch_optional(pool)
        .await?;

        Ok(row.map(|r| Self::row_to_entity(&r)))
    }

    async fn list(&self, pool: &SqlitePool) -> Result<Vec<Self::Entity>> {
        let rows = sqlx::query(
            r#"
            SELECT id, name, color, icon, created_at, updated_at
            FROM workspaces
            ORDER BY updated_at DESC
            "#,
        )
        .fetch_all(pool)
        .await?;

        Ok(rows.iter().map(Self::row_to_entity).collect())
    }

    async fn count(&self, pool: &SqlitePool) -> Result<i64> {
        let row = sqlx::query("SELECT COUNT(*) as count FROM workspaces")
            .fetch_one(pool)
            .await?;

        Ok(row.get("count"))
    }

    async fn exists(&self, pool: &SqlitePool, id: i64) -> Result<bool> {
        let row = sqlx::query("SELECT 1 FROM workspaces WHERE id = ? LIMIT 1")
            .bind(id)
            .fetch_optional(pool)
            .await?;

        Ok(row.is_some())
    }
}

impl WorkspaceRepository {
    fn row_to_entity(row: &sqlx::sqlite::SqliteRow) -> Workspace {
        Workspace {
            id: Some(row.get("id")),
            name: row.get("name"),
            color: row.get("color"),
            icon: row.get("icon"),
            created_at: Some(row.get("created_at")),
            updated_at: Some(row.get("updated_at")),
        }
    }
}

pub fn init(cx: &mut App) {
    let storage_state = cx.global::<GlobalStorageState>();
    let conn_repo = ConnectionRepository::new();
    let workspace_repo = WorkspaceRepository::new();
    let query_repo = QueryRepository::new();
    let storage = storage_state.storage.clone();

    let result: Result<()> = Tokio::block_on(cx, async move {
        let pool = storage.get_pool().await?;
        workspace_repo.create_table(&pool).await?;
        storage.register(workspace_repo).await?;
        conn_repo.create_table(&pool).await?;
        storage.register(conn_repo).await?;
        query_repo.create_table(&pool).await?;
        storage.register(query_repo).await?;
        Ok(())
    });
    if let Err(e) = result {
        panic!("Failed to initialize repositories: {}", e);
    }
}