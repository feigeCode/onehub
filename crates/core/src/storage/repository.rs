use anyhow::Result;
use async_trait::async_trait;
use gpui::{App, SharedString};
use sqlx::{FromRow, SqlitePool};
use crate::gpui_tokio::Tokio;
use crate::storage::{traits::Repository, StoredConnection, ConnectionType};
use crate::storage::query_repository::QueryRepository;
use crate::storage::manager::{now, GlobalStorageState};
use crate::storage::Workspace;

#[derive(FromRow)]
struct ConnectionRow {
    id: i64,
    name: String,
    connection_type: String,
    params: String,
    workspace_id: Option<i64>,
    created_at: i64,
    updated_at: i64,
}

impl From<ConnectionRow> for StoredConnection {
    fn from(row: ConnectionRow) -> Self {
        StoredConnection {
            id: Some(row.id),
            name: row.name,
            connection_type: ConnectionType::from_str(&row.connection_type),
            params: row.params,
            workspace_id: row.workspace_id,
            created_at: Some(row.created_at),
            updated_at: Some(row.updated_at),
        }
    }
}

#[derive(FromRow)]
struct WorkspaceRow {
    id: i64,
    name: String,
    color: Option<String>,
    icon: Option<String>,
    created_at: i64,
    updated_at: i64,
}

impl From<WorkspaceRow> for Workspace {
    fn from(row: WorkspaceRow) -> Self {
        Workspace {
            id: Some(row.id),
            name: row.name,
            color: row.color,
            icon: row.icon,
            created_at: Some(row.created_at),
            updated_at: Some(row.updated_at),
        }
    }
}

#[derive(Clone)]
pub struct ConnectionRepository {
    pool: SqlitePool,
}

impl ConnectionRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl Repository for ConnectionRepository {
    type Entity = StoredConnection;

    fn entity_type(&self) -> SharedString {
       SharedString::from("Connection")
    }

    async fn insert(&self, item: &mut Self::Entity) -> Result<i64> {
        let now = now();
        let connection_type = item.connection_type.to_string();
        let result = sqlx::query(
            r#"
            INSERT INTO connections (name, connection_type, params, workspace_id, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(&item.name)
        .bind(&connection_type)
        .bind(&item.params)
        .bind(item.workspace_id)
        .bind(now)
        .bind(now)
        .execute(&self.pool)
        .await?;

        let id = result.last_insert_rowid();
        item.id = Some(id);
        item.created_at = Some(now);
        item.updated_at = Some(now);

        Ok(id)
    }

    async fn update(&self, item: &Self::Entity) -> Result<()> {
        let id = item.id.ok_or_else(|| anyhow::anyhow!("Cannot update without ID"))?;
        let now = now();
        let connection_type = item.connection_type.to_string();
        sqlx::query(
            r#"
            UPDATE connections
            SET name = ?, connection_type = ?, params = ?, workspace_id = ?, updated_at = ?
            WHERE id = ?
            "#,
        )
        .bind(&item.name)
        .bind(&connection_type)
        .bind(&item.params)
        .bind(item.workspace_id)
        .bind(now)
        .bind(id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn delete(&self, id: i64) -> Result<()> {
        sqlx::query("DELETE FROM connections WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    async fn get(&self, id: i64) -> Result<Option<Self::Entity>> {
        let row: Option<ConnectionRow> = sqlx::query_as(
            r#"
            SELECT id, name, connection_type, params, workspace_id, created_at, updated_at
            FROM connections
            WHERE id = ?
            "#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(Into::into))
    }

    async fn list(&self) -> Result<Vec<Self::Entity>> {
        let rows: Vec<ConnectionRow> = sqlx::query_as(
            r#"
            SELECT id, name, connection_type, params, workspace_id, created_at, updated_at
            FROM connections
            ORDER BY updated_at DESC
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(Into::into).collect())
    }

    async fn count(&self) -> Result<i64> {
        let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM connections")
            .fetch_one(&self.pool)
            .await?;

        Ok(count)
    }

    async fn exists(&self, id: i64) -> Result<bool> {
        let row: Option<(i64,)> = sqlx::query_as("SELECT 1 FROM connections WHERE id = ? LIMIT 1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;

        Ok(row.is_some())
    }
}

impl ConnectionRepository {
    pub async fn list_by_workspace(&self, workspace_id: Option<i64>) -> Result<Vec<StoredConnection>> {
        let rows: Vec<ConnectionRow> = sqlx::query_as(
            r#"
            SELECT id, name, connection_type, params, workspace_id, created_at, updated_at
            FROM connections
            WHERE workspace_id IS ? OR (? IS NULL AND workspace_id IS NULL)
            ORDER BY updated_at DESC
            "#,
        )
        .bind(workspace_id)
        .bind(workspace_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(Into::into).collect())
    }
}

#[derive(Clone)]
pub struct WorkspaceRepository {
    pool: SqlitePool,
}

impl WorkspaceRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl Repository for WorkspaceRepository {
    type Entity = Workspace;

    fn entity_type(&self) -> SharedString {
        SharedString::from("Workspace")
    }

    async fn insert(&self, item: &mut Self::Entity) -> Result<i64> {
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
        .execute(&self.pool)
        .await?;

        let id = result.last_insert_rowid();
        item.id = Some(id);
        item.created_at = Some(now);
        item.updated_at = Some(now);

        Ok(id)
    }

    async fn update(&self, item: &Self::Entity) -> Result<()> {
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
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn delete(&self, id: i64) -> Result<()> {
        sqlx::query("UPDATE connections SET workspace_id = NULL WHERE workspace_id = ?")
            .bind(id)
            .execute(&self.pool)
            .await?;

        sqlx::query("DELETE FROM workspaces WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    async fn get(&self, id: i64) -> Result<Option<Self::Entity>> {
        let row: Option<WorkspaceRow> = sqlx::query_as(
            r#"
            SELECT id, name, color, icon, created_at, updated_at
            FROM workspaces
            WHERE id = ?
            "#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(Into::into))
    }

    async fn list(&self) -> Result<Vec<Self::Entity>> {
        let rows: Vec<WorkspaceRow> = sqlx::query_as(
            r#"
            SELECT id, name, color, icon, created_at, updated_at
            FROM workspaces
            ORDER BY updated_at DESC
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(Into::into).collect())
    }

    async fn count(&self) -> Result<i64> {
        let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM workspaces")
            .fetch_one(&self.pool)
            .await?;

        Ok(count)
    }

    async fn exists(&self, id: i64) -> Result<bool> {
        let row: Option<(i64,)> = sqlx::query_as("SELECT 1 FROM workspaces WHERE id = ? LIMIT 1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;

        Ok(row.is_some())
    }
}

pub async fn run_migrations(pool: &SqlitePool) -> Result<()> {
    sqlx::migrate!("./migrations")
        .run(pool)
        .await?;
    Ok(())
}

pub fn init(cx: &mut App) {
    let storage_state = cx.global::<GlobalStorageState>();
    let storage = storage_state.storage.clone();

    let result: Result<()> = Tokio::block_on(cx, async move {
        let pool = storage.get_pool().await?;
        run_migrations(&pool).await?;

        let conn_repo = ConnectionRepository::new(pool.clone());
        let workspace_repo = WorkspaceRepository::new(pool.clone());
        let query_repo = QueryRepository::new(pool);

        storage.register(workspace_repo).await?;
        storage.register(conn_repo).await?;
        storage.register(query_repo).await?;
        Ok(())
    });
    if let Err(e) = result {
        panic!("Failed to initialize repositories: {}", e);
    }
}
