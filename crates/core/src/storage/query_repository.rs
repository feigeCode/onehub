use anyhow::Result;
use async_trait::async_trait;
use gpui::SharedString;
use sqlx::{Row, SqlitePool};

use crate::storage::traits::Repository;
use crate::storage::query_model::Query;

/// Repository for Query
#[derive(Clone)]
pub struct QueryRepository;

impl QueryRepository {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Repository for QueryRepository {
    type Entity = Query;

    fn entity_type(&self) -> SharedString {
        SharedString::from("Query")
    }

    async fn create_table(&self, pool: &SqlitePool) -> Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS queries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                content TEXT NOT NULL,
                connection_id TEXT NOT NULL,
                database_name TEXT,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                UNIQUE(connection_id, name)
            )
            "#,
        )
        .execute(pool)
        .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_queries_connection ON queries(connection_id)")
            .execute(pool)
            .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_queries_database ON queries(database_name) WHERE database_name IS NOT NULL"
        )
        .execute(pool)
        .await?;

        Ok(())
    }

    async fn insert(&self, pool: &SqlitePool, item: &mut Self::Entity) -> Result<i64> {
        // Validate the query name before insertion
        Self::validate_query_name(&item.name)?;

        // Check for duplicate name within the same connection
        if self.find_by_name(pool, &item.connection_id, &item.name).await?.is_some() {
            return Err(anyhow::anyhow!("A query with this name already exists in the connection"));
        }

        let now = crate::storage::manager::now();
        let result = sqlx::query(
            r#"
            INSERT INTO queries (name, content, connection_id, database_name, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(&item.name)
        .bind(&item.content)
        .bind(&item.connection_id)
        .bind(&item.database_name)
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

        // Validate the query name before update
        Self::validate_query_name(&item.name)?;

        // Check for duplicate name within the same connection (excluding current item)
        if let Some(existing) = self.find_by_name(pool, &item.connection_id, &item.name).await? {
            if existing.id != item.id {
                return Err(anyhow::anyhow!("A query with this name already exists in the connection"));
            }
        }

        let now = crate::storage::manager::now();
        sqlx::query(
            r#"
            UPDATE queries
            SET name = ?, content = ?, connection_id = ?, database_name = ?, updated_at = ?
            WHERE id = ?
            "#,
        )
        .bind(&item.name)
        .bind(&item.content)
        .bind(&item.connection_id)
        .bind(&item.database_name)
        .bind(now)
        .bind(id)
        .execute(pool)
        .await?;

        Ok(())
    }

    async fn delete(&self, pool: &SqlitePool, id: i64) -> Result<()> {
        sqlx::query("DELETE FROM queries WHERE id = ?")
            .bind(id)
            .execute(pool)
            .await?;

        Ok(())
    }

    async fn get(&self, pool: &SqlitePool, id: i64) -> Result<Option<Self::Entity>> {
        let row = sqlx::query(
            r#"
            SELECT id, name, content, connection_id, database_name, created_at, updated_at
            FROM queries
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
            SELECT id, name, content, connection_id, database_name, created_at, updated_at
            FROM queries
            ORDER BY updated_at DESC
            "#,
        )
        .fetch_all(pool)
        .await?;

        Ok(rows.iter().map(|r| Self::row_to_entity(r)).collect())
    }

    async fn count(&self, pool: &SqlitePool) -> Result<i64> {
        let row = sqlx::query("SELECT COUNT(*) as count FROM queries")
            .fetch_one(pool)
            .await?;

        Ok(row.get("count"))
    }

    async fn exists(&self, pool: &SqlitePool, id: i64) -> Result<bool> {
        let row = sqlx::query("SELECT 1 FROM queries WHERE id = ? LIMIT 1")
            .bind(id)
            .fetch_optional(pool)
            .await?;

        Ok(row.is_some())
    }
}

impl QueryRepository {
    // Validate query name according to requirements
    fn validate_query_name(name: &str) -> Result<()> {
        if name.is_empty() {
            return Err(anyhow::anyhow!("Query name cannot be empty"));
        }

        if name.len() > 100 {
            return Err(anyhow::anyhow!("Query name must be 100 characters or less"));
        }

        // Check for allowed characters (alphanumeric, spaces, hyphens, underscores)
        for c in name.chars() {
            if !c.is_alphanumeric() && c != ' ' && c != '-' && c != '_' {
                return Err(anyhow::anyhow!("Query name contains invalid characters. Only alphanumeric characters, spaces, hyphens, and underscores are allowed"));
            }
        }

        Ok(())
    }

    fn row_to_entity(row: &sqlx::sqlite::SqliteRow) -> Query {
        Query {
            id: Some(row.get("id")),
            name: row.get("name"),
            content: row.get("content"),
            connection_id: row.get("connection_id"),
            database_name: row.get("database_name"),
            created_at: Some(row.get("created_at")),
            updated_at: Some(row.get("updated_at")),
        }
    }

    pub async fn list_by_connection(&self, pool: &SqlitePool, connection_id: &str) -> Result<Vec<Query>> {
        let rows = sqlx::query(
            r#"
            SELECT id, name, content, connection_id, database_name, created_at, updated_at
            FROM queries
            WHERE connection_id = ?
            ORDER BY updated_at DESC
            "#,
        )
        .bind(connection_id)
        .fetch_all(pool)
        .await?;

        Ok(rows.iter().map(|r| Self::row_to_entity(r)).collect())
    }

    pub async fn list_by_database(&self, pool: &SqlitePool, connection_id: &str, database_name: &str) -> Result<Vec<Query>> {
        let rows = sqlx::query(
            r#"
            SELECT id, name, content, connection_id, database_name, created_at, updated_at
            FROM queries
            WHERE connection_id = ? AND database_name = ?
            ORDER BY updated_at DESC
            "#,
        )
        .bind(connection_id)
        .bind(database_name)
        .fetch_all(pool)
        .await?;

        Ok(rows.iter().map(|r| Self::row_to_entity(r)).collect())
    }

    pub async fn find_by_name(&self, pool: &SqlitePool, connection_id: &str, name: &str) -> Result<Option<Query>> {
        let row = sqlx::query(
            r#"
            SELECT id, name, content, connection_id, database_name, created_at, updated_at
            FROM queries
            WHERE connection_id = ? AND name = ?
            "#,
        )
        .bind(connection_id)
        .bind(name)
        .fetch_optional(pool)
        .await?;

        Ok(row.map(|r| Self::row_to_entity(&r)))
    }
}