use anyhow::Result;
use async_trait::async_trait;
use gpui::SharedString;
use sqlx::SqlitePool;

use crate::storage::traits::Repository;
use crate::storage::query_model::Query;

#[derive(Clone)]
pub struct QueryRepository {
    pool: SqlitePool,
}

impl QueryRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl Repository for QueryRepository {
    type Entity = Query;

    fn entity_type(&self) -> SharedString {
        SharedString::from("Query")
    }

    async fn insert(&self, item: &mut Self::Entity) -> Result<i64> {
        Self::validate_query_name(&item.name)?;

        if self.find_by_name(&item.connection_id, &item.name).await?.is_some() {
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

        Self::validate_query_name(&item.name)?;

        if let Some(existing) = self.find_by_name(&item.connection_id, &item.name).await?
            && existing.id != item.id {
                return Err(anyhow::anyhow!("A query with this name already exists in the connection"));
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
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn delete(&self, id: i64) -> Result<()> {
        sqlx::query("DELETE FROM queries WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    async fn get(&self, id: i64) -> Result<Option<Self::Entity>> {
        let row: Option<Query> = sqlx::query_as(
            r#"
            SELECT id, name, content, connection_id, database_name, created_at, updated_at
            FROM queries
            WHERE id = ?
            "#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row)
    }

    async fn list(&self) -> Result<Vec<Self::Entity>> {
        let rows: Vec<Query> = sqlx::query_as(
            r#"
            SELECT id, name, content, connection_id, database_name, created_at, updated_at
            FROM queries
            ORDER BY updated_at DESC
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows)
    }

    async fn count(&self) -> Result<i64> {
        let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM queries")
            .fetch_one(&self.pool)
            .await?;

        Ok(count)
    }

    async fn exists(&self, id: i64) -> Result<bool> {
        let row: Option<(i64,)> = sqlx::query_as("SELECT 1 FROM queries WHERE id = ? LIMIT 1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;

        Ok(row.is_some())
    }
}

impl QueryRepository {
    fn validate_query_name(name: &str) -> Result<()> {
        if name.is_empty() {
            return Err(anyhow::anyhow!("Query name cannot be empty"));
        }

        if name.len() > 100 {
            return Err(anyhow::anyhow!("Query name must be 100 characters or less"));
        }

        for c in name.chars() {
            if !c.is_alphanumeric() && c != ' ' && c != '-' && c != '_' {
                return Err(anyhow::anyhow!("Query name contains invalid characters. Only alphanumeric characters, spaces, hyphens, and underscores are allowed"));
            }
        }

        Ok(())
    }

    pub async fn list_by_connection(&self, connection_id: &str) -> Result<Vec<Query>> {
        let rows: Vec<Query> = sqlx::query_as(
            r#"
            SELECT id, name, content, connection_id, database_name, created_at, updated_at
            FROM queries
            WHERE connection_id = ?
            ORDER BY updated_at DESC
            "#,
        )
        .bind(connection_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows)
    }

    pub async fn list_by_database(&self, connection_id: &str, database_name: &str) -> Result<Vec<Query>> {
        let rows: Vec<Query> = sqlx::query_as(
            r#"
            SELECT id, name, content, connection_id, database_name, created_at, updated_at
            FROM queries
            WHERE connection_id = ? AND database_name = ?
            ORDER BY updated_at DESC
            "#,
        )
        .bind(connection_id)
        .bind(database_name)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows)
    }

    pub async fn find_by_name(&self, connection_id: &str, name: &str) -> Result<Option<Query>> {
        let row: Option<Query> = sqlx::query_as(
            r#"
            SELECT id, name, content, connection_id, database_name, created_at, updated_at
            FROM queries
            WHERE connection_id = ? AND name = ?
            "#,
        )
        .bind(connection_id)
        .bind(name)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row)
    }
}
