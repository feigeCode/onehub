use anyhow::Result;
use async_trait::async_trait;
use gpui::{App, SharedString};
use sqlx::{query, Row, SqlitePool};
use crate::gpui_tokio::Tokio;
use crate::llm::chat_history::{MessageRepository, SessionRepository};
use super::types::{ProviderConfig};
use crate::storage::{now, GlobalStorageState};
use crate::storage::traits::{Entity, Repository};

/// Provider storage implementation using SQLite
#[derive(Clone)]
pub struct ProviderRepository;
impl Default for ProviderRepository {
    fn default() -> Self {
        Self::new()
    }
}

impl ProviderRepository {
    pub fn new() -> Self {
        Self
    }

    fn row_to_config(&self, row: &sqlx::sqlite::SqliteRow) -> Result<ProviderConfig> {
        use super::types::ProviderType;
        
        let provider_type_str: String = row.try_get("provider_type")?;
        let provider_type = ProviderType::from_str(&provider_type_str)
            .ok_or_else(|| anyhow::anyhow!("Invalid provider type: {}", provider_type_str))?;

        Ok(ProviderConfig {
            id: row.try_get("id")?,
            name: row.try_get("name")?,
            provider_type,
            api_key: row.try_get("api_key")?,
            api_base: row.try_get("api_base")?,
            model: row.try_get("model")?,
            max_tokens: row.try_get("max_tokens")?,
            temperature: row.try_get("temperature")?,
            enabled: row.try_get::<i32, _>("enabled")? != 0,
            created_at: row.try_get("created_at")?,
            updated_at: row.try_get("updated_at")?,
        })
    }
}

// Implement Entity for ProviderConfig
impl Entity for ProviderConfig {
    fn id(&self) -> Option<i64> {
        Some(self.id)
    }

    fn created_at(&self) -> i64 {
        self.created_at
    }

    fn updated_at(&self) -> i64 {
        self.updated_at
    }
}

// Implement Repository trait for ProviderStore
#[async_trait]
impl Repository for ProviderRepository {
    type Entity = ProviderConfig;

    fn entity_type(&self) -> SharedString {
        SharedString::from("ProviderConfig")
    }

    async fn create_table(&self, pool: &SqlitePool) -> Result<()> {
        query(
            r#"
            CREATE TABLE IF NOT EXISTS llm_providers (
                id int PRIMARY KEY,
                name TEXT NOT NULL,
                provider_type TEXT NOT NULL,
                api_key TEXT,
                api_base TEXT,
                model TEXT NOT NULL,
                max_tokens INTEGER,
                temperature REAL,
                enabled INTEGER NOT NULL DEFAULT 1,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            )
            "#,
        )
        .execute(pool)
        .await?;

        Ok(())
    }

    async fn insert(&self, pool: &SqlitePool, item: &mut Self::Entity) -> Result<i64> {
        query(
            r#"
            INSERT INTO llm_providers (
                id, name, provider_type, api_key, api_base, model,
                max_tokens, temperature, enabled, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(item.id)
        .bind(&item.name)
        .bind(item.provider_type.as_str())
        .bind(&item.api_key)
        .bind(&item.api_base)
        .bind(&item.model)
        .bind(item.max_tokens)
        .bind(item.temperature)
        .bind(item.enabled)
        .bind(item.created_at)
        .bind(item.updated_at)
        .execute(pool)
        .await?;

        // Since we use String ID, return 0 as placeholder
        Ok(0)
    }

    async fn update(&self, pool: &SqlitePool, item: &Self::Entity) -> Result<()> {
        let updated_at = now();
        query(
            r#"
            UPDATE llm_providers SET
                name = ?,
                provider_type = ?,
                api_key = ?,
                api_base = ?,
                model = ?,
                max_tokens = ?,
                temperature = ?,
                enabled = ?,
                updated_at = ?
            WHERE id = ?
            "#,
        )
        .bind(&item.name)
        .bind(item.provider_type.as_str())
        .bind(&item.api_key)
        .bind(&item.api_base)
        .bind(&item.model)
        .bind(item.max_tokens)
        .bind(item.temperature)
        .bind(item.enabled)
        .bind(updated_at)
        .bind(item.id)
        .execute(pool)
        .await?;

        Ok(())
    }

    async fn delete(&self, pool: &SqlitePool, id: i64) -> Result<()> {
        // Since we use String ID, this method is not directly usable
        // Use the delete(&str) method instead
        query("DELETE FROM llm_providers WHERE id = ?")
            .bind(id.to_string())
            .execute(pool)
            .await?;
        Ok(())
    }

    async fn get(&self, pool: &SqlitePool, id: i64) -> Result<Option<Self::Entity>> {
        // Since we use String ID, this method is not directly usable
        let row = query("SELECT * FROM llm_providers WHERE id = ?")
            .bind(id.to_string())
            .fetch_optional(pool)
            .await?;

        Ok(row.map(|r| self.row_to_config(&r)).transpose()?)
    }

    async fn list(&self, pool: &SqlitePool) -> Result<Vec<Self::Entity>> {
        let rows = query("SELECT * FROM llm_providers ORDER BY created_at DESC")
            .fetch_all(pool)
            .await?;

        rows.iter()
            .map(|r| self.row_to_config(r))
            .collect::<Result<Vec<_>>>()
    }

    async fn count(&self, pool: &SqlitePool) -> Result<i64> {
        let row = query("SELECT COUNT(*) as count FROM llm_providers")
            .fetch_one(pool)
            .await?;
        Ok(row.get("count"))
    }

    async fn exists(&self, pool: &SqlitePool, id: i64) -> Result<bool> {
        let row = query("SELECT 1 FROM llm_providers WHERE id = ? LIMIT 1")
            .bind(id.to_string())
            .fetch_optional(pool)
            .await?;
        Ok(row.is_some())
    }
}

pub fn init(cx: &mut App) {
    let storage_state = cx.global::<GlobalStorageState>();
    let storage = storage_state.storage.clone();
    let provider_repo = ProviderRepository::new();
    let session_repo = SessionRepository::new();
    let message_repo = MessageRepository::new();
    let result: Result<()> = Tokio::block_on(cx, async move {
        let pool = storage.get_pool().await?;
        provider_repo.create_table(&pool).await?;
        storage.register(provider_repo).await?;
        session_repo.create_table(&pool).await?;
        storage.register(session_repo).await?;
        message_repo.create_table(&pool).await?;
        storage.register(message_repo).await?;
        Ok(())
    });
    if let Err(e) = result {
        panic!("Failed to initialize LLM provider repository: {}", e);
    }
}
