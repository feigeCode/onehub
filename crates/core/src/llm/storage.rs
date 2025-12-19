use anyhow::Result;
use async_trait::async_trait;
use gpui::{App, SharedString};
use sqlx::{FromRow, SqlitePool};
use crate::gpui_tokio::Tokio;
use crate::llm::chat_history::{MessageRepository, SessionRepository};
use super::types::{ProviderConfig, ProviderType};
use crate::storage::{now, GlobalStorageState};
use crate::storage::traits::{Entity, Repository};

#[derive(FromRow)]
struct ProviderConfigRow {
    id: i64,
    name: String,
    provider_type: String,
    api_key: Option<String>,
    api_base: Option<String>,
    model: String,
    max_tokens: Option<i32>,
    temperature: Option<f32>,
    enabled: i32,
    created_at: i64,
    updated_at: i64,
}

impl TryFrom<ProviderConfigRow> for ProviderConfig {
    type Error = anyhow::Error;

    fn try_from(row: ProviderConfigRow) -> Result<Self> {
        let provider_type = ProviderType::from_str(&row.provider_type)
            .ok_or_else(|| anyhow::anyhow!("Invalid provider type: {}", row.provider_type))?;

        Ok(ProviderConfig {
            id: row.id,
            name: row.name,
            provider_type,
            api_key: row.api_key,
            api_base: row.api_base,
            model: row.model,
            max_tokens: row.max_tokens,
            temperature: row.temperature,
            enabled: row.enabled != 0,
            created_at: row.created_at,
            updated_at: row.updated_at,
        })
    }
}

#[derive(Clone)]
pub struct ProviderRepository {
    pool: SqlitePool,
}

impl ProviderRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

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

#[async_trait]
impl Repository for ProviderRepository {
    type Entity = ProviderConfig;

    fn entity_type(&self) -> SharedString {
        SharedString::from("ProviderConfig")
    }

    async fn insert(&self, item: &mut Self::Entity) -> Result<i64> {
        sqlx::query(
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
        .execute(&self.pool)
        .await?;

        Ok(0)
    }

    async fn update(&self, item: &Self::Entity) -> Result<()> {
        let updated_at = now();
        sqlx::query(
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
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn delete(&self, id: i64) -> Result<()> {
        sqlx::query("DELETE FROM llm_providers WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn get(&self, id: i64) -> Result<Option<Self::Entity>> {
        let row: Option<ProviderConfigRow> = sqlx::query_as("SELECT * FROM llm_providers WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;

        row.map(TryInto::try_into).transpose()
    }

    async fn list(&self) -> Result<Vec<Self::Entity>> {
        let rows: Vec<ProviderConfigRow> = sqlx::query_as("SELECT * FROM llm_providers ORDER BY created_at DESC")
            .fetch_all(&self.pool)
            .await?;

        rows.into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>>>()
    }

    async fn count(&self) -> Result<i64> {
        let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM llm_providers")
            .fetch_one(&self.pool)
            .await?;
        Ok(count)
    }

    async fn exists(&self, id: i64) -> Result<bool> {
        let row: Option<(i64,)> = sqlx::query_as("SELECT 1 FROM llm_providers WHERE id = ? LIMIT 1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.is_some())
    }
}

pub fn init(cx: &mut App) {
    let storage_state = cx.global::<GlobalStorageState>();
    let storage = storage_state.storage.clone();

    let result: Result<()> = Tokio::block_on(cx, async move {
        let pool = storage.get_pool().await?;

        let provider_repo = ProviderRepository::new(pool.clone());
        let session_repo = SessionRepository::new(pool.clone());
        let message_repo = MessageRepository::new(pool);

        storage.register(provider_repo).await?;
        storage.register(session_repo).await?;
        storage.register(message_repo).await?;
        Ok(())
    });
    if let Err(e) = result {
        panic!("Failed to initialize LLM provider repository: {}", e);
    }
}
