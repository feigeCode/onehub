
use anyhow::Result;
use async_trait::async_trait;
use gpui::SharedString;
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, SqlitePool};

use super::types::ChatMessage as LlmChatMessage;
use crate::storage::now;
use crate::storage::traits::Repository;

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct ChatSession {
    pub id: i64,
    pub name: String,
    pub provider_id: String,
    pub created_at: i64,
    pub updated_at: i64,
}

impl crate::storage::traits::Entity for ChatSession {
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

impl ChatSession {
    pub fn new(name: String, provider_id: String) -> Self {
        let now = now();
        Self {
            id: 0,
            name,
            provider_id,
            created_at: now,
            updated_at: now,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct ChatMessage {
    pub id: i64,
    pub session_id: i64,
    pub role: String,
    pub content: String,
    pub created_at: i64,
}

impl crate::storage::traits::Entity for ChatMessage {
    fn id(&self) -> Option<i64> {
        Some(self.id)
    }

    fn created_at(&self) -> i64 {
        self.created_at
    }

    fn updated_at(&self) -> i64 {
        self.created_at
    }
}

impl ChatMessage {
    pub fn new(session_id: i64, role: String, content: String) -> Self {
        Self {
            id: 0,
            session_id,
            role,
            content,
            created_at: now(),
        }
    }

    pub fn user(session_id: i64, content: String) -> Self {
        Self::new(session_id, "user".to_string(), content)
    }

    pub fn assistant(session_id: i64, content: String) -> Self {
        Self::new(session_id, "assistant".to_string(), content)
    }

    pub fn system(session_id: i64, content: String) -> Self {
        Self::new(session_id, "system".to_string(), content)
    }
}

#[derive(Clone)]
pub struct SessionRepository {
    pool: SqlitePool,
}

impl SessionRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl Repository for SessionRepository {
    type Entity = ChatSession;

    fn entity_type(&self) -> SharedString {
        SharedString::from("ChatSession")
    }

    async fn insert(&self, item: &mut Self::Entity) -> Result<i64> {
        let result = sqlx::query(
            r#"
            INSERT INTO chat_sessions (name, provider_id, created_at, updated_at)
            VALUES (?, ?, ?, ?)
            "#,
        )
        .bind(&item.name)
        .bind(&item.provider_id)
        .bind(item.created_at)
        .bind(item.updated_at)
        .execute(&self.pool)
        .await?;

        let id = result.last_insert_rowid();
        item.id = id;
        Ok(id)
    }

    async fn update(&self, item: &Self::Entity) -> Result<()> {
        let updated_at = now();
        sqlx::query(
            r#"
            UPDATE chat_sessions SET
                name = ?,
                provider_id = ?,
                updated_at = ?
            WHERE id = ?
            "#,
        )
        .bind(&item.name)
        .bind(&item.provider_id)
        .bind(updated_at)
        .bind(item.id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn delete(&self, id: i64) -> Result<()> {
        sqlx::query("DELETE FROM chat_sessions WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    async fn get(&self, id: i64) -> Result<Option<Self::Entity>> {
        let row: Option<ChatSession> = sqlx::query_as("SELECT * FROM chat_sessions WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;

        Ok(row)
    }

    async fn list(&self) -> Result<Vec<Self::Entity>> {
        let rows: Vec<ChatSession> = sqlx::query_as("SELECT * FROM chat_sessions ORDER BY updated_at DESC")
            .fetch_all(&self.pool)
            .await?;

        Ok(rows)
    }

    async fn count(&self) -> Result<i64> {
        let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM chat_sessions")
            .fetch_one(&self.pool)
            .await?;

        Ok(count)
    }

    async fn exists(&self, id: i64) -> Result<bool> {
        let row: Option<(i64,)> = sqlx::query_as("SELECT 1 FROM chat_sessions WHERE id = ? LIMIT 1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;

        Ok(row.is_some())
    }
}

impl SessionRepository {
    pub async fn list_by_provider(&self, provider_id: &str) -> Result<Vec<ChatSession>> {
        let rows: Vec<ChatSession> = sqlx::query_as("SELECT * FROM chat_sessions WHERE provider_id = ? ORDER BY updated_at DESC")
            .bind(provider_id)
            .fetch_all(&self.pool)
            .await?;

        Ok(rows)
    }
}

#[derive(Clone)]
pub struct MessageRepository {
    pool: SqlitePool,
}

impl MessageRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl Repository for MessageRepository {
    type Entity = ChatMessage;

    fn entity_type(&self) -> SharedString {
        SharedString::from("ChatMessage")
    }

    async fn insert(&self, item: &mut Self::Entity) -> Result<i64> {
        let result = sqlx::query(
            r#"
            INSERT INTO chat_messages (session_id, role, content, created_at)
            VALUES (?, ?, ?, ?)
            "#,
        )
        .bind(item.session_id)
        .bind(&item.role)
        .bind(&item.content)
        .bind(item.created_at)
        .execute(&self.pool)
        .await?;

        let id = result.last_insert_rowid();
        item.id = id;
        Ok(id)
    }

    async fn update(&self, item: &Self::Entity) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE chat_messages SET
                session_id = ?,
                role = ?,
                content = ?
            WHERE id = ?
            "#,
        )
        .bind(item.session_id)
        .bind(&item.role)
        .bind(&item.content)
        .bind(item.id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn delete(&self, id: i64) -> Result<()> {
        sqlx::query("DELETE FROM chat_messages WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    async fn get(&self, id: i64) -> Result<Option<Self::Entity>> {
        let row: Option<ChatMessage> = sqlx::query_as("SELECT * FROM chat_messages WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;

        Ok(row)
    }

    async fn list(&self) -> Result<Vec<Self::Entity>> {
        let rows: Vec<ChatMessage> = sqlx::query_as("SELECT * FROM chat_messages ORDER BY created_at ASC")
            .fetch_all(&self.pool)
            .await?;

        Ok(rows)
    }

    async fn count(&self) -> Result<i64> {
        let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM chat_messages")
            .fetch_one(&self.pool)
            .await?;

        Ok(count)
    }

    async fn exists(&self, id: i64) -> Result<bool> {
        let row: Option<(i64,)> = sqlx::query_as("SELECT 1 FROM chat_messages WHERE id = ? LIMIT 1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;

        Ok(row.is_some())
    }
}

impl MessageRepository {
    pub async fn list_by_session(&self, session_id: i64) -> Result<Vec<ChatMessage>> {
        let rows: Vec<ChatMessage> = sqlx::query_as("SELECT * FROM chat_messages WHERE session_id = ? ORDER BY created_at ASC")
            .bind(session_id)
            .fetch_all(&self.pool)
            .await?;

        Ok(rows)
    }

    pub async fn list_recent(&self, limit: i32) -> Result<Vec<ChatMessage>> {
        let rows: Vec<ChatMessage> = sqlx::query_as("SELECT * FROM chat_messages ORDER BY created_at DESC LIMIT ?")
            .bind(limit)
            .fetch_all(&self.pool)
            .await?;

        Ok(rows)
    }

    pub async fn delete_by_session(&self, session_id: i64) -> Result<()> {
        sqlx::query("DELETE FROM chat_messages WHERE session_id = ?")
            .bind(session_id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    pub fn to_llm_message(chat_message: &ChatMessage) -> LlmChatMessage {
        LlmChatMessage {
            role: chat_message.role.clone(),
            content: chat_message.content.clone(),
        }
    }
}
