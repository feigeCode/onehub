
use anyhow::Result;
use async_trait::async_trait;
use gpui::SharedString;
use serde::{Deserialize, Serialize};
use sqlx::{query, Row, SqlitePool};

use super::types::ChatMessage as LlmChatMessage;
use crate::storage::now;
use crate::storage::traits::Repository;

// Chat session represents a conversation with an LLM provider
#[derive(Debug, Clone, Serialize, Deserialize)]
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
            id: 0, // 临时值，insert 后会被数据库 ID 覆盖
            name,
            provider_id,
            created_at: now,
            updated_at: now,
        }
    }
}

// Chat message represents a single message in a conversation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatMessage {
    pub id: i64,
    pub session_id: i64,
    pub role: String,  // "user", "assistant", "system"
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
        self.created_at // ChatMessage doesn't have updated_at, so using created_at
    }
}

impl ChatMessage {
    pub fn new(session_id: i64, role: String, content: String) -> Self {
        Self {
            id: 0, // 临时值，insert 后会被数据库 ID 覆盖
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

// Session Repository
#[derive(Clone)]
pub struct SessionRepository;

impl Default for SessionRepository {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionRepository {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Repository for SessionRepository {
    type Entity = ChatSession;

    fn entity_type(&self) -> SharedString {
        SharedString::from("ChatSession")
    }

    async fn create_table(&self, pool: &SqlitePool) -> Result<()> {
        query(
            r#"
            CREATE TABLE IF NOT EXISTS chat_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                provider_id TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            )
            "#,
        )
        .execute(pool)
        .await?;

        query("CREATE INDEX IF NOT EXISTS idx_chat_sessions_provider_id ON chat_sessions (provider_id)")
            .execute(pool)
            .await?;

        Ok(())
    }

    async fn insert(&self, pool: &SqlitePool, item: &mut Self::Entity) -> Result<i64> {
        let result = query(
            r#"
            INSERT INTO chat_sessions (name, provider_id, created_at, updated_at)
            VALUES (?, ?, ?, ?)
            "#,
        )
        .bind(&item.name)
        .bind(&item.provider_id)
        .bind(item.created_at)
        .bind(item.updated_at)
        .execute(pool)
        .await?;

        let id = result.last_insert_rowid();
        item.id = id;
        Ok(id)
    }

    async fn update(&self, pool: &SqlitePool, item: &Self::Entity) -> Result<()> {
        let updated_at = now();
        query(
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
        .execute(pool)
        .await?;

        Ok(())
    }

    async fn delete(&self, pool: &SqlitePool, id: i64) -> Result<()> {
        query("DELETE FROM chat_sessions WHERE id = ?")
            .bind(id)
            .execute(pool)
            .await?;

        Ok(())
    }

    async fn get(&self, pool: &SqlitePool, id: i64) -> Result<Option<Self::Entity>> {
        let row = query("SELECT * FROM chat_sessions WHERE id = ?")
            .bind(id)
            .fetch_optional(pool)
            .await?;

        Ok(row.map(|r| Self::row_to_session(&r)))
    }

    async fn list(&self, pool: &SqlitePool) -> Result<Vec<Self::Entity>> {
        let rows = query("SELECT * FROM chat_sessions ORDER BY updated_at DESC")
            .fetch_all(pool)
            .await?;

        Ok(rows.iter().map(Self::row_to_session).collect())
    }

    async fn count(&self, pool: &SqlitePool) -> Result<i64> {
        let row = query("SELECT COUNT(*) as count FROM chat_sessions")
            .fetch_one(pool)
            .await?;

        Ok(row.get("count"))
    }

    async fn exists(&self, pool: &SqlitePool, id: i64) -> Result<bool> {
        let row = query("SELECT 1 FROM chat_sessions WHERE id = ? LIMIT 1")
            .bind(id)
            .fetch_optional(pool)
            .await?;

        Ok(row.is_some())
    }
}

impl SessionRepository {
    fn row_to_session(row: &sqlx::sqlite::SqliteRow) -> ChatSession {
        ChatSession {
            id: row.get("id"),
            name: row.get("name"),
            provider_id: row.get("provider_id"),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
        }
    }

    pub async fn list_by_provider(&self, pool: &SqlitePool, provider_id: &str) -> Result<Vec<ChatSession>> {
        let rows = query("SELECT * FROM chat_sessions WHERE provider_id = ? ORDER BY updated_at DESC")
            .bind(provider_id)
            .fetch_all(pool)
            .await?;

        Ok(rows.iter().map(Self::row_to_session).collect())
    }
}

// Message Repository
#[derive(Clone)]
pub struct MessageRepository;

impl Default for MessageRepository {
    fn default() -> Self {
        Self::new()
    }
}

impl MessageRepository {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Repository for MessageRepository {
    type Entity = ChatMessage;

    fn entity_type(&self) -> SharedString {
        SharedString::from("ChatMessage")
    }

    async fn create_table(&self, pool: &SqlitePool) -> Result<()> {
        query(
            r#"
            CREATE TABLE IF NOT EXISTS chat_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id INTEGER NOT NULL,
                role TEXT NOT NULL,
                content TEXT NOT NULL,
                created_at INTEGER NOT NULL
            )
            "#,
        )
        .execute(pool)
        .await?;

        query("CREATE INDEX IF NOT EXISTS idx_chat_messages_session_id ON chat_messages (session_id)")
            .execute(pool)
            .await?;

        Ok(())
    }

    async fn insert(&self, pool: &SqlitePool, item: &mut Self::Entity) -> Result<i64> {
        let result = query(
            r#"
            INSERT INTO chat_messages (session_id, role, content, created_at)
            VALUES (?, ?, ?, ?)
            "#,
        )
        .bind(item.session_id)
        .bind(&item.role)
        .bind(&item.content)
        .bind(item.created_at)
        .execute(pool)
        .await?;

        let id = result.last_insert_rowid();
        item.id = id;
        Ok(id)
    }

    async fn update(&self, pool: &SqlitePool, item: &Self::Entity) -> Result<()> {
        let updated_at = now();
        query(
            r#"
            UPDATE chat_messages SET
                session_id = ?,
                role = ?,
                content = ?,
                created_at = ?
            WHERE id = ?
            "#,
        )
        .bind(item.session_id)
        .bind(&item.role)
        .bind(&item.content)
        .bind(updated_at)
        .bind(item.id)
        .execute(pool)
        .await?;

        Ok(())
    }

    async fn delete(&self, pool: &SqlitePool, id: i64) -> Result<()> {
        query("DELETE FROM chat_messages WHERE id = ?")
            .bind(id)
            .execute(pool)
            .await?;

        Ok(())
    }

    async fn get(&self, pool: &SqlitePool, id: i64) -> Result<Option<Self::Entity>> {
        let row = query("SELECT * FROM chat_messages WHERE id = ?")
            .bind(id)
            .fetch_optional(pool)
            .await?;

        Ok(row.map(|r| Self::row_to_message(&r)))
    }

    async fn list(&self, pool: &SqlitePool) -> Result<Vec<Self::Entity>> {
        let rows = query("SELECT * FROM chat_messages ORDER BY created_at ASC")
            .fetch_all(pool)
            .await?;

        Ok(rows.iter().map(Self::row_to_message).collect())
    }

    async fn count(&self, pool: &SqlitePool) -> Result<i64> {
        let row = query("SELECT COUNT(*) as count FROM chat_messages")
            .fetch_one(pool)
            .await?;

        Ok(row.get("count"))
    }

    async fn exists(&self, pool: &SqlitePool, id: i64) -> Result<bool> {
        let row = query("SELECT 1 FROM chat_messages WHERE id = ? LIMIT 1")
            .bind(id)
            .fetch_optional(pool)
            .await?;

        Ok(row.is_some())
    }
}

impl MessageRepository {
    pub async fn list_by_session(&self, pool: &SqlitePool, session_id: i64) -> Result<Vec<ChatMessage>> {
        let rows = query("SELECT * FROM chat_messages WHERE session_id = ? ORDER BY created_at ASC")
            .bind(session_id)
            .fetch_all(pool)
            .await?;

        Ok(rows.iter().map(Self::row_to_message).collect())
    }

    pub async fn list_recent(&self, pool: &SqlitePool, limit: i32) -> Result<Vec<ChatMessage>> {
        let rows = query("SELECT * FROM chat_messages ORDER BY created_at DESC LIMIT ?")
            .bind(limit)
            .fetch_all(pool)
            .await?;

        Ok(rows.iter().map(Self::row_to_message).collect())
    }

    pub async fn delete_by_session(&self, pool: &SqlitePool, session_id: i64) -> Result<()> {
        query("DELETE FROM chat_messages WHERE session_id = ?")
            .bind(session_id)
            .execute(pool)
            .await?;

        Ok(())
    }

    fn row_to_message(row: &sqlx::sqlite::SqliteRow) -> ChatMessage {
        ChatMessage {
            id: row.get("id"),
            session_id: row.get("session_id"),
            role: row.get("role"),
            content: row.get("content"),
            created_at: row.get("created_at"),
        }
    }

    pub fn to_llm_message(chat_message: &ChatMessage) -> LlmChatMessage {
        LlmChatMessage {
            role: chat_message.role.clone(),
            content: chat_message.content.clone(),
        }
    }
}