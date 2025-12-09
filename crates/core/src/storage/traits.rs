use anyhow::Result;
use async_trait::async_trait;
use gpui::SharedString;
use sqlx::SqlitePool;

/// Repository trait - each entity implements its own CRUD operations
/// 

pub trait Entity: Send + Sync {
    
    fn id(&self) -> Option<i64>;
    
    fn created_at(&self) -> i64;
    
    fn updated_at(&self) -> i64;
}


#[async_trait]
pub trait Repository: Send + Sync {
    type Entity: Entity;

    fn entity_type(&self) -> SharedString;
    
    /// Create table schema
    async fn create_table(&self, pool: &SqlitePool) -> Result<()>;

    /// Insert a new record
    async fn insert(&self, pool: &SqlitePool, item: &mut Self::Entity) -> Result<i64>;

    /// Update an existing record
    async fn update(&self, pool: &SqlitePool, item: &Self::Entity) -> Result<()>;

    /// Delete a record by ID
    async fn delete(&self, pool: &SqlitePool, id: i64) -> Result<()>;

    /// Get a record by ID
    async fn get(&self, pool: &SqlitePool, id: i64) -> Result<Option<Self::Entity>>;

    /// List all records
    async fn list(&self, pool: &SqlitePool) -> Result<Vec<Self::Entity>>;

    /// Count records
    async fn count(&self, pool: &SqlitePool) -> Result<i64>;

    /// Check if exists
    async fn exists(&self, pool: &SqlitePool, id: i64) -> Result<bool>;
}
