use anyhow::Result;
use async_trait::async_trait;
use gpui::SharedString;

pub trait Entity: Send + Sync {

    fn id(&self) -> Option<i64>;

    fn created_at(&self) -> i64;

    fn updated_at(&self) -> i64;
}


#[async_trait]
pub trait Repository: Send + Sync {
    type Entity: Entity;

    fn entity_type(&self) -> SharedString;

    async fn insert(&self, item: &mut Self::Entity) -> Result<i64>;

    async fn update(&self, item: &Self::Entity) -> Result<()>;

    async fn delete(&self, id: i64) -> Result<()>;

    async fn get(&self, id: i64) -> Result<Option<Self::Entity>>;

    async fn list(&self) -> Result<Vec<Self::Entity>>;

    async fn count(&self) -> Result<i64>;

    async fn exists(&self, id: i64) -> Result<bool>;
}
