use serde::{Deserialize, Serialize};
use crate::storage::traits::Entity;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Query {
    pub id: Option<i64>,
    pub name: String,
    pub content: String,
    pub connection_id: String,
    pub database_name: Option<String>,
    pub created_at: Option<i64>,
    pub updated_at: Option<i64>,
}

impl Entity for Query {
    fn id(&self) -> Option<i64> {
        self.id
    }

    fn created_at(&self) -> i64 {
        self.created_at.unwrap_or(0)
    }

    fn updated_at(&self) -> i64 {
        self.updated_at.unwrap_or(0)
    }
}

impl Query {
    pub fn new(
        name: String,
        content: String,
        connection_id: String,
        database_name: Option<String>,
    ) -> Self {
        Self {
            id: None,
            name,
            content,
            connection_id,
            database_name,
            created_at: None,
            updated_at: None,
        }
    }
}