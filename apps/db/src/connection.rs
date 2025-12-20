use crate::executor::{ExecOptions, SqlResult};
use crate::types::SqlValue;
use async_trait::async_trait;
use one_core::storage::DbConnectionConfig;
use tokio::sync::mpsc;

#[derive(Debug)]
pub enum DbError {
    ConnectionError(String),
    QueryError(String),
    Custom(String),
}

impl DbError {
    pub fn new(msg: impl Into<String>) -> Self {
        DbError::Custom(msg.into())
    }
}

impl std::fmt::Display for DbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DbError::ConnectionError(msg) => write!(f, "Connection error: {}", msg),
            DbError::QueryError(msg) => write!(f, "Query error: {}", msg),
            DbError::Custom(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for DbError {}

/// 流式执行进度信息
#[derive(Clone, Debug)]
pub struct StreamingProgress {
    pub current: usize,
    pub total: usize,
    pub result: SqlResult,
}

#[async_trait]
pub trait DbConnection: Sync + Send {
    fn config(&self) -> &DbConnectionConfig;

    /// Update database field in config (used when connection's actual database changes)
    fn set_config_database(&mut self, database: Option<String>);

    /// Whether this database type supports switching database within a connection
    fn supports_database_switch(&self) -> bool {
        true
    }

    async fn connect(&mut self) -> Result<(), DbError>;
    async fn disconnect(&mut self) -> Result<(), DbError>;
    async fn execute(&self, script: &str, options: ExecOptions) -> Result<Vec<SqlResult>, DbError>;
    async fn query(&self, query: &str, params: Option<Vec<SqlValue>>, options: ExecOptions) -> Result<SqlResult, DbError>;

    async fn ping(&self) -> Result<(), DbError> {
        self.query("SELECT 1", None, ExecOptions::default()).await.map(|_| ())
    }

    /// Get current database/schema name from the connection
    async fn current_database(&self) -> Result<Option<String>, DbError>;

    /// Switch to a different database
    async fn switch_database(&self, database: &str) -> Result<(), DbError>;

    async fn execute_streaming(
        &self,
        script: &str,
        options: ExecOptions,
        sender: mpsc::Sender<StreamingProgress>,
    ) -> Result<(), DbError>;
}
