use crate::connection::{DbConnection, DbError};
use crate::plugin::DatabasePlugin;
use crate::mysql::MySqlPlugin;
use crate::postgresql::PostgresPlugin;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tokio::time;
use gpui::Global;
use one_core::storage::{DatabaseType, DbConnectionConfig};

pub struct DbManager {}

impl DbManager {
    pub fn new() -> Self {
        Self {}
    }

    pub fn get_plugin(&self, db_type: &DatabaseType) -> Result<Box<dyn DatabasePlugin>, DbError> {
        match db_type {
            DatabaseType::MySQL => Ok(Box::new(MySqlPlugin::new())),
            DatabaseType::PostgreSQL => Ok(Box::new(PostgresPlugin::new())),
            _ => Err(DbError::new("Unsupported database type")),
        }
    }
}

impl Default for DbManager {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for DbManager {
    fn clone(&self) -> Self {
        Self {}
    }
}

/// Connection pool manager
pub struct ConnectionPool {
    connections: Arc<RwLock<HashMap<String, ConnectionEntry>>>,
    /// Connection idle timeout in seconds (default: 300 seconds = 5 minutes)
    idle_timeout: Duration,
}

struct ConnectionEntry {
    connection: Arc<RwLock<Box<dyn DbConnection + Send + Sync>>>,
    config: DbConnectionConfig,
    /// Last active time for this connection
    last_active: Instant,
}

impl ConnectionEntry {
    fn new(connection: Box<dyn DbConnection + Send + Sync>, config: DbConnectionConfig) -> Self {
        Self {
            connection: Arc::new(RwLock::new(connection)),
            config,
            last_active: Instant::now(),
        }
    }

    fn update_last_active(&mut self) {
        self.last_active = Instant::now();
    }

    fn is_expired(&self, timeout: Duration) -> bool {
        self.last_active.elapsed() > timeout
    }
}

impl ConnectionPool {
    pub fn new() -> Self {
        Self::with_timeout(Duration::from_secs(300))
    }

    pub fn with_timeout(idle_timeout: Duration) -> Self {
        Self {
            connections: Arc::new(RwLock::new(HashMap::new())),
            idle_timeout,
        }
    }

    /// Generate connection key: config_id + database
    fn make_connection_key(config_id: &str, database: Option<&str>) -> String {
        match database {
            Some(db) => format!("{}:{}", config_id, db),
            None => config_id.to_string(),
        }
    }

    /// Get or create a connection
    pub async fn get_connection(
        &self,
        config: DbConnectionConfig,
        db_manager: &DbManager
    ) -> Result<Arc<RwLock<Box<dyn DbConnection + Send + Sync>>>, DbError> {
        let key = Self::make_connection_key(&config.id, config.database.as_deref());

        // First, try to get existing connection
        {
            let mut connections = self.connections.write().await;
            if let Some(entry) = connections.get_mut(&key) {
                // Update last active time
                entry.update_last_active();
                return Ok(entry.connection.clone());
            }
        }

        // If connection doesn't exist, create a new one
        let plugin = db_manager.get_plugin(&config.database_type)?;
        let mut connection = plugin.create_connection(config.clone()).await?;

        // Connect to the database
        connection.connect().await?;

        // Store the connection
        let entry = ConnectionEntry::new(connection, config.clone());
        let connection_arc = entry.connection.clone();

        {
            let mut connections = self.connections.write().await;
            connections.insert(key, entry);
        }

        Ok(connection_arc)
    }

    /// Update last active time for a connection
    pub async fn update_last_active(&self, config_id: &str, database: Option<&str>) {
        let key = Self::make_connection_key(config_id, database);
        let mut connections = self.connections.write().await;
        if let Some(entry) = connections.get_mut(&key) {
            entry.update_last_active();
        }
    }

    pub async fn remove_connection(&self, config_id: &str, database: Option<&str>) -> Option<(Arc<RwLock<Box<dyn DbConnection + Send + Sync>>>, DbConnectionConfig)> {
        let key = Self::make_connection_key(config_id, database);
        let mut connections = self.connections.write().await;
        let removed = connections.remove(&key);

        removed.map(|entry| (entry.connection, entry.config))
    }

    /// Clean up expired connections
    async fn cleanup_expired_connections(&self) {
        let mut connections = self.connections.write().await;
        let timeout = self.idle_timeout;

        // Find expired connection keys
        let expired_keys: Vec<String> = connections
            .iter()
            .filter(|(_, entry)| entry.is_expired(timeout))
            .map(|(key, _)| key.clone())
            .collect();

        // Disconnect and remove expired connections
        for key in expired_keys {
            if let Some(entry) = connections.remove(&key) {
                // Try to disconnect gracefully
                if let Ok(mut conn) = entry.connection.try_write() {
                    let _ = conn.disconnect().await;
                }
                tracing::info!("Removed expired connection: {}", key);
            }
        }
    }

    /// Start a background task to periodically clean up expired connections
    pub fn start_cleanup_task(self: Arc<Self>) {
        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(60)); // Check every minute

            loop {
                interval.tick().await;
                self.cleanup_expired_connections().await;
            }
        });
    }
}

impl Default for ConnectionPool {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for ConnectionPool {
    fn clone(&self) -> Self {
        Self {
            connections: Arc::clone(&self.connections),
            idle_timeout: self.idle_timeout,
        }
    }
}


/// Global database state - stores DbManager and ConnectionPool
#[derive(Clone)]
pub struct GlobalDbState {
    pub db_manager: DbManager,
    pub connection_pool: ConnectionPool,
    /// connection_id -> config 映射
    connections: Arc<RwLock<HashMap<String, DbConnectionConfig>>>,
}

impl GlobalDbState {
    pub fn new() -> Self {
        Self {
            db_manager: DbManager::new(),
            connection_pool: ConnectionPool::new(),
            connections: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// 注册连接配置
    pub async fn register_connection(&self, config: DbConnectionConfig) {
        let mut connections = self.connections.write().await;
        connections.insert(config.id.clone(), config);
    }

    /// 根据 connection_id 获取配置
    pub async fn get_config(&self, connection_id: &str) -> Option<DbConnectionConfig> {
        let connections = self.connections.read().await;
        connections.get(connection_id).cloned()
    }

    /// 获取 plugin 和 connection（封装重复逻辑）
    pub async fn get_plugin_and_connection(
        &self,
        connection_id: &str,
    ) -> Result<(Box<dyn DatabasePlugin>, Arc<RwLock<Box<dyn DbConnection + Send + Sync>>>), DbError> {
        let config = self.get_config(connection_id).await
            .ok_or_else(|| DbError::ConnectionError(format!("Connection not found: {}", connection_id)))?;

        let plugin = self.db_manager.get_plugin(&config.database_type)?;
        let conn = self.connection_pool.get_connection(config, &self.db_manager).await?;

        Ok((plugin, conn))
    }
}

impl Default for GlobalDbState {
    fn default() -> Self {
        Self::new()
    }
}

impl Global for GlobalDbState {}
