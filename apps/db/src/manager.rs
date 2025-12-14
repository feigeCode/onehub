use crate::connection::{DbConnection, DbError};
use crate::plugin::DatabasePlugin;
use crate::mysql::MySqlPlugin;
use crate::postgresql::PostgresPlugin;
use crate::{DbNode, ExecOptions, SqlResult};
use one_core::gpui_tokio::Tokio;
use one_core::storage::{DatabaseType, DbConnectionConfig, GlobalStorageState};
use gpui::{AppContext, Global, Task};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::RwLock;
use tracing::{error, info, warn};

/// Database manager - creates database plugins
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

/// Connection session - represents a single database connection
struct ConnectionSession {
    connection: Box<dyn DbConnection + Send + Sync>,
    config: DbConnectionConfig,
    last_active: Instant,
    created_at: Instant,
    session_id: String,
    /// Whether this session is currently in a transaction
    in_transaction: bool,
}

impl ConnectionSession {
    fn new(connection: Box<dyn DbConnection + Send + Sync>, config: DbConnectionConfig, session_id: String) -> Self {
        let now = Instant::now();
        Self {
            connection,
            config,
            last_active: now,
            created_at: now,
            session_id,
            in_transaction: false,
        }
    }

    fn update_last_active(&mut self) {
        self.last_active = Instant::now();
    }

    /// Check if session is expired (idle timeout)
    fn is_expired(&self, timeout: Duration) -> bool {
        // Don't expire sessions that are in a transaction
        if self.in_transaction {
            return false;
        }
        self.last_active.elapsed() > timeout
    }

    /// Check if session has exceeded maximum lifetime
    fn is_lifetime_expired(&self, max_lifetime: Duration) -> bool {
        self.created_at.elapsed() > max_lifetime
    }

    async fn close(&mut self) {
        if let Err(e) = self.connection.disconnect().await {
            error!("Failed to disconnect session {}: {}", self.session_id, e);
        } else {
            info!("Closed session: {}", self.session_id);
        }
    }
}

/// Connection manager - manages database connections for a client application
pub struct ConnectionManager {
    /// config_id -> list of sessions for that config
    sessions: Arc<RwLock<HashMap<String, Vec<ConnectionSession>>>>,
    /// Connection idle timeout (default: 5 minutes)
    idle_timeout: Duration,
    /// Maximum connection lifetime (default: 30 minutes)
    max_lifetime: Duration,
    /// Session counter for generating unique IDs
    session_counter: Arc<tokio::sync::Mutex<u64>>,
}

impl ConnectionManager {
    pub fn new() -> Self {
        Self {
            sessions: Arc::new(RwLock::new(HashMap::new())),
            idle_timeout: Duration::from_secs(300),      // 5 minutes
            max_lifetime: Duration::from_secs(1800),     // 30 minutes
            session_counter: Arc::new(tokio::sync::Mutex::new(0)),
        }
    }

    pub fn with_config(idle_timeout: Duration, max_lifetime: Duration) -> Self {
        Self {
            sessions: Arc::new(RwLock::new(HashMap::new())),
            idle_timeout,
            max_lifetime,
            session_counter: Arc::new(tokio::sync::Mutex::new(0)),
        }
    }

    /// Generate unique session ID
    async fn generate_session_id(&self, config_id: &str) -> String {
        let mut counter = self.session_counter.lock().await;
        *counter += 1;
        format!("{}:session:{}", config_id, *counter)
    }

    /// Create a new connection session
    pub async fn create_session(
        &self,
        config: DbConnectionConfig,
        db_manager: &DbManager,
    ) -> Result<String, DbError> {
        let config_id = config.id.clone();
        let session_id = self.generate_session_id(&config_id).await;

        // Create new connection
        let plugin = db_manager.get_plugin(&config.database_type)?;
        let mut connection = plugin.create_connection(config.clone()).await?;

        // Connect to database
        connection.connect().await?;
        info!("Created new session: {}", session_id);

        // Store session
        let session = ConnectionSession::new(connection, config, session_id.clone());

        let mut sessions = self.sessions.write().await;
        sessions.entry(config_id)
            .or_insert_with(Vec::new)
            .push(session);

        Ok(session_id)
    }

    /// Get mutable access to a session's connection
    /// Returns the connection wrapped in the write guard to maintain lock
    pub async fn get_session_connection(
        &self,
        session_id: &str,
    ) -> Result<SessionConnectionGuard<'_>, DbError> {
        let sessions = self.sessions.write().await;

        // Check if session exists
        let exists = sessions.values().any(|list| {
            list.iter().any(|s| s.session_id == session_id)
        });

        if !exists {
            return Err(DbError::new(&format!("Session not found: {}", session_id)));
        }

        Ok(SessionConnectionGuard {
            sessions,
            session_id: session_id.to_string(),
        })
    }
}

/// Guard that holds the write lock and provides access to a session's connection
pub struct SessionConnectionGuard<'a> {
    sessions: tokio::sync::RwLockWriteGuard<'a, HashMap<String, Vec<ConnectionSession>>>,
    session_id: String,
}

impl<'a> SessionConnectionGuard<'a> {
    /// Get mutable reference to the connection and update last active time
    pub fn connection(&mut self) -> Option<&mut (dyn DbConnection + Send + Sync)> {
        for session_list in self.sessions.values_mut() {
            if let Some(session) = session_list.iter_mut().find(|s| s.session_id == self.session_id) {
                session.update_last_active();
                return Some(&mut *session.connection);
            }
        }
        None
    }
}

impl ConnectionManager {
    /// Get session config
    pub async fn get_session_config(&self, session_id: &str) -> Option<DbConnectionConfig> {
        let sessions = self.sessions.read().await;

        for session_list in sessions.values() {
            if let Some(session) = session_list.iter().find(|s| s.session_id == session_id) {
                return Some(session.config.clone());
            }
        }

        None
    }

    /// Mark session as in/out of transaction
    pub async fn set_transaction_state(&self, session_id: &str, in_transaction: bool) {
        let mut sessions = self.sessions.write().await;

        for session_list in sessions.values_mut() {
            if let Some(session) = session_list.iter_mut().find(|s| s.session_id == session_id) {
                session.in_transaction = in_transaction;
                info!("Session {} transaction state: {}", session_id, in_transaction);
                break;
            }
        }
    }

    /// Close a specific session
    pub async fn close_session(&self, session_id: &str) -> Result<(), DbError> {
        let mut sessions = self.sessions.write().await;

        let mut found_config_id: Option<String> = None;
        let mut removed_session: Option<ConnectionSession> = None;

        for (config_id, session_list) in sessions.iter_mut() {
            if let Some(pos) = session_list.iter().position(|s| s.session_id == session_id) {
                removed_session = Some(session_list.remove(pos));
                if session_list.is_empty() {
                    found_config_id = Some(config_id.clone());
                }
                break;
            }
        }

        // Remove empty config entry after iteration
        if let Some(config_id) = found_config_id {
            sessions.remove(&config_id);
        }

        // Close session after releasing iteration
        if let Some(mut session) = removed_session {
            session.close().await;
            return Ok(());
        }

        Err(DbError::new(&format!("Session not found: {}", session_id)))
    }

    /// Remove all sessions for a connection config
    pub async fn remove_all_sessions(&self, config_id: &str) {
        let mut sessions = self.sessions.write().await;

        if let Some(mut session_list) = sessions.remove(config_id) {
            info!("Closing {} sessions for config: {}", session_list.len(), config_id);

            for session in session_list.iter_mut() {
                session.close().await;
            }
        }
    }

    /// Clean up expired sessions
    async fn cleanup_expired_sessions(&self) {
        let mut sessions = self.sessions.write().await;
        let idle_timeout = self.idle_timeout;
        let max_lifetime = self.max_lifetime;

        for (config_id, session_list) in sessions.iter_mut() {
            let mut i = 0;
            while i < session_list.len() {
                let should_remove = session_list[i].is_expired(idle_timeout)
                    || session_list[i].is_lifetime_expired(max_lifetime);

                if should_remove {
                    let mut session = session_list.remove(i);
                    warn!(
                        "Closing expired session {} for config {} (in_transaction: {}, idle: {}s, lifetime: {}s)",
                        session.session_id,
                        config_id,
                        session.in_transaction,
                        session.last_active.elapsed().as_secs(),
                        session.created_at.elapsed().as_secs()
                    );
                    session.close().await;
                } else {
                    i += 1;
                }
            }
        }

        // Remove empty config entries
        sessions.retain(|_, list| !list.is_empty());
    }

    /// Get connection statistics
    pub async fn stats(&self) -> ConnectionStats {
        let sessions = self.sessions.read().await;
        let mut total = 0;
        let mut in_transaction = 0;

        for session_list in sessions.values() {
            total += session_list.len();
            in_transaction += session_list.iter().filter(|s| s.in_transaction).count();
        }

        ConnectionStats {
            total_sessions: total,
            active_transactions: in_transaction,
            configs_with_sessions: sessions.len(),
        }
    }

    /// List all sessions for a config
    pub async fn list_sessions(&self, config_id: &str) -> Vec<SessionInfo> {
        let sessions = self.sessions.read().await;

        sessions.get(config_id)
            .map(|list| {
                list.iter().map(|s| SessionInfo {
                    session_id: s.session_id.clone(),
                    database: s.config.database.clone(),
                    in_transaction: s.in_transaction,
                    idle_time: s.last_active.elapsed(),
                    lifetime: s.created_at.elapsed(),
                }).collect()
            })
            .unwrap_or_default()
    }
}

impl Default for ConnectionManager {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for ConnectionManager {
    fn clone(&self) -> Self {
        Self {
            sessions: Arc::clone(&self.sessions),
            idle_timeout: self.idle_timeout,
            max_lifetime: self.max_lifetime,
            session_counter: Arc::clone(&self.session_counter),
        }
    }
}

/// Connection statistics
#[derive(Debug, Clone)]
pub struct ConnectionStats {
    pub total_sessions: usize,
    pub active_transactions: usize,
    pub configs_with_sessions: usize,
}

/// Session information
#[derive(Debug, Clone)]
pub struct SessionInfo {
    pub session_id: String,
    pub database: Option<String>,
    pub in_transaction: bool,
    pub idle_time: Duration,
    pub lifetime: Duration,
}

/// Connection pool compatibility layer
#[derive(Clone)]
pub struct ConnectionPool {
    db_manager: DbManager,
}

impl ConnectionPool {
    pub fn new(db_manager: DbManager) -> Self {
        Self { db_manager }
    }

    pub async fn get_connection(&self, config: DbConnectionConfig, _db_manager: &DbManager) -> anyhow::Result<Arc<RwLock<Box<dyn DbConnection + Send + Sync>>>> {
        let plugin = self.db_manager.get_plugin(&config.database_type)?;
        let mut connection = plugin.create_connection(config).await?;
        connection.connect().await?;
        Ok(Arc::new(RwLock::new(connection)))
    }
}

/// Global database state - stores DbManager and ConnectionManager
#[derive(Clone)]
pub struct GlobalDbState {
    pub db_manager: DbManager,
    pub connection_manager: ConnectionManager,
    pub connection_pool: ConnectionPool,
    /// connection_id -> config mapping
    connections: Arc<RwLock<HashMap<String, DbConnectionConfig>>>,
}

impl GlobalDbState {
    pub fn new() -> Self {
        let manager = ConnectionManager::new();
        let db_manager = DbManager::new();

        Self {
            db_manager: db_manager.clone(),
            connection_manager: manager,
            connection_pool: ConnectionPool::new(db_manager),
            connections: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Start the cleanup task (should be called after Tokio runtime is available)
    pub fn start_cleanup_task<C>(&self, cx: &mut C) 
    where 
        C: AppContext
    {
        let manager = Arc::new(self.connection_manager.clone());
        Tokio::spawn(cx, async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            loop {
                interval.tick().await;
                manager.cleanup_expired_sessions().await;
            }
        });
    }

    /// Internal async method for get_config
    pub async fn get_config_async(&self, connection_id: &str) -> Option<DbConnectionConfig> {
        let connections = self.connections.read().await;
        connections.get(connection_id).cloned()
    }
    
    
    pub fn get_plugin(&self, database_type: &DatabaseType) -> Result<Box<dyn DatabasePlugin>, DbError> {
        self.db_manager.get_plugin(database_type)
    }
    
    pub fn drop_database<C>(
        &self,
        cx: &mut C,
        config_id: String,
        database_name: String,
    ) -> C::Result<Task<anyhow::Result<SqlResult>>>
    where 
        C: AppContext
    {
        let clone_self = self.clone();
        Tokio::spawn_result(cx, async move {
            let config = clone_self.get_config_async(&*config_id).await
                .ok_or_else(|| anyhow::anyhow!("Connection not found: {}", config_id))?;
            
            let plugin = clone_self.get_plugin(&config.database_type)?;
            let sql = plugin.drop_database(&database_name);
            
            // Create session and execute
            let session_id = clone_self.connection_manager
                .create_session(config, &clone_self.db_manager)
                .await?;
            
            let result = {
                let mut guard = clone_self.connection_manager.get_session_connection(&session_id).await?;
                let conn = guard.connection()
                    .ok_or_else(|| anyhow::anyhow!("Session connection not found"))?;
                let results = conn.execute(&sql, ExecOptions::default()).await?;
                results.into_iter().next().unwrap_or(SqlResult::Exec(crate::executor::ExecResult {
                    sql: sql.clone(),
                    rows_affected: 0,
                    elapsed_ms: 0,
                    message: None,
                }))
            };
            
            // Close session after execution
            let _ = clone_self.connection_manager.close_session(&session_id).await;
            
            Ok(result)
        })
    }

    /// Drop table
    pub fn drop_table<C>(
        &self,
        cx: &mut C,
        config_id: String,
        database: String,
        table_name: String,
    ) -> C::Result<Task<anyhow::Result<SqlResult>>>
    where 
        C: AppContext
    {
        let clone_self = self.clone();
        Tokio::spawn_result(cx, async move {
            let config = clone_self.get_config_async(&config_id).await
                .ok_or_else(|| anyhow::anyhow!("Connection not found: {}", config_id))?;
            
            let plugin = clone_self.get_plugin(&config.database_type)?;
            let sql = plugin.drop_table(&database, &table_name);
            
            let session_id = clone_self.connection_manager
                .create_session(config, &clone_self.db_manager)
                .await?;
            
            let result = {
                let mut guard = clone_self.connection_manager.get_session_connection(&session_id).await?;
                let conn = guard.connection()
                    .ok_or_else(|| anyhow::anyhow!("Session connection not found"))?;
                let results = conn.execute(&sql, ExecOptions::default()).await?;
                results.into_iter().next().unwrap_or(SqlResult::Exec(crate::executor::ExecResult {
                    sql: sql.clone(),
                    rows_affected: 0,
                    elapsed_ms: 0,
                    message: None,
                }))
            };
            
            let _ = clone_self.connection_manager.close_session(&session_id).await;
            Ok(result)
        })
    }

    /// Truncate table
    pub fn truncate_table<C>(
        &self,
        cx: &mut C,
        config_id: String,
        database: String,
        table_name: String,
    ) -> C::Result<Task<anyhow::Result<SqlResult>>>
    where 
        C: AppContext
    {
        let clone_self = self.clone();
        Tokio::spawn_result(cx, async move {
            let config = clone_self.get_config_async(&config_id).await
                .ok_or_else(|| anyhow::anyhow!("Connection not found: {}", config_id))?;
            
            let plugin = clone_self.get_plugin(&config.database_type)?;
            let sql = plugin.truncate_table(&database, &table_name);
            
            let session_id = clone_self.connection_manager
                .create_session(config, &clone_self.db_manager)
                .await?;
            
            let result = {
                let mut guard = clone_self.connection_manager.get_session_connection(&session_id).await?;
                let conn = guard.connection()
                    .ok_or_else(|| anyhow::anyhow!("Session connection not found"))?;
                let results = conn.execute(&sql, ExecOptions::default()).await?;
                results.into_iter().next().unwrap_or(SqlResult::Exec(crate::executor::ExecResult {
                    sql: sql.clone(),
                    rows_affected: 0,
                    elapsed_ms: 0,
                    message: None,
                }))
            };
            
            let _ = clone_self.connection_manager.close_session(&session_id).await;
            Ok(result)
        })
    }

    /// Rename table
    pub fn rename_table<C>(
        &self,
        cx: &mut C,
        config_id: String,
        database: String,
        old_name: String,
        new_name: String,
    ) -> C::Result<Task<anyhow::Result<SqlResult>>>
    where 
        C: AppContext
    {
        let clone_self = self.clone();
        Tokio::spawn_result(cx, async move {
            let config = clone_self.get_config_async(&config_id).await
                .ok_or_else(|| anyhow::anyhow!("Connection not found: {}", config_id))?;
            
            let plugin = clone_self.get_plugin(&config.database_type)?;
            let sql = plugin.rename_table(&database, &old_name, &new_name);
            
            let session_id = clone_self.connection_manager
                .create_session(config, &clone_self.db_manager)
                .await?;
            
            let result = {
                let mut guard = clone_self.connection_manager.get_session_connection(&session_id).await?;
                let conn = guard.connection()
                    .ok_or_else(|| anyhow::anyhow!("Session connection not found"))?;
                let results = conn.execute(&sql, ExecOptions::default()).await?;
                results.into_iter().next().unwrap_or(SqlResult::Exec(crate::executor::ExecResult {
                    sql: sql.clone(),
                    rows_affected: 0,
                    elapsed_ms: 0,
                    message: None,
                }))
            };
            
            let _ = clone_self.connection_manager.close_session(&session_id).await;
            Ok(result)
        })
    }

    /// Drop view
    pub fn drop_view<C>(
        &self,
        cx: &mut C,
        config_id: String,
        database: String,
        view_name: String,
    ) -> C::Result<Task<anyhow::Result<SqlResult>>>
    where 
        C: AppContext
    {
        let clone_self = self.clone();
        Tokio::spawn_result(cx, async move {
            let config = clone_self.get_config_async(&config_id).await
                .ok_or_else(|| anyhow::anyhow!("Connection not found: {}", config_id))?;
            
            let plugin = clone_self.get_plugin(&config.database_type)?;
            let sql = plugin.drop_view(&database, &view_name);
            
            let session_id = clone_self.connection_manager
                .create_session(config, &clone_self.db_manager)
                .await?;
            
            let result = {
                let mut guard = clone_self.connection_manager.get_session_connection(&session_id).await?;
                let conn = guard.connection()
                    .ok_or_else(|| anyhow::anyhow!("Session connection not found"))?;
                let results = conn.execute(&sql, ExecOptions::default()).await?;
                results.into_iter().next().unwrap_or(SqlResult::Exec(crate::executor::ExecResult {
                    sql: sql.clone(),
                    rows_affected: 0,
                    elapsed_ms: 0,
                    message: None,
                }))
            };
            
            let _ = clone_self.connection_manager.close_session(&session_id).await;
            Ok(result)
        })
    }

    /// Create database
    pub fn create_database<C>(
        &self,
        cx: &mut C,
        config_id: String,
        database_name: String,
    ) -> C::Result<Task<anyhow::Result<SqlResult>>>
    where 
        C: AppContext
    {
        let clone_self = self.clone();
        Tokio::spawn_result(cx, async move {
            let config = clone_self.get_config_async(&config_id).await
                .ok_or_else(|| anyhow::anyhow!("Connection not found: {}", config_id))?;
            
            let plugin = clone_self.get_plugin(&config.database_type)?;
            let sql = plugin.create_database(&database_name, &database_name);
            
            let session_id = clone_self.connection_manager
                .create_session(config, &clone_self.db_manager)
                .await?;
            
            let result = {
                let mut guard = clone_self.connection_manager.get_session_connection(&session_id).await?;
                let conn = guard.connection()
                    .ok_or_else(|| anyhow::anyhow!("Session connection not found"))?;
                let results = conn.execute(&sql, ExecOptions::default()).await?;
                results.into_iter().next().unwrap_or(SqlResult::Exec(crate::executor::ExecResult {
                    sql: sql.clone(),
                    rows_affected: 0,
                    elapsed_ms: 0,
                    message: None,
                }))
            };
            
            let _ = clone_self.connection_manager.close_session(&session_id).await;
            Ok(result)
        })
    }

    /// Register a connection configuration
    pub async fn register_connection(
        &self,
        config: DbConnectionConfig,
    ) {
        let mut connections = self.connections.write().await;
        connections.insert(config.id.clone(), config);
    }

    /// Unregister a connection configuration
    pub fn unregister_connection<C>(
        &self,
        cx: &mut C,
        connection_id: String,
    ) -> C::Result<Task<anyhow::Result<()>>>
    where
        C: AppContext
    {
        let clone_self = self.clone();
        Tokio::spawn_result(cx, async move {
            // Close all sessions for this connection
            clone_self.connection_manager.remove_all_sessions(&connection_id).await;

            // Remove from registry
            let mut connections = clone_self.connections.write().await;
            connections.remove(&connection_id);
            Ok(())
        })
    }
    

    /// Get all registered connections
    pub fn list_connections<C>(
        &self,
        cx: &mut C,
    ) -> C::Result<Task<anyhow::Result<Vec<DbConnectionConfig>>>>
    where
        C: AppContext
    {
        let clone_self = self.clone();
        Tokio::spawn_result(cx, async move {
            let connections = clone_self.connections.read().await;
            Ok(connections.values().cloned().collect())
        })
    }

    /// Create a new session for executing queries
    pub fn create_session<C>(
        &self,
        cx: &mut C,
        connection_id: String,
        database: Option<String>,
    ) -> C::Result<Task<anyhow::Result<String>>>
    where
        C: AppContext
    {
        let clone_self = self.clone();
        Tokio::spawn_result(cx, async move {
            let mut config = clone_self.get_config_async(&connection_id).await
                .ok_or_else(|| anyhow::anyhow!("Connection not found: {}", connection_id))?;

            // Override database if specified
            if let Some(db) = database {
                config.database = Some(db);
            }

            clone_self.connection_manager.create_session(config, &clone_self.db_manager).await
                .map_err(|e| anyhow::anyhow!("{}", e))
        })
    }

    /// Execute SQL script (simplified - creates session per execution)
    pub fn execute_script<C>(
        &self,
        cx: &mut C,
        connection_id: String,
        script: String,
        database: Option<String>,
        opts: Option<ExecOptions>,
    ) -> C::Result<Task<anyhow::Result<Vec<SqlResult>>>>
    where
        C: AppContext
    {
        let clone_self = self.clone();
        Tokio::spawn_result(cx, async move {
            // 1. Get config
            let mut config = clone_self.get_config_async(&connection_id).await
                .ok_or_else(|| anyhow::anyhow!("Connection not found: {}", connection_id))?;

            if let Some(db) = database {
                config.database = Some(db);
            }

            // 2. Create session
            let session_id = clone_self.connection_manager
                .create_session(config.clone(), &clone_self.db_manager)
                .await?;

            // 3. Execute query on session
            let opts = opts.unwrap_or_default();
            let is_transactional = opts.transactional;
            let result = {
                let mut guard = clone_self.connection_manager.get_session_connection(&session_id).await?;
                let conn = guard.connection()
                    .ok_or_else(|| anyhow::anyhow!("Session connection not found"))?;
                conn.execute(&script, opts).await?
            };

            // 4. Detect transaction state
            let trimmed = script.trim().to_uppercase();
            let starts_transaction = trimmed.starts_with("BEGIN") || trimmed.starts_with("START TRANSACTION");
            let ends_transaction = trimmed.starts_with("COMMIT") || trimmed.starts_with("ROLLBACK");

            if starts_transaction || is_transactional {
                clone_self.connection_manager.set_transaction_state(&session_id, true).await;
            } else if ends_transaction {
                clone_self.connection_manager.set_transaction_state(&session_id, false).await;
                // Close session after transaction ends
                let _ = clone_self.connection_manager.close_session(&session_id).await;
            } else {
                // For non-transaction queries, close session immediately
                let _ = clone_self.connection_manager.close_session(&session_id).await;
            }

            Ok(result)
        })
    }

    /// Execute script with existing session (for transaction scenarios)
    pub fn execute_with_session<C>(
        &self,
        cx: &mut C,
        session_id: String,
        script: String,
        opts: Option<ExecOptions>,
    ) -> C::Result<Task<anyhow::Result<Vec<SqlResult>>>>
    where
        C: AppContext
    {
        let clone_self = self.clone();
        Tokio::spawn_result(cx, async move {
            // Execute query on session
            let opts = opts.unwrap_or_default();
            let is_transactional = opts.transactional;
            let result = {
                let mut guard = clone_self.connection_manager.get_session_connection(&session_id).await?;
                let conn = guard.connection()
                    .ok_or_else(|| anyhow::anyhow!("Session connection not found"))?;
                conn.execute(&script, opts).await?
            };

            // Update transaction state
            let trimmed = script.trim().to_uppercase();
            let starts_transaction = trimmed.starts_with("BEGIN") || trimmed.starts_with("START TRANSACTION");
            let ends_transaction = trimmed.starts_with("COMMIT") || trimmed.starts_with("ROLLBACK");

            if starts_transaction || is_transactional {
                clone_self.connection_manager.set_transaction_state(&session_id, true).await;
            } else if ends_transaction {
                clone_self.connection_manager.set_transaction_state(&session_id, false).await;
            }

            Ok(result)
        })
    }

    /// Get connection statistics
    pub fn stats<C>(
        &self,
        cx: &mut C,
    ) -> C::Result<Task<anyhow::Result<ConnectionStats>>>
    where
        C: AppContext
    {
        let clone_self = self.clone();
        Tokio::spawn_result(cx, async move {
            Ok(clone_self.connection_manager.stats().await)
        })
    }

    /// List all sessions for a connection
    pub fn list_sessions<C>(
        &self,
        cx: &mut C,
        connection_id: String,
    ) -> C::Result<Task<anyhow::Result<Vec<SessionInfo>>>>
    where
        C: AppContext
    {
        let clone_self = self.clone();
        Tokio::spawn_result(cx, async move {
            Ok(clone_self.connection_manager.list_sessions(&connection_id).await)
        })
    }

    /// Close a specific session
    pub fn close_session<C>(
        &self,
        cx: &mut C,
        session_id: String,
    ) -> C::Result<Task<anyhow::Result<()>>>
    where
        C: AppContext
    {
        let clone_self = self.clone();
        Tokio::spawn_result(cx, async move {
            clone_self.connection_manager.close_session(&session_id).await
                .map_err(|e| anyhow::anyhow!("{}", e))
        })
    }

    /// Disconnect all sessions for a connection
    pub fn disconnect_all<C>(
        &self,
        cx: &mut C,
        connection_id: String,
    ) -> C::Result<Task<anyhow::Result<()>>>
    where
        C: AppContext
    {
        let clone_self = self.clone();
        Tokio::spawn_result(cx, async move {
            clone_self.connection_manager.remove_all_sessions(&connection_id).await;
            Ok(())
        })
    }

    /// Query table data
    pub fn query_table_data<C>(
        &self,
        cx: &mut C,
        connection_id: String,
        request: crate::types::TableDataRequest,
    ) -> C::Result<Task<anyhow::Result<crate::types::TableDataResponse>>>
    where
        C: AppContext
    {
        let clone_self = self.clone();
        Tokio::spawn_result(cx, async move {
            let config = clone_self.get_config_async(&connection_id).await
                .ok_or_else(|| anyhow::anyhow!("Connection not found: {}", connection_id))?;
            
            let plugin = clone_self.get_plugin(&config.database_type)?;
            let mut connection = plugin.create_connection(config).await?;
            connection.connect().await?;
            
            plugin.query_table_data(&*connection, &request).await.map_err(|e| anyhow::anyhow!("{}", e))
        })
    }

    /// Load node children for tree view
    pub fn load_node_children<C>(
        &self,
        cx: &mut C,
        connection_id: String,
        node: DbNode,
        storage_state: GlobalStorageState,
    ) -> C::Result<Task<anyhow::Result<Vec<DbNode>>>>
    where
        C: AppContext
    {
        let clone_self = self.clone();
        Tokio::spawn_result(cx, async move {
            let config = clone_self.get_config_async(&connection_id).await
                .ok_or_else(|| anyhow::anyhow!("Connection not found: {}", connection_id))?;
            
            let plugin = clone_self.get_plugin(&config.database_type)?;
            let mut connection = plugin.create_connection(config).await?;
            connection.connect().await?;
            
            plugin.load_node_children(&*connection, &node, &storage_state).await.map_err(|e| anyhow::anyhow!("{}", e))
        })
    }

    /// Apply table changes
    pub fn apply_table_changes<C>(
        &self,
        cx: &mut C,
        connection_id: String,
        request: crate::types::TableSaveRequest,
    ) -> C::Result<Task<anyhow::Result<crate::types::TableSaveResponse>>>
    where
        C: AppContext
    {
        let clone_self = self.clone();
        Tokio::spawn_result(cx, async move {
            let config = clone_self.get_config_async(&connection_id).await
                .ok_or_else(|| anyhow::anyhow!("Connection not found: {}", connection_id))?;
            
            let plugin = clone_self.get_plugin(&config.database_type)?;
            let mut connection = plugin.create_connection(config).await?;
            connection.connect().await?;
            
            plugin.apply_table_changes(&*connection, request).await.map_err(|e| anyhow::anyhow!("{}", e))
        })
    }

    /// Generate table changes SQL
    pub fn generate_table_changes_sql<C>(
        &self,
        cx: &mut C,
        connection_id: String,
        request: crate::types::TableSaveRequest,
    ) -> C::Result<Task<anyhow::Result<String>>>
    where
        C: AppContext
    {
        let clone_self = self.clone();
        Tokio::spawn_result(cx, async move {
            if let Some(config) = clone_self.get_config_async(&connection_id).await {
                match clone_self.get_plugin(&config.database_type) {
                    Ok(plugin) => Ok(plugin.generate_table_changes_sql(&request)),
                    Err(_) => Ok("-- 无法获取数据库插件".to_string()),
                }
            } else {
                Ok("-- 连接不存在".to_string())
            }
        })
    }

    /// List databases
    pub fn list_databases<C>(
        &self,
        cx: &mut C,
        connection_id: String,
    ) -> C::Result<Task<anyhow::Result<Vec<String>>>>
    where
        C: AppContext
    {
        let clone_self = self.clone();
        Tokio::spawn_result(cx, async move {
            let config = clone_self.get_config_async(&connection_id).await
                .ok_or_else(|| anyhow::anyhow!("Connection not found: {}", connection_id))?;
            
            let plugin = clone_self.get_plugin(&config.database_type)?;
            let mut connection = plugin.create_connection(config).await?;
            connection.connect().await?;
            
            plugin.list_databases(&*connection).await.map_err(|e| anyhow::anyhow!("{}", e))
        })
    }

    /// List databases view
    pub fn list_databases_view<C>(
        &self,
        cx: &mut C,
        connection_id: String,
    ) -> C::Result<Task<anyhow::Result<crate::types::ObjectView>>>
    where
        C: AppContext
    {
        let clone_self = self.clone();
        Tokio::spawn_result(cx, async move {
            let config = clone_self.get_config_async(&connection_id).await
                .ok_or_else(|| anyhow::anyhow!("Connection not found: {}", connection_id))?;
            
            let plugin = clone_self.get_plugin(&config.database_type)?;
            let mut connection = plugin.create_connection(config).await?;
            connection.connect().await?;
            
            plugin.list_databases_view(&*connection).await.map_err(|e| anyhow::anyhow!("{}", e))
        })
    }

    /// List tables
    pub fn list_tables<C>(
        &self,
        cx: &mut C,
        connection_id: String,
        database: String,
    ) -> C::Result<Task<anyhow::Result<Vec<crate::types::TableInfo>>>>
    where
        C: AppContext
    {
        let clone_self = self.clone();
        Tokio::spawn_result(cx, async move {
            let config = clone_self.get_config_async(&connection_id).await
                .ok_or_else(|| anyhow::anyhow!("Connection not found: {}", connection_id))?;
            
            let plugin = clone_self.get_plugin(&config.database_type)?;
            let mut connection = plugin.create_connection(config).await?;
            connection.connect().await?;
            
            plugin.list_tables(&*connection, &database).await.map_err(|e| anyhow::anyhow!("{}", e))
        })
    }

    /// List tables view
    pub fn list_tables_view<C>(
        &self,
        cx: &mut C,
        connection_id: String,
        database: String,
    ) -> C::Result<Task<anyhow::Result<crate::types::ObjectView>>>
    where
        C: AppContext
    {
        let clone_self = self.clone();
        Tokio::spawn_result(cx, async move {
            let config = clone_self.get_config_async(&connection_id).await
                .ok_or_else(|| anyhow::anyhow!("Connection not found: {}", connection_id))?;
            
            let plugin = clone_self.get_plugin(&config.database_type)?;
            let mut connection = plugin.create_connection(config).await?;
            connection.connect().await?;
            
            plugin.list_tables_view(&*connection, &database).await.map_err(|e| anyhow::anyhow!("{}", e))
        })
    }

    /// List columns
    pub fn list_columns<C>(
        &self,
        cx: &mut C,
        connection_id: String,
        database: String,
        table: String,
    ) -> C::Result<Task<anyhow::Result<Vec<crate::types::ColumnInfo>>>>
    where
        C: AppContext
    {
        let clone_self = self.clone();
        Tokio::spawn_result(cx, async move {
            let config = clone_self.get_config_async(&connection_id).await
                .ok_or_else(|| anyhow::anyhow!("Connection not found: {}", connection_id))?;
            
            let plugin = clone_self.get_plugin(&config.database_type)?;
            let mut connection = plugin.create_connection(config).await?;
            connection.connect().await?;
            
            plugin.list_columns(&*connection, &database, &table).await.map_err(|e| anyhow::anyhow!("{}", e))
        })
    }

    /// List columns view
    pub fn list_columns_view<C>(
        &self,
        cx: &mut C,
        connection_id: String,
        database: String,
        table: String,
    ) -> C::Result<Task<anyhow::Result<crate::types::ObjectView>>>
    where
        C: AppContext
    {
        let clone_self = self.clone();
        Tokio::spawn_result(cx, async move {
            let config = clone_self.get_config_async(&connection_id).await
                .ok_or_else(|| anyhow::anyhow!("Connection not found: {}", connection_id))?;
            
            let plugin = clone_self.get_plugin(&config.database_type)?;
            let mut connection = plugin.create_connection(config).await?;
            connection.connect().await?;
            
            plugin.list_columns_view(&*connection, &database, &table).await.map_err(|e| anyhow::anyhow!("{}", e))
        })
    }

    /// List views
    pub fn list_views_view<C>(
        &self,
        cx: &mut C,
        connection_id: String,
        database: String,
    ) -> C::Result<Task<anyhow::Result<crate::types::ObjectView>>>
    where
        C: AppContext
    {
        let clone_self = self.clone();
        Tokio::spawn_result(cx, async move {
            let config = clone_self.get_config_async(&connection_id).await
                .ok_or_else(|| anyhow::anyhow!("Connection not found: {}", connection_id))?;
            
            let plugin = clone_self.get_plugin(&config.database_type)?;
            let mut connection = plugin.create_connection(config).await?;
            connection.connect().await?;
            
            plugin.list_views_view(&*connection, &database).await.map_err(|e| anyhow::anyhow!("{}", e))
        })
    }

    /// List functions view
    pub fn list_functions_view<C>(
        &self,
        cx: &mut C,
        connection_id: String,
        database: String,
    ) -> C::Result<Task<anyhow::Result<crate::types::ObjectView>>>
    where
        C: AppContext
    {
        let clone_self = self.clone();
        Tokio::spawn_result(cx, async move {
            let config = clone_self.get_config_async(&connection_id).await
                .ok_or_else(|| anyhow::anyhow!("Connection not found: {}", connection_id))?;
            
            let plugin = clone_self.get_plugin(&config.database_type)?;
            let mut connection = plugin.create_connection(config).await?;
            connection.connect().await?;
            
            plugin.list_functions_view(&*connection, &database).await.map_err(|e| anyhow::anyhow!("{}", e))
        })
    }

    /// List procedures view
    pub fn list_procedures_view<C>(
        &self,
        cx: &mut C,
        connection_id: String,
        database: String,
    ) -> C::Result<Task<anyhow::Result<crate::types::ObjectView>>>
    where
        C: AppContext
    {
        let clone_self = self.clone();
        Tokio::spawn_result(cx, async move {
            let config = clone_self.get_config_async(&connection_id).await
                .ok_or_else(|| anyhow::anyhow!("Connection not found: {}", connection_id))?;
            
            let plugin = clone_self.get_plugin(&config.database_type)?;
            let mut connection = plugin.create_connection(config).await?;
            connection.connect().await?;
            
            plugin.list_procedures_view(&*connection, &database).await.map_err(|e| anyhow::anyhow!("{}", e))
        })
    }

    /// List triggers view
    pub fn list_triggers_view<C>(
        &self,
        cx: &mut C,
        connection_id: String,
        database: String,
    ) -> C::Result<Task<anyhow::Result<crate::types::ObjectView>>>
    where
        C: AppContext
    {
        let clone_self = self.clone();
        Tokio::spawn_result(cx, async move {
            let config = clone_self.get_config_async(&connection_id).await
                .ok_or_else(|| anyhow::anyhow!("Connection not found: {}", connection_id))?;
            
            let plugin = clone_self.get_plugin(&config.database_type)?;
            let mut connection = plugin.create_connection(config).await?;
            connection.connect().await?;
            
            plugin.list_triggers_view(&*connection, &database).await.map_err(|e| anyhow::anyhow!("{}", e))
        })
    }

    /// List sequences view
    pub fn list_sequences_view<C>(
        &self,
        cx: &mut C,
        connection_id: String,
        database: String,
    ) -> C::Result<Task<anyhow::Result<crate::types::ObjectView>>>
    where
        C: AppContext
    {
        let clone_self = self.clone();
        Tokio::spawn_result(cx, async move {
            let config = clone_self.get_config_async(&connection_id).await
                .ok_or_else(|| anyhow::anyhow!("Connection not found: {}", connection_id))?;
            
            let plugin = clone_self.get_plugin(&config.database_type)?;
            let mut connection = plugin.create_connection(config).await?;
            connection.connect().await?;
            
            plugin.list_sequences_view(&*connection, &database).await.map_err(|e| anyhow::anyhow!("{}", e))
        })
    }

    /// Get completion info
    pub fn get_completion_info<C>(
        &self,
        cx: &mut C,
        connection_id: String,
    ) -> C::Result<Task<anyhow::Result<crate::plugin::SqlCompletionInfo>>>
    where
        C: AppContext
    {
        let clone_self = self.clone();
        Tokio::spawn_result(cx, async move {
            if let Some(config) = clone_self.get_config_async(&connection_id).await {
                match clone_self.get_plugin(&config.database_type) {
                    Ok(plugin) => Ok(plugin.get_completion_info()),
                    Err(_) => Ok(crate::plugin::SqlCompletionInfo::default()),
                }
            } else {
                Ok(crate::plugin::SqlCompletionInfo::default())
            }
        })
    }
}

impl Default for GlobalDbState {
    fn default() -> Self {
        Self::new()
    }
}

impl Global for GlobalDbState {}