use crate::connection::{DbConnection, DbError, StreamingProgress};
use crate::plugin::DatabasePlugin;
use crate::mysql::MySqlPlugin;
use crate::postgresql::PostgresPlugin;
use crate::{DbNode, ExecOptions, SqlResult};
use tokio::sync::mpsc;
use one_core::gpui_tokio::Tokio;
use one_core::storage::{DatabaseType, DbConnectionConfig, GlobalStorageState};
use gpui::{AppContext, AsyncApp, Global};
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
    /// Whether this session is currently checked out for use
    in_use: bool,
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
            in_use: false,
        }
    }

    fn mark_in_use(&mut self) {
        self.in_use = true;
        self.update_last_active();
    }

    fn release(&mut self) {
        self.in_use = false;
        self.update_last_active();
    }

    fn update_last_active(&mut self) {
        self.last_active = Instant::now();
    }

    /// Check if session is expired (idle timeout)
    fn is_expired(&self, timeout: Duration) -> bool {
        if self.in_use {
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

        if let Some(session_id) = self.try_acquire_session(&config).await {
            return Ok(session_id);
        }

        let session_id = self.generate_session_id(&config_id).await;

        // Create new connection
        let plugin = db_manager.get_plugin(&config.database_type)?;
        let mut connection = plugin.create_connection(config.clone()).await?;

        // Connect to database
        connection.connect().await?;
        info!("Created new session: {}", session_id);

        // Store session
        let mut session = ConnectionSession::new(connection, config, session_id.clone());
        session.mark_in_use();

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

    async fn try_acquire_session(&self, config: &DbConnectionConfig) -> Option<String> {
        let mut sessions = self.sessions.write().await;

        if let Some(session_list) = sessions.get_mut(&config.id) {
            if let Some(session) = session_list.iter_mut().find(|s| !s.in_use) {
                session.config = config.clone();
                session.mark_in_use();
                info!("Reusing session: {}", session.session_id);
                return Some(session.session_id.clone());
            }
        }

        None
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
                session.mark_in_use();
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

    pub async fn release_session(&self, session_id: &str) -> Result<(), DbError> {
        let mut sessions = self.sessions.write().await;

        for session_list in sessions.values_mut() {
            if let Some(session) = session_list.iter_mut().find(|s| s.session_id == session_id) {
                session.release();
                info!("Session {} released", session_id);
                return Ok(());
            }
        }

        Err(DbError::new(&format!("Session not found: {}", session_id)))
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
            session.release();
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
                        "Closing expired session {} for config {} (in_use: {}, idle: {}s, lifetime: {}s)",
                        session.session_id,
                        config_id,
                        session.in_use,
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
        let mut in_use_count = 0;

        for session_list in sessions.values() {
            total += session_list.len();
            in_use_count += session_list.iter().filter(|s| s.in_use).count();
        }

        ConnectionStats {
            total_sessions: total,
            active_sessions: in_use_count,
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
                    in_use: s.in_use,
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
    pub active_sessions: usize,
    pub configs_with_sessions: usize,
}

/// Session information
#[derive(Debug, Clone)]
pub struct SessionInfo {
    pub session_id: String,
    pub database: Option<String>,
    pub in_use: bool,
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


    fn wrapper_result(result: Vec<SqlResult>) -> anyhow::Result<SqlResult> {
        match result.into_iter().next() {
            Some(re) => Ok(re),
            None => Err(anyhow::anyhow!("No result returned")),
        }
    }
    
    pub async fn drop_database(
        &self,
        cx: &mut AsyncApp,
        config_id: String,
        database_name: String,
    ) -> anyhow::Result<SqlResult>
    {
        let config = self.get_config_async(&config_id).await
            .ok_or_else(|| anyhow::anyhow!("Connection not found: {}", config_id))?;
        let plugin = self.get_plugin(&config.database_type)?;
        let sql = plugin.drop_database(&database_name);

        let result = self.execute_with_session(cx, config, sql, None).await?;

        Self::wrapper_result(result)
    }

    /// Drop table
    pub async fn drop_table(
        &self,
        cx: &mut AsyncApp,
        config_id: String,
        database: String,
        table_name: String,
    ) ->anyhow::Result<SqlResult>
    {
        let config = self.get_config_async(&config_id).await
            .ok_or_else(|| anyhow::anyhow!("Connection not found: {}", config_id))?;
        let plugin = self.get_plugin(&config.database_type)?;
        let sql = plugin.drop_table(&database, &table_name);

        let result = self.execute_with_session(cx, config, sql, None).await?;

        Self::wrapper_result(result)
    }

    /// Truncate table
    pub async fn truncate_table(
        &self,
        cx: &mut AsyncApp,
        config_id: String,
        database: String,
        table_name: String,
    ) -> anyhow::Result<SqlResult>
    {
        let config = self.get_config_async(&config_id).await
            .ok_or_else(|| anyhow::anyhow!("Connection not found: {}", config_id))?;
        let plugin = self.get_plugin(&config.database_type)?;
        let sql = plugin.truncate_table(&database, &table_name);

        let result = self.execute_with_session(cx, config, sql, None).await?;

        Self::wrapper_result(result)
    }

    /// Rename table
    pub async fn rename_table(
        &self,
        cx: &mut AsyncApp,
        config_id: String,
        database: String,
        old_name: String,
        new_name: String,
    ) -> anyhow::Result<SqlResult>
    {
        let config = self.get_config_async(&config_id).await
            .ok_or_else(|| anyhow::anyhow!("Connection not found: {}", config_id))?;
        let plugin = self.get_plugin(&config.database_type)?;
        let sql = plugin.rename_table(&database, &old_name, &new_name);

        let result = self.execute_with_session(cx, config, sql, None).await?;

        Self::wrapper_result(result)
    }

    /// Drop view
    pub async fn drop_view(
        &self,
        cx: &mut AsyncApp,
        config_id: String,
        database: String,
        view_name: String,
    ) -> anyhow::Result<SqlResult>
    {
        let config = self.get_config_async(&config_id).await
            .ok_or_else(|| anyhow::anyhow!("Connection not found: {}", config_id))?;
        let plugin = self.get_plugin(&config.database_type)?;
        let sql = plugin.drop_view(&database, &view_name);

        let result = self.execute_with_session(cx, config, sql, None).await?;

        Self::wrapper_result(result)
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
    pub async fn unregister_connection(
        &self,
        cx: &mut AsyncApp,
        connection_id: String,
    ) -> anyhow::Result<()>
    {
        let clone_self = self.clone();
        Tokio::spawn_result(cx, async move {
            // Close all sessions for this connection
            clone_self.connection_manager.remove_all_sessions(&connection_id).await;

            // Remove from registry
            let mut connections = clone_self.connections.write().await;
            connections.remove(&connection_id);
            Ok(())
        })?.await
    }


    /// Get all registered connections
    pub async fn list_connections(
        &self,
        cx: &mut AsyncApp,
    ) -> anyhow::Result<Vec<DbConnectionConfig>>
    {
        let clone_self = self.clone();
        Tokio::spawn_result(cx, async move {
            let connections = clone_self.connections.read().await;
            Ok(connections.values().cloned().collect())
        })?.await
    }

    /// Create a new session for executing queries
    pub async fn create_session(
        &self,
        cx: &mut AsyncApp,
        connection_id: String,
        database: Option<String>,
    ) -> anyhow::Result<String>
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
        })?.await
    }

    /// Execute SQL  (simplified - creates session per execution)
    pub async fn execute_single(
        &self,
        cx: &mut AsyncApp,
        connection_id: String,
        script: String,
        database: Option<String>,
        opts: Option<ExecOptions>,
    ) -> anyhow::Result<SqlResult>
    {
        let result=  self.execute_script(cx, connection_id, script, database, opts).await?;
        Self::wrapper_result(result)
    }

    /// Execute SQL script (simplified - creates session per execution)
    pub async fn execute_script(
        &self,
        cx: &mut AsyncApp,
        connection_id: String,
        script: String,
        database: Option<String>,
        opts: Option<ExecOptions>,
    ) -> anyhow::Result<Vec<SqlResult>>
    {
        //  Get config
        let mut config = self.get_config_async(&connection_id).await
            .ok_or_else(|| anyhow::anyhow!("Connection not found: {}", connection_id))?;

        if let Some(db) = database {
            config.database = Some(db);
        }
        self.execute_with_session(cx, config, script, opts).await
    }

    /// Execute script with existing session (for transaction scenarios)
    pub async fn execute_with_session(
        &self,
        cx: &mut AsyncApp,
        config: DbConnectionConfig,
        script: String,
        opts: Option<ExecOptions>,
    ) -> anyhow::Result<Vec<SqlResult>>
    {
        let clone_self = self.clone();
        Tokio::spawn_result(cx, async move {
            // Create session
            let session_id = clone_self.connection_manager
                .create_session(config.clone(), &clone_self.db_manager)
                .await?;

            // Execute query on session
            let opts = opts.unwrap_or_default();
            let is_transactional = opts.transactional;

            let result = {
                let mut guard = clone_self.connection_manager.get_session_connection(&session_id).await?;
                let conn = guard.connection()
                    .ok_or_else(|| anyhow::anyhow!("Session connection not found"))?;
                conn.execute(&script, opts).await?
            };

            // Determine if session should stay open based on script content
            let upper_script = script.to_uppercase();
            let has_begin = upper_script.contains("BEGIN") || upper_script.contains("START TRANSACTION");
            let has_commit = upper_script.contains("COMMIT");
            let has_rollback = upper_script.contains("ROLLBACK");

            // Keep session open if: in transactional mode, or has BEGIN without COMMIT/ROLLBACK
            let keep_session = is_transactional || (has_begin && !has_commit && !has_rollback);

            if keep_session {
                // Release but don't close - session can be reused later
                clone_self.connection_manager.release_session(&session_id).await?;
            } else {
                // Close session completely
                clone_self.connection_manager.close_session(&session_id).await?;
            }

            Ok(result)
        })?.await
    }

    /// Execute SQL script with streaming progress
    /// Returns a receiver that will receive progress updates for each statement
    pub fn execute_script_streaming(
        &self,
        cx: &mut AsyncApp,
        connection_id: String,
        script: String,
        database: Option<String>,
        opts: Option<ExecOptions>,
    ) -> anyhow::Result<mpsc::Receiver<StreamingProgress>> {
        let (tx, rx) = mpsc::channel::<StreamingProgress>(100);

        let clone_self = self.clone();
        Tokio::spawn(cx, async move {
            let config_result = async {
                let mut config = clone_self.get_config_async(&connection_id).await
                    .ok_or_else(|| anyhow::anyhow!("Connection not found: {}", connection_id))?;

                if let Some(db) = database {
                    config.database = Some(db);
                }
                Ok::<_, anyhow::Error>(config)
            }.await;

            let config = match config_result {
                Ok(c) => c,
                Err(_) => return,
            };

            let session_result = clone_self.connection_manager
                .create_session(config.clone(), &clone_self.db_manager)
                .await;

            let session_id = match session_result {
                Ok(id) => id,
                Err(_) => return,
            };

            let opts = opts.unwrap_or_default();

            let exec_result = async {
                let mut guard = clone_self.connection_manager.get_session_connection(&session_id).await?;
                let conn = guard.connection()
                    .ok_or_else(|| anyhow::anyhow!("Session connection not found"))?;
                conn.execute_streaming(&script, opts, tx).await
                    .map_err(|e| anyhow::anyhow!("{}", e))?;
                Ok::<_, anyhow::Error>(())
            }.await;

            let _ = clone_self.connection_manager.close_session(&session_id).await;

            if let Err(e) = exec_result {
                error!("Streaming execution error: {}", e);
            }
        })?.detach();

        Ok(rx)
    }

    pub async fn with_session_connection<R, F>(
        &self,
        cx: &mut AsyncApp,
        config: DbConnectionConfig,
        f: F,
    ) -> anyhow::Result<R>
    where
        R: Send + 'static,
        F: FnOnce(&dyn DatabasePlugin, &mut (dyn DbConnection + Send + Sync)) -> anyhow::Result<R> + Send + 'static,
    {
        let clone_self = self.clone();
        Tokio::spawn_result(cx, async move {
            let plugin = clone_self.get_plugin(&config.database_type)?;
            let session_id = clone_self.connection_manager
                .create_session(config.clone(), &clone_self.db_manager)
                .await?;

            let result = {
                let mut guard = clone_self.connection_manager.get_session_connection(&session_id).await?;
                let conn = guard.connection()
                    .ok_or_else(|| anyhow::anyhow!("Session connection not found"))?;
                f(&*plugin, conn)
            };

            clone_self.connection_manager.close_session(&session_id).await?;

            result
        })?.await
    }

    /// Get connection statistics
    pub async fn stats(
        &self,
        cx: &mut AsyncApp,
    ) -> anyhow::Result<ConnectionStats>
    {
        let clone_self = self.clone();
        Tokio::spawn_result(cx, async move {
            Ok(clone_self.connection_manager.stats().await)
        })?.await
    }

    /// List all sessions for a connection
    pub async fn list_sessions(
        &self,
        cx: &mut AsyncApp,
        connection_id: String,
    ) -> anyhow::Result<Vec<SessionInfo>>
    {
        let clone_self = self.clone();
        Tokio::spawn_result(cx, async move {
            Ok(clone_self.connection_manager.list_sessions(&connection_id).await)
        })?.await
    }

    /// Close a specific session
    pub async fn close_session(
        &self,
        cx: &mut AsyncApp,
        session_id: String,
    ) -> anyhow::Result<()>
    {
        let clone_self = self.clone();
        Tokio::spawn_result(cx, async move {
            clone_self.connection_manager.close_session(&session_id).await
                .map_err(|e| anyhow::anyhow!("{}", e))
        })?.await
    }

    /// Disconnect all sessions for a connection
    pub async fn disconnect_all(
        &self,
        cx: &mut AsyncApp,
        connection_id: String,
    ) -> anyhow::Result<()>
    {
        let clone_self = self.clone();
        Tokio::spawn_result(cx, async move {
            clone_self.connection_manager.remove_all_sessions(&connection_id).await;
            Ok(())
        })?.await
    }

    /// Query table data
    pub async fn query_table_data(
        &self,
        cx: &mut AsyncApp,
        connection_id: String,
        request: crate::types::TableDataRequest,
    ) -> anyhow::Result<crate::types::TableDataResponse> {
        let config = self.get_config_async(&connection_id).await
            .ok_or_else(|| anyhow::anyhow!("Connection not found: {}", connection_id))?;

        let clone_self = self.clone();
        Tokio::spawn_result(cx, async move {
            let plugin = clone_self.get_plugin(&config.database_type)?;
            let session_id = clone_self.connection_manager
                .create_session(config.clone(), &clone_self.db_manager)
                .await?;

            let result = {
                let mut guard = clone_self.connection_manager.get_session_connection(&session_id).await?;
                let conn = guard.connection()
                    .ok_or_else(|| anyhow::anyhow!("Session connection not found"))?;
                plugin.query_table_data(&*conn, &request).await
                    .map_err(|e| anyhow::anyhow!("{}", e))
            };

            match result {
                Ok(value) => {
                    clone_self.connection_manager.release_session(&session_id).await
                        .map_err(|e| anyhow::anyhow!("{}", e))?;
                    Ok(value)
                }
                Err(err) => {
                    if let Err(release_err) = clone_self.connection_manager.release_session(&session_id).await {
                        warn!("Failed to release session {}: {}", session_id, release_err);
                    }
                    Err(err)
                }
            }
        })?.await
    }

    /// Load node children for tree view
    pub async fn load_node_children(
        &self,
        cx: &mut AsyncApp,
        connection_id: String,
        node: DbNode,
        storage_state: GlobalStorageState,
    ) -> anyhow::Result<Vec<DbNode>>
    {
        let config = self.get_config_async(&connection_id).await
            .ok_or_else(|| anyhow::anyhow!("Connection not found: {}", connection_id))?;

        let clone_self = self.clone();
        Tokio::spawn_result(cx, async move {
            let plugin = clone_self.get_plugin(&config.database_type)?;
            let session_id = clone_self.connection_manager
                .create_session(config.clone(), &clone_self.db_manager)
                .await?;

            let result = {
                let mut guard = clone_self.connection_manager.get_session_connection(&session_id).await?;
                let conn = guard.connection()
                    .ok_or_else(|| anyhow::anyhow!("Session connection not found"))?;
                plugin.load_node_children(&*conn, &node, &storage_state).await
                    .map_err(|e| anyhow::anyhow!("{}", e))
            };

            match result {
                Ok(value) => {
                    clone_self.connection_manager.release_session(&session_id).await
                        .map_err(|e| anyhow::anyhow!("{}", e))?;
                    Ok(value)
                }
                Err(err) => {
                    if let Err(release_err) = clone_self.connection_manager.release_session(&session_id).await {
                        warn!("Failed to release session {}: {}", session_id, release_err);
                    }
                    Err(err)
                }
            }
        })?.await
    }

    /// Apply table changes
    pub async fn apply_table_changes(
        &self,
        cx: &mut AsyncApp,
        connection_id: String,
        request: crate::types::TableSaveRequest,
    ) -> anyhow::Result<crate::types::TableSaveResponse>
    {
        let config = self.get_config_async(&connection_id).await
            .ok_or_else(|| anyhow::anyhow!("Connection not found: {}", connection_id))?;

        let clone_self = self.clone();
        Tokio::spawn_result(cx, async move {
            let plugin = clone_self.get_plugin(&config.database_type)?;
            let session_id = clone_self.connection_manager
                .create_session(config.clone(), &clone_self.db_manager)
                .await?;

            let result = {
                let mut guard = clone_self.connection_manager.get_session_connection(&session_id).await?;
                let conn = guard.connection()
                    .ok_or_else(|| anyhow::anyhow!("Session connection not found"))?;
                plugin.apply_table_changes(&*conn, request).await
                    .map_err(|e| anyhow::anyhow!("{}", e))
            };

            match result {
                Ok(value) => {
                    clone_self.connection_manager.release_session(&session_id).await
                        .map_err(|e| anyhow::anyhow!("{}", e))?;
                    Ok(value)
                }
                Err(err) => {
                    if let Err(release_err) = clone_self.connection_manager.release_session(&session_id).await {
                        warn!("Failed to release session {}: {}", session_id, release_err);
                    }
                    Err(err)
                }
            }
        })?.await
    }

    /// Generate table changes SQL
    pub async fn generate_table_changes_sql(
        &self,
        cx: &mut AsyncApp,
        connection_id: String,
        request: crate::types::TableSaveRequest,
    ) -> anyhow::Result<String>
    {
        let _ = cx;
        let config = self.get_config_async(&connection_id).await
            .ok_or_else(|| anyhow::anyhow!("Connection not found: {}", connection_id))?;

        match self.get_plugin(&config.database_type) {
            Ok(plugin) => Ok(plugin.generate_table_changes_sql(&request)),
            Err(_) => Ok("-- 无法获取数据库插件".to_string()),
        }
    }

    /// List databases
    pub async fn list_databases(
        &self,
        cx: &mut AsyncApp,
        connection_id: String,
    ) -> anyhow::Result<Vec<String>>
    {
        let config = self.get_config_async(&connection_id).await
            .ok_or_else(|| anyhow::anyhow!("Connection not found: {}", connection_id))?;

        let clone_self = self.clone();
        Tokio::spawn_result(cx, async move {
            let plugin = clone_self.get_plugin(&config.database_type)?;
            let session_id = clone_self.connection_manager
                .create_session(config.clone(), &clone_self.db_manager)
                .await?;

            let result = {
                let mut guard = clone_self.connection_manager.get_session_connection(&session_id).await?;
                let conn = guard.connection()
                    .ok_or_else(|| anyhow::anyhow!("Session connection not found"))?;
                plugin.list_databases(&*conn).await
                    .map_err(|e| anyhow::anyhow!("{}", e))
            };

            match result {
                Ok(value) => {
                    clone_self.connection_manager.release_session(&session_id).await
                        .map_err(|e| anyhow::anyhow!("{}", e))?;
                    Ok(value)
                }
                Err(err) => {
                    if let Err(release_err) = clone_self.connection_manager.release_session(&session_id).await {
                        warn!("Failed to release session {}: {}", session_id, release_err);
                    }
                    Err(err)
                }
            }
        })?.await
    }

    /// List databases view
    pub async fn list_databases_view(
        &self,
        cx: &mut AsyncApp,
        connection_id: String,
    ) -> anyhow::Result<crate::types::ObjectView>
    {
        let config = self.get_config_async(&connection_id).await
            .ok_or_else(|| anyhow::anyhow!("Connection not found: {}", connection_id))?;

        let clone_self = self.clone();
        Tokio::spawn_result(cx, async move {
            let plugin = clone_self.get_plugin(&config.database_type)?;
            let session_id = clone_self.connection_manager
                .create_session(config.clone(), &clone_self.db_manager)
                .await?;

            let result = {
                let mut guard = clone_self.connection_manager.get_session_connection(&session_id).await?;
                let conn = guard.connection()
                    .ok_or_else(|| anyhow::anyhow!("Session connection not found"))?;
                plugin.list_databases_view(&*conn).await
                    .map_err(|e| anyhow::anyhow!("{}", e))
            };

            match result {
                Ok(value) => {
                    clone_self.connection_manager.release_session(&session_id).await
                        .map_err(|e| anyhow::anyhow!("{}", e))?;
                    Ok(value)
                }
                Err(err) => {
                    if let Err(release_err) = clone_self.connection_manager.release_session(&session_id).await {
                        warn!("Failed to release session {}: {}", session_id, release_err);
                    }
                    Err(err)
                }
            }
        })?.await
    }

    /// List tables
    pub async fn list_tables(
        &self,
        cx: &mut AsyncApp,
        connection_id: String,
        database: String,
    ) -> anyhow::Result<Vec<crate::types::TableInfo>>
    {
        let config = self.get_config_async(&connection_id).await
            .ok_or_else(|| anyhow::anyhow!("Connection not found: {}", connection_id))?;

        let clone_self = self.clone();
        Tokio::spawn_result(cx, async move {
            let plugin = clone_self.get_plugin(&config.database_type)?;
            let session_id = clone_self.connection_manager
                .create_session(config.clone(), &clone_self.db_manager)
                .await?;

            let result = {
                let mut guard = clone_self.connection_manager.get_session_connection(&session_id).await?;
                let conn = guard.connection()
                    .ok_or_else(|| anyhow::anyhow!("Session connection not found"))?;
                plugin.list_tables(&*conn, &database).await
                    .map_err(|e| anyhow::anyhow!("{}", e))
            };

            match result {
                Ok(value) => {
                    clone_self.connection_manager.release_session(&session_id).await
                        .map_err(|e| anyhow::anyhow!("{}", e))?;
                    Ok(value)
                }
                Err(err) => {
                    if let Err(release_err) = clone_self.connection_manager.release_session(&session_id).await {
                        warn!("Failed to release session {}: {}", session_id, release_err);
                    }
                    Err(err)
                }
            }
        })?.await
    }

    /// List tables view
    pub async fn list_tables_view(
        &self,
        cx: &mut AsyncApp,
        connection_id: String,
        database: String,
    ) -> anyhow::Result<crate::types::ObjectView>
    {
        let config = self.get_config_async(&connection_id).await
            .ok_or_else(|| anyhow::anyhow!("Connection not found: {}", connection_id))?;

        let clone_self = self.clone();
        Tokio::spawn_result(cx, async move {
            let plugin = clone_self.get_plugin(&config.database_type)?;
            let session_id = clone_self.connection_manager
                .create_session(config.clone(), &clone_self.db_manager)
                .await?;

            let result = {
                let mut guard = clone_self.connection_manager.get_session_connection(&session_id).await?;
                let conn = guard.connection()
                    .ok_or_else(|| anyhow::anyhow!("Session connection not found"))?;
                plugin.list_tables_view(&*conn, &database).await
                    .map_err(|e| anyhow::anyhow!("{}", e))
            };

            match result {
                Ok(value) => {
                    clone_self.connection_manager.release_session(&session_id).await
                        .map_err(|e| anyhow::anyhow!("{}", e))?;
                    Ok(value)
                }
                Err(err) => {
                    if let Err(release_err) = clone_self.connection_manager.release_session(&session_id).await {
                        warn!("Failed to release session {}: {}", session_id, release_err);
                    }
                    Err(err)
                }
            }
        })?.await
    }

    /// List columns
    pub async fn list_columns(
        &self,
        cx: &mut AsyncApp,
        connection_id: String,
        database: String,
        table: String,
    ) -> anyhow::Result<Vec<crate::types::ColumnInfo>>
    {
        let config = self.get_config_async(&connection_id).await
            .ok_or_else(|| anyhow::anyhow!("Connection not found: {}", connection_id))?;

        let clone_self = self.clone();
        Tokio::spawn_result(cx, async move {
            let plugin = clone_self.get_plugin(&config.database_type)?;
            let session_id = clone_self.connection_manager
                .create_session(config.clone(), &clone_self.db_manager)
                .await?;

            let result = {
                let mut guard = clone_self.connection_manager.get_session_connection(&session_id).await?;
                let conn = guard.connection()
                    .ok_or_else(|| anyhow::anyhow!("Session connection not found"))?;
                plugin.list_columns(&*conn, &database, &table).await
                    .map_err(|e| anyhow::anyhow!("{}", e))
            };

            match result {
                Ok(value) => {
                    clone_self.connection_manager.release_session(&session_id).await
                        .map_err(|e| anyhow::anyhow!("{}", e))?;
                    Ok(value)
                }
                Err(err) => {
                    if let Err(release_err) = clone_self.connection_manager.release_session(&session_id).await {
                        warn!("Failed to release session {}: {}", session_id, release_err);
                    }
                    Err(err)
                }
            }
        })?.await
    }

    /// List columns view
    pub async fn list_columns_view(
        &self,
        cx: &mut AsyncApp,
        connection_id: String,
        database: String,
        table: String,
    ) -> anyhow::Result<crate::types::ObjectView>
    {
        let config = self.get_config_async(&connection_id).await
            .ok_or_else(|| anyhow::anyhow!("Connection not found: {}", connection_id))?;

        let clone_self = self.clone();
        Tokio::spawn_result(cx, async move {
            let plugin = clone_self.get_plugin(&config.database_type)?;
            let session_id = clone_self.connection_manager
                .create_session(config.clone(), &clone_self.db_manager)
                .await?;

            let result = {
                let mut guard = clone_self.connection_manager.get_session_connection(&session_id).await?;
                let conn = guard.connection()
                    .ok_or_else(|| anyhow::anyhow!("Session connection not found"))?;
                plugin.list_columns_view(&*conn, &database, &table).await
                    .map_err(|e| anyhow::anyhow!("{}", e))
            };

            match result {
                Ok(value) => {
                    clone_self.connection_manager.release_session(&session_id).await
                        .map_err(|e| anyhow::anyhow!("{}", e))?;
                    Ok(value)
                }
                Err(err) => {
                    if let Err(release_err) = clone_self.connection_manager.release_session(&session_id).await {
                        warn!("Failed to release session {}: {}", session_id, release_err);
                    }
                    Err(err)
                }
            }
        })?.await
    }

    /// List indexes
    pub async fn list_indexes(
        &self,
        cx: &mut AsyncApp,
        connection_id: String,
        database: String,
        table: String,
    ) -> anyhow::Result<Vec<crate::types::IndexInfo>>
    {
        let config = self.get_config_async(&connection_id).await
            .ok_or_else(|| anyhow::anyhow!("Connection not found: {}", connection_id))?;

        let clone_self = self.clone();
        Tokio::spawn_result(cx, async move {
            let plugin = clone_self.get_plugin(&config.database_type)?;
            let session_id = clone_self.connection_manager
                .create_session(config.clone(), &clone_self.db_manager)
                .await?;

            let result = {
                let mut guard = clone_self.connection_manager.get_session_connection(&session_id).await?;
                let conn = guard.connection()
                    .ok_or_else(|| anyhow::anyhow!("Session connection not found"))?;
                plugin.list_indexes(&*conn, &database, &table).await
                    .map_err(|e| anyhow::anyhow!("{}", e))
            };

            match result {
                Ok(value) => {
                    clone_self.connection_manager.release_session(&session_id).await
                        .map_err(|e| anyhow::anyhow!("{}", e))?;
                    Ok(value)
                }
                Err(err) => {
                    if let Err(release_err) = clone_self.connection_manager.release_session(&session_id).await {
                        warn!("Failed to release session {}: {}", session_id, release_err);
                    }
                    Err(err)
                }
            }
        })?.await
    }

    /// List views
    pub async fn list_views_view(
        &self,
        cx: &mut AsyncApp,
        connection_id: String,
        database: String,
    ) -> anyhow::Result<crate::types::ObjectView>
    {
        let config = self.get_config_async(&connection_id).await
            .ok_or_else(|| anyhow::anyhow!("Connection not found: {}", connection_id))?;

        let clone_self = self.clone();
        Tokio::spawn_result(cx, async move {
            let plugin = clone_self.get_plugin(&config.database_type)?;
            let session_id = clone_self.connection_manager
                .create_session(config.clone(), &clone_self.db_manager)
                .await?;

            let result = {
                let mut guard = clone_self.connection_manager.get_session_connection(&session_id).await?;
                let conn = guard.connection()
                    .ok_or_else(|| anyhow::anyhow!("Session connection not found"))?;
                plugin.list_views_view(&*conn, &database).await
                    .map_err(|e| anyhow::anyhow!("{}", e))
            };

            match result {
                Ok(value) => {
                    clone_self.connection_manager.release_session(&session_id).await
                        .map_err(|e| anyhow::anyhow!("{}", e))?;
                    Ok(value)
                }
                Err(err) => {
                    if let Err(release_err) = clone_self.connection_manager.release_session(&session_id).await {
                        warn!("Failed to release session {}: {}", session_id, release_err);
                    }
                    Err(err)
                }
            }
        })?.await
    }

    /// List functions view
    pub async fn list_functions_view(
        &self,
        cx: &mut AsyncApp,
        connection_id: String,
        database: String,
    ) -> anyhow::Result<crate::types::ObjectView>
    {
        let config = self.get_config_async(&connection_id).await
            .ok_or_else(|| anyhow::anyhow!("Connection not found: {}", connection_id))?;

        let clone_self = self.clone();
        Tokio::spawn_result(cx, async move {
            let plugin = clone_self.get_plugin(&config.database_type)?;
            let session_id = clone_self.connection_manager
                .create_session(config.clone(), &clone_self.db_manager)
                .await?;

            let result = {
                let mut guard = clone_self.connection_manager.get_session_connection(&session_id).await?;
                let conn = guard.connection()
                    .ok_or_else(|| anyhow::anyhow!("Session connection not found"))?;
                plugin.list_functions_view(&*conn, &database).await
                    .map_err(|e| anyhow::anyhow!("{}", e))
            };

            match result {
                Ok(value) => {
                    clone_self.connection_manager.release_session(&session_id).await
                        .map_err(|e| anyhow::anyhow!("{}", e))?;
                    Ok(value)
                }
                Err(err) => {
                    if let Err(release_err) = clone_self.connection_manager.release_session(&session_id).await {
                        warn!("Failed to release session {}: {}", session_id, release_err);
                    }
                    Err(err)
                }
            }
        })?.await
    }

    /// List procedures view
    pub async fn list_procedures_view(
        &self,
        cx: &mut AsyncApp,
        connection_id: String,
        database: String,
    ) -> anyhow::Result<crate::types::ObjectView>
    {
        let config = self.get_config_async(&connection_id).await
            .ok_or_else(|| anyhow::anyhow!("Connection not found: {}", connection_id))?;

        let clone_self = self.clone();
        Tokio::spawn_result(cx, async move {
            let plugin = clone_self.get_plugin(&config.database_type)?;
            let session_id = clone_self.connection_manager
                .create_session(config.clone(), &clone_self.db_manager)
                .await?;

            let result = {
                let mut guard = clone_self.connection_manager.get_session_connection(&session_id).await?;
                let conn = guard.connection()
                    .ok_or_else(|| anyhow::anyhow!("Session connection not found"))?;
                plugin.list_procedures_view(&*conn, &database).await
                    .map_err(|e| anyhow::anyhow!("{}", e))
            };

            match result {
                Ok(value) => {
                    clone_self.connection_manager.release_session(&session_id).await
                        .map_err(|e| anyhow::anyhow!("{}", e))?;
                    Ok(value)
                }
                Err(err) => {
                    if let Err(release_err) = clone_self.connection_manager.release_session(&session_id).await {
                        warn!("Failed to release session {}: {}", session_id, release_err);
                    }
                    Err(err)
                }
            }
        })?.await
    }

    /// List triggers view
    pub async fn list_triggers_view(
        &self,
        cx: &mut AsyncApp,
        connection_id: String,
        database: String,
    ) -> anyhow::Result<crate::types::ObjectView>
    {
        let config = self.get_config_async(&connection_id).await
            .ok_or_else(|| anyhow::anyhow!("Connection not found: {}", connection_id))?;

        let clone_self = self.clone();
        Tokio::spawn_result(cx, async move {
            let plugin = clone_self.get_plugin(&config.database_type)?;
            let session_id = clone_self.connection_manager
                .create_session(config.clone(), &clone_self.db_manager)
                .await?;

            let result = {
                let mut guard = clone_self.connection_manager.get_session_connection(&session_id).await?;
                let conn = guard.connection()
                    .ok_or_else(|| anyhow::anyhow!("Session connection not found"))?;
                plugin.list_triggers_view(&*conn, &database).await
                    .map_err(|e| anyhow::anyhow!("{}", e))
            };

            match result {
                Ok(value) => {
                    clone_self.connection_manager.release_session(&session_id).await
                        .map_err(|e| anyhow::anyhow!("{}", e))?;
                    Ok(value)
                }
                Err(err) => {
                    if let Err(release_err) = clone_self.connection_manager.release_session(&session_id).await {
                        warn!("Failed to release session {}: {}", session_id, release_err);
                    }
                    Err(err)
                }
            }
        })?.await
    }

    /// List sequences view
    pub async fn list_sequences_view(
        &self,
        cx: &mut AsyncApp,
        connection_id: String,
        database: String,
    ) -> anyhow::Result<crate::types::ObjectView>
    {
        let config = self.get_config_async(&connection_id).await
            .ok_or_else(|| anyhow::anyhow!("Connection not found: {}", connection_id))?;

        let clone_self = self.clone();
        Tokio::spawn_result(cx, async move {
            let plugin = clone_self.get_plugin(&config.database_type)?;
            let session_id = clone_self.connection_manager
                .create_session(config.clone(), &clone_self.db_manager)
                .await?;

            let result = {
                let mut guard = clone_self.connection_manager.get_session_connection(&session_id).await?;
                let conn = guard.connection()
                    .ok_or_else(|| anyhow::anyhow!("Session connection not found"))?;
                plugin.list_sequences_view(&*conn, &database).await
                    .map_err(|e| anyhow::anyhow!("{}", e))
            };

            match result {
                Ok(value) => {
                    clone_self.connection_manager.release_session(&session_id).await
                        .map_err(|e| anyhow::anyhow!("{}", e))?;
                    Ok(value)
                }
                Err(err) => {
                    if let Err(release_err) = clone_self.connection_manager.release_session(&session_id).await {
                        warn!("Failed to release session {}: {}", session_id, release_err);
                    }
                    Err(err)
                }
            }
        })?.await
    }

    /// Get completion info
    pub async fn get_completion_info(
        &self,
        cx: &mut AsyncApp,
        connection_id: String,
    ) -> anyhow::Result<crate::plugin::SqlCompletionInfo>
    {
        let _ = cx;
        if let Some(config) = self.get_config_async(&connection_id).await {
            match self.get_plugin(&config.database_type) {
                Ok(plugin) => Ok(plugin.get_completion_info()),
                Err(_) => Ok(crate::plugin::SqlCompletionInfo::default()),
            }
        } else {
            Ok(crate::plugin::SqlCompletionInfo::default())
        }
    }
}

impl Default for GlobalDbState {
    fn default() -> Self {
        Self::new()
    }
}

impl Global for GlobalDbState {}