use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use one_core::storage::DbConnectionConfig;
use tokio::sync::Mutex;
use tiberius::{Client, Config, AuthMethod, Row};
use tokio::net::TcpStream;
use tokio_util::compat::{TokioAsyncWriteCompatExt, Compat};
use tokio::sync::mpsc;
use tracing::{debug, error, info};

use crate::connection::{DbConnection, DbError, StreamingProgress};
use crate::executor::{
    ExecOptions, ExecResult, QueryResult, SqlErrorInfo, SqlResult,
    SqlStatementClassifier,
};
use crate::{DatabasePlugin, SqlValue};

pub struct MssqlDbConnection {
    config: DbConnectionConfig,
    client: Arc<Mutex<Option<Client<Compat<TcpStream>>>>>,
}

impl MssqlDbConnection {
    pub fn new(config: DbConnectionConfig) -> Self {
        Self {
            config,
            client: Arc::new(Mutex::new(None)),
        }
    }

    /// Extract value from MSSQL row
    fn extract_value(row: &Row, index: usize) -> Option<String> {
        // Try different types
        row.try_get::<&str, _>(index)
            .ok()
            .flatten()
            .map(|s| s.to_string())
            .or_else(|| {
                row.try_get::<i32, _>(index)
                    .ok()
                    .flatten()
                    .map(|v| v.to_string())
            })
            .or_else(|| {
                row.try_get::<i64, _>(index)
                    .ok()
                    .flatten()
                    .map(|v| v.to_string())
            })
            .or_else(|| {
                row.try_get::<f64, _>(index)
                    .ok()
                    .flatten()
                    .map(|v| v.to_string())
            })
            .or_else(|| {
                row.try_get::<bool, _>(index)
                    .ok()
                    .flatten()
                    .map(|v| v.to_string())
            })
            .or_else(|| {
                // Try chrono types
                use chrono::{NaiveDateTime, NaiveDate, NaiveTime};

                row.try_get::<NaiveDateTime, _>(index)
                    .ok()
                    .flatten()
                    .map(|v| v.format("%Y-%m-%d %H:%M:%S").to_string())
                    .or_else(|| {
                        row.try_get::<NaiveDate, _>(index)
                            .ok()
                            .flatten()
                            .map(|v| v.format("%Y-%m-%d").to_string())
                    })
                    .or_else(|| {
                        row.try_get::<NaiveTime, _>(index)
                            .ok()
                            .flatten()
                            .map(|v| v.format("%H:%M:%S").to_string())
                    })
            })
    }

    fn apply_max_rows_limit(sql: &str, max_rows: Option<usize>) -> String {
        if let Some(max) = max_rows {
            if SqlStatementClassifier::is_query_statement(sql)
                && !sql.to_uppercase().contains(" TOP ")
            {
                // MSSQL uses TOP instead of LIMIT
                // Try to insert TOP after SELECT
                let upper = sql.to_uppercase();
                if let Some(pos) = upper.find("SELECT") {
                    let (before, after) = sql.split_at(pos + 6); // "SELECT" has 6 chars
                    return format!("{} TOP {}{}", before, max, after);
                }
            }
        }
        sql.to_string()
    }

    fn rows_to_query_result(
        columns: Vec<String>,
        rows: Vec<Row>,
        sql: String,
        elapsed_ms: u128,
        table_name: Option<String>,
    ) -> SqlResult {
        debug!("[MSSQL] Query returned {} rows, {} columns: {:?}",
            rows.len(), columns.len(), columns);

        let all_rows: Vec<Vec<Option<String>>> = rows
            .iter()
            .map(|row| {
                (0..columns.len())
                    .map(|i| Self::extract_value(row, i))
                    .collect()
            })
            .collect();

        let editable = table_name.is_some();
        SqlResult::Query(QueryResult {
            sql,
            columns,
            rows: all_rows,
            elapsed_ms,
            table_name,
            editable,
        })
    }

    fn build_exec_result(sql: String, rows_affected: u64, elapsed_ms: u128) -> SqlResult {
        let message = SqlStatementClassifier::format_message(&sql, rows_affected);
        SqlResult::Exec(ExecResult {
            sql,
            rows_affected,
            elapsed_ms,
            message: Some(message),
        })
    }

    async fn execute_single(
        client: &mut Client<Compat<TcpStream>>,
        sql: &str,
        is_query: bool,
    ) -> Result<SqlResult, DbError> {
        let start = Instant::now();
        let sql_string = sql.to_string();

        if is_query {
            let table_name = SqlStatementClassifier::analyze_select_editability(sql);

            match client.query(sql, &[]).await {
                Ok(mut stream) => {
                    let columns: Vec<String> = match stream.columns().await {
                        Ok(Some(cols)) => cols.iter().map(|c| c.name().to_string()).collect(),
                        _ => Vec::new(),
                    };
                    debug!("[MSSQL] Got {} columns from stream: {:?}", columns.len(), columns);

                    match stream.into_first_result().await {
                        Ok(rows) => {
                            let elapsed_ms = start.elapsed().as_millis();
                            Ok(Self::rows_to_query_result(columns, rows, sql_string, elapsed_ms, table_name))
                        }
                        Err(e) => Ok(SqlResult::Error(SqlErrorInfo {
                            sql: sql_string,
                            message: e.to_string(),
                        })),
                    }
                }
                Err(e) => Ok(SqlResult::Error(SqlErrorInfo {
                    sql: sql_string,
                    message: e.to_string(),
                })),
            }
        } else {
            match client.execute(sql, &[]).await {
                Ok(result) => {
                    let elapsed_ms = start.elapsed().as_millis();
                    let rows_affected = result.total();
                    Ok(Self::build_exec_result(sql_string, rows_affected, elapsed_ms))
                }
                Err(e) => Ok(SqlResult::Error(SqlErrorInfo {
                    sql: sql_string,
                    message: e.to_string(),
                })),
            }
        }
    }
}

#[async_trait]
impl DbConnection for MssqlDbConnection {
    fn config(&self) -> &DbConnectionConfig {
        &self.config
    }

    fn set_config_database(&mut self, database: Option<String>) {
        self.config.database = database;
    }

    async fn connect(&mut self) -> Result<(), DbError> {
        let config = &self.config;
        info!("[MSSQL] Connecting to {}:{}", config.host, config.port);

        let mut tiberius_config = Config::new();
        tiberius_config.host(&config.host);
        tiberius_config.port(config.port);
        tiberius_config.authentication(AuthMethod::sql_server(&config.username, &config.password));

        // Trust certificate (default: true)
        if config.get_param("trust_cert").map(|v| v != "false").unwrap_or(true) {
            tiberius_config.trust_cert();
        }

        // Encryption level: off, on, required (default: off for compatibility)
        let encrypt = config.get_param("encrypt").map(|s| s.as_str()).unwrap_or("off");
        match encrypt {
            "on" => tiberius_config.encryption(tiberius::EncryptionLevel::On),
            "required" => tiberius_config.encryption(tiberius::EncryptionLevel::Required),
            _ => tiberius_config.encryption(tiberius::EncryptionLevel::NotSupported),
        };

        // Application name
        if let Some(app_name) = config.get_param("application_name") {
            tiberius_config.application_name(app_name);
        }

        if let Some(ref db) = config.database {
            tiberius_config.database(db);
            debug!("[MSSQL] Using database: {}", db);
        }

        // Connection timeout (default: 30 seconds)
        let connect_timeout = config.get_param_as::<u64>("connect_timeout").unwrap_or(30);
        debug!("[MSSQL] Connect timeout: {}s", connect_timeout);

        debug!("[MSSQL] Establishing TCP connection...");
        let tcp = tokio::time::timeout(
            std::time::Duration::from_secs(connect_timeout),
            TcpStream::connect(tiberius_config.get_addr())
        )
            .await
            .map_err(|_| DbError::ConnectionError("Connection timeout".to_string()))?
            .map_err(|e| {
                error!("[MSSQL] TCP connection failed: {}", e);
                DbError::ConnectionError(format!("Failed to connect to TCP: {}", e))
            })?;
        debug!("[MSSQL] TCP connection established");

        debug!("[MSSQL] Authenticating with SQL Server...");
        let client = Client::connect(tiberius_config, tcp.compat_write())
            .await
            .map_err(|e| {
                error!("[MSSQL] Authentication failed: {}", e);
                DbError::ConnectionError(format!("Failed to connect to MSSQL: {}", e))
            })?;
        info!("[MSSQL] Connected successfully");

        {
            let mut guard = self.client.lock().await;
            *guard = Some(client);
        }

        Ok(())
    }

    async fn current_database(&self) -> Result<Option<String>, DbError> {
        let mut guard = self.client.lock().await;
        let client = guard.as_mut()
            .ok_or_else(|| DbError::ConnectionError("Not connected".into()))?;

        let result = match client.query("SELECT DB_NAME()", &[]).await {
            Ok(stream) => {
                match stream.into_first_result().await {
                    Ok(rows) => {
                        if let Some(row) = rows.first() {
                            row.try_get::<&str, _>(0).ok().flatten().map(|s| s.to_string())
                        } else {
                            None
                        }
                    }
                    Err(_) => None,
                }
            }
            Err(_) => None,
        };
        Ok(result)
    }

    async fn disconnect(&mut self) -> Result<(), DbError> {
        let mut guard = self.client.lock().await;
        *guard = None;
        Ok(())
    }

    async fn execute(&self, plugin: Arc<dyn DatabasePlugin>, script: &str, options: ExecOptions) -> Result<Vec<SqlResult>, DbError> {
        let mut guard = self.client.lock().await;
        let client = guard.as_mut()
            .ok_or_else(|| DbError::ConnectionError("Not connected to database".to_string()))?;

        let statements = plugin.split_statements(script);
        let mut results = Vec::new();

        // MSSQL supports transactions
        if options.transactional {
            // Begin transaction
            match client.execute("BEGIN TRANSACTION", &[]).await {
                Ok(_) => {},
                Err(e) => return Err(DbError::QueryError(format!("Failed to begin transaction: {}", e))),
            }

            for sql in statements {
                let sql = sql.trim();
                if sql.is_empty() {
                    continue;
                }

                let modified_sql = Self::apply_max_rows_limit(sql, options.max_rows);
                let is_query = plugin.is_query_statement(&modified_sql);
                let result = Self::execute_single(client, &modified_sql, is_query).await?;

                let is_error = result.is_error();
                results.push(result);

                if is_error {
                    break;
                }
            }

            // Commit or rollback
            let has_error = results.iter().any(|r| r.is_error());
            if has_error {
                client.execute("ROLLBACK", &[]).await
                    .map_err(|e| DbError::QueryError(format!("Failed to rollback: {}", e)))?;
            } else {
                client.execute("COMMIT", &[]).await
                    .map_err(|e| DbError::QueryError(format!("Failed to commit: {}", e)))?;
            }
        } else {
            for sql in statements {
                let sql = sql.trim();
                if sql.is_empty() {
                    continue;
                }

                let modified_sql = Self::apply_max_rows_limit(sql, options.max_rows);
                let is_query = plugin.is_query_statement(&modified_sql);
                let result = Self::execute_single(client, &modified_sql, is_query).await?;

                let is_error = result.is_error();
                results.push(result);

                if is_error && options.stop_on_error {
                    break;
                }
            }
        }

        Ok(results)
    }

    async fn query(
        &self,
        query: &str,
        _params: Option<Vec<SqlValue>>,
        _options: ExecOptions,
    ) -> Result<SqlResult, DbError> {
        debug!("[MSSQL] Acquiring client lock...");
        let mut guard = self.client.lock().await;
        let client = guard.as_mut()
            .ok_or_else(|| DbError::ConnectionError("Not connected to database".to_string()))?;
        debug!("[MSSQL] Lock acquired");

        let start = Instant::now();
        let is_query = SqlStatementClassifier::is_query_statement(query);
        let query_string = query.to_string();
        debug!("[MSSQL] Executing query: {}", &query_string[..query_string.len().min(100)]);

        if is_query {
            let table_name = SqlStatementClassifier::analyze_select_editability(query);

            debug!("[MSSQL] Sending query to server...");
            match client.query(query, &[]).await {
                Ok(mut stream) => {
                    let columns: Vec<String> = match stream.columns().await {
                        Ok(Some(cols)) => cols.iter().map(|c| c.name().to_string()).collect(),
                        _ => Vec::new(),
                    };
                    debug!("[MSSQL] Got {} columns from stream: {:?}", columns.len(), columns);

                    debug!("[MSSQL] Fetching results...");
                    match stream.into_first_result().await {
                        Ok(rows) => {
                            let elapsed_ms = start.elapsed().as_millis();
                            debug!("[MSSQL] Query completed, {} rows in {}ms", rows.len(), elapsed_ms);
                            Ok(Self::rows_to_query_result(columns, rows, query_string, elapsed_ms, table_name))
                        }
                        Err(e) => {
                            error!("[MSSQL] Failed to fetch results: {}", e);
                            Ok(SqlResult::Error(SqlErrorInfo {
                                sql: query_string,
                                message: e.to_string(),
                            }))
                        }
                    }
                }
                Err(e) => {
                    error!("[MSSQL] Query failed: {}", e);
                    Ok(SqlResult::Error(SqlErrorInfo {
                        sql: query_string,
                        message: e.to_string(),
                    }))
                }
            }
        } else {
            debug!("[MSSQL] Executing non-query statement...");
            match client.execute(query, &[]).await {
                Ok(result) => {
                    let elapsed_ms = start.elapsed().as_millis();
                    let rows_affected = result.total();
                    let message = SqlStatementClassifier::format_message(query, rows_affected);
                    debug!("[MSSQL] Statement completed, {} rows affected in {}ms", rows_affected, elapsed_ms);

                    Ok(SqlResult::Exec(ExecResult {
                        sql: query_string,
                        rows_affected,
                        elapsed_ms,
                        message: Some(message),
                    }))
                }
                Err(e) => {
                    error!("[MSSQL] Statement failed: {}", e);
                    Ok(SqlResult::Error(SqlErrorInfo {
                        sql: query_string,
                        message: e.to_string(),
                    }))
                }
            }
        }
    }

    async fn execute_streaming(
        &self, plugin: Arc<dyn DatabasePlugin>,
        script: &str,
        options: ExecOptions,
        sender: mpsc::Sender<StreamingProgress>,
    ) -> Result<(), DbError> {
        let mut guard = self.client.lock().await;
        let client = guard.as_mut()
            .ok_or_else(|| DbError::ConnectionError("Not connected to database".to_string()))?;

        let statements: Vec<String> = plugin.split_statements(script)
            .into_iter()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        let total = statements.len();

        if options.transactional {
            // Begin transaction
            client.execute("BEGIN TRANSACTION", &[]).await
                .map_err(|e| DbError::QueryError(format!("Failed to begin transaction: {}", e)))?;

            let mut has_error = false;

            for (index, sql) in statements.into_iter().enumerate() {
                let current = index + 1;
                let modified_sql = Self::apply_max_rows_limit(&sql, options.max_rows);
                let is_query = plugin.is_query_statement(&modified_sql);

                let result = match Self::execute_single(client, &modified_sql, is_query).await {
                    Ok(r) => r,
                    Err(e) => SqlResult::Error(SqlErrorInfo {
                        sql: sql.clone(),
                        message: e.to_string(),
                    }),
                };

                let is_error = result.is_error();
                if is_error {
                    has_error = true;
                }

                let progress = StreamingProgress {
                    current,
                    total,
                    result,
                };

                if sender.send(progress).await.is_err() {
                    break;
                }

                if is_error {
                    break;
                }
            }

            if has_error {
                client.execute("ROLLBACK", &[]).await
                    .map_err(|e| DbError::QueryError(format!("Failed to rollback: {}", e)))?;
            } else {
                client.execute("COMMIT", &[]).await
                    .map_err(|e| DbError::QueryError(format!("Failed to commit: {}", e)))?;
            }
        } else {
            for (index, sql) in statements.into_iter().enumerate() {
                let current = index + 1;
                let modified_sql = Self::apply_max_rows_limit(&sql, options.max_rows);
                let is_query = plugin.is_query_statement(&modified_sql);

                let result = match Self::execute_single(client, &modified_sql, is_query).await {
                    Ok(r) => r,
                    Err(e) => SqlResult::Error(SqlErrorInfo {
                        sql: sql.clone(),
                        message: e.to_string(),
                    }),
                };

                let is_error = result.is_error();
                let progress = StreamingProgress {
                    current,
                    total,
                    result,
                };

                if sender.send(progress).await.is_err() {
                    break;
                }

                if is_error && options.stop_on_error {
                    break;
                }
            }
        }

        Ok(())
    }

    async fn switch_database(&self, database: &str) -> Result<(), DbError> {
        let mut guard = self.client.lock().await;
        let client = guard.as_mut()
            .ok_or_else(|| DbError::ConnectionError("Not connected to database".to_string()))?;

        let sql = format!("USE [{}]", database.replace("]", "]]"));
        client.execute(&sql, &[])
            .await
            .map_err(|e| DbError::QueryError(format!("Failed to switch database: {}", e)))?;

        Ok(())
    }
}
