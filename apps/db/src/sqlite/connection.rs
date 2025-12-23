use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use sqlx::sqlite::SqliteRow;
use sqlx::{Column, Connection, Row, SqliteConnection};
use tokio::sync::Mutex;
use one_core::storage::DbConnectionConfig;
use crate::connection::{DbConnection, DbError, StreamingProgress};
use tokio::sync::mpsc;
use crate::DatabasePlugin;
use crate::executor::{
    ExecOptions, ExecResult, QueryResult, SqlErrorInfo, SqlResult,
    SqlStatementClassifier,
};

use crate::types::{SqlValue};

pub struct SqliteDbConnection {
    config: DbConnectionConfig,
    connection: Arc<Mutex<Option<SqliteConnection>>>,
}

impl SqliteDbConnection {
    pub fn new(config: DbConnectionConfig) -> Self {
        Self {
            config,
            connection: Arc::new(Mutex::new(None)),
        }
    }

    fn extract_value(row: &SqliteRow, index: usize) -> Option<String> {
        use sqlx::types::chrono::{NaiveDate, NaiveDateTime, NaiveTime, DateTime};
        use sqlx::{TypeInfo, ValueRef};

        if let Ok(val) = row.try_get_raw(index) {
            if val.is_null() {
                return None;
            }
        }

        let column = row.column(index);
        let type_name = column.type_info().name().to_uppercase();

        match type_name.as_str() {
            "DATETIME" | "TIMESTAMP" => {
                // Try parsing as string first (ISO8601 format)
                if let Ok(val) = row.try_get::<String, _>(index) {
                    return Some(val);
                }
                // Try parsing as Unix timestamp (integer, milliseconds)
                if let Ok(ts) = row.try_get::<i64, _>(index) {
                    if let Some(dt) = DateTime::from_timestamp_millis(ts) {
                        return Some(dt.format("%Y-%m-%d %H:%M:%S").to_string());
                    }
                    return Some(ts.to_string());
                }
                // Try parsing as NaiveDateTime
                if let Ok(val) = row.try_get::<NaiveDateTime, _>(index) {
                    return Some(val.format("%Y-%m-%d %H:%M:%S").to_string());
                }
            }
            "DATE" => {
                if let Ok(val) = row.try_get::<String, _>(index) {
                    return Some(val);
                }
                if let Ok(val) = row.try_get::<NaiveDate, _>(index) {
                    return Some(val.format("%Y-%m-%d").to_string());
                }
            }
            "TIME" => {
                if let Ok(val) = row.try_get::<String, _>(index) {
                    return Some(val);
                }
                if let Ok(val) = row.try_get::<NaiveTime, _>(index) {
                    return Some(val.format("%H:%M:%S").to_string());
                }
            }
            _ => {}
        }

        // Generic type handling
        if let Ok(val) = row.try_get::<String, _>(index) {
            return Some(val);
        }

        if let Ok(val) = row.try_get::<i64, _>(index) {
            return Some(val.to_string());
        }

        if let Ok(val) = row.try_get::<f64, _>(index) {
            return Some(val.to_string());
        }

        if let Ok(val) = row.try_get::<bool, _>(index) {
            return Some(if val { "1" } else { "0" }.to_string());
        }

        if let Ok(val) = row.try_get::<Vec<u8>, _>(index) {
            if let Ok(s) = String::from_utf8(val.clone()) {
                return Some(s);
            }
            return Some(format!("0x{}", hex::encode(&val)));
        }

        Some(format!("<{}>", type_name))
    }
}

#[async_trait]
impl DbConnection for SqliteDbConnection {
    fn config(&self) -> &DbConnectionConfig {
        &self.config
    }

    fn set_config_database(&mut self, database: Option<String>) {
        self.config.database = database;
    }

    fn supports_database_switch(&self) -> bool {
        false
    }

    async fn connect(&mut self) -> Result<(), DbError> {
        let config = &self.config;

        // SQLite uses `host` field as the database file path
        let database_path = if !config.host.is_empty() {
            config.host.clone()
        } else {
            config.database.clone().ok_or_else(|| {
                DbError::ConnectionError("Database path is required for SQLite".to_string())
            })?
        };

        // Handle create_if_missing for SQLite
        let url = format!("sqlite://{}?mode=rwc", database_path);
        let conn = SqliteConnection::connect(&url)
            .await
            .map_err(|e| DbError::ConnectionError(format!("Failed to connect: {}", e)))?;

        {
            let mut guard = self.connection.lock().await;
            *guard = Some(conn);
        }

        Ok(())
    }

    async fn disconnect(&mut self) -> Result<(), DbError> {
        let conn_opt = {
            let mut guard = self.connection.lock().await;
            guard.take()
        };

        if let Some(conn) = conn_opt {
            conn.close().await.map_err(|e| DbError::ConnectionError(format!("Failed to disconnect: {}", e)))?;
        }

        Ok(())
    }

    async fn execute(
        &self, plugin: Arc<dyn DatabasePlugin>,
        script: &str,
        options: ExecOptions,
    ) -> Result<Vec<SqlResult>, DbError> {
        let statements = plugin.split_statements(script);
        let mut results = Vec::new();

        for sql in statements {
            let sql = sql.trim();
            if sql.is_empty() {
                continue;
            }

            let modified_sql = if let Some(max_rows) = options.max_rows {
                if plugin.is_query_statement(sql)
                    && !sql.to_uppercase().contains(" LIMIT ")
                {
                    format!("{} LIMIT {}", sql, max_rows)
                } else {
                    sql.to_string()
                }
            } else {
                sql.to_string()
            };

            let is_query = SqlStatementClassifier::is_query_statement(&modified_sql);
            let start = Instant::now();

            let result = if is_query {
                let mut guard = self.connection.lock().await;
                let conn = guard.as_mut()
                    .ok_or_else(|| DbError::ConnectionError("Not connected to database".to_string()))?;

                match sqlx::query(&modified_sql).fetch_all(conn).await {
                    Ok(rows) => {
                        let elapsed_ms = start.elapsed().as_millis();

                        if rows.is_empty() {
                            SqlResult::Query(QueryResult {
                                sql: sql.to_string(),
                                columns: Vec::new(),
                                rows: Vec::new(),
                                elapsed_ms,
                                table_name: None,
                                editable: false,
                            })
                        } else {
                            let columns: Vec<String> = rows[0]
                                .columns()
                                .iter()
                                .map(|col| col.name().to_string())
                                .collect();

                            let data_rows: Vec<Vec<Option<String>>> = rows
                                .iter()
                                .map(|row| {
                                    (0..columns.len())
                                        .map(|i| Self::extract_value(row, i))
                                        .collect()
                                })
                                .collect();

                            SqlResult::Query(QueryResult {
                                sql: sql.to_string(),
                                columns,
                                rows: data_rows,
                                elapsed_ms,
                                table_name: None,
                                editable: false,
                            })
                        }
                    }
                    Err(e) => SqlResult::Error(SqlErrorInfo {
                        sql: sql.to_string(),
                        message: e.to_string(),
                    }),
                }
            } else {
                let mut guard = self.connection.lock().await;
                let conn = guard.as_mut()
                    .ok_or_else(|| DbError::ConnectionError("Not connected to database".to_string()))?;

                match sqlx::query(&modified_sql).execute(conn).await {
                    Ok(exec_result) => {
                        let elapsed_ms = start.elapsed().as_millis();
                        let rows_affected = exec_result.rows_affected();
                        let message = SqlStatementClassifier::format_message(sql, rows_affected);

                        SqlResult::Exec(ExecResult {
                            sql: sql.to_string(),
                            rows_affected,
                            elapsed_ms,
                            message: Some(message),
                        })
                    }
                    Err(e) => SqlResult::Error(SqlErrorInfo {
                        sql: sql.to_string(),
                        message: e.to_string(),
                    }),
                }
            };

            let is_error = result.is_error();
            results.push(result);

            if is_error && options.stop_on_error {
                break;
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
        let start = Instant::now();
        let is_query = SqlStatementClassifier::is_query_statement(query);

        let result = if is_query {
            let mut guard = self.connection.lock().await;
            let conn = guard.as_mut()
                .ok_or_else(|| DbError::ConnectionError("Not connected to database".to_string()))?;

            match sqlx::query(query).fetch_all(conn).await {
                Ok(rows) => {
                    let elapsed_ms = start.elapsed().as_millis();

                    if rows.is_empty() {
                        SqlResult::Query(QueryResult {
                            sql: query.to_string(),
                            columns: Vec::new(),
                            rows: Vec::new(),
                            elapsed_ms,
                            table_name: None,
                            editable: false,
                        })
                    } else {
                        let columns: Vec<String> = rows[0]
                            .columns()
                            .iter()
                            .map(|col| col.name().to_string())
                            .collect();

                        let data_rows: Vec<Vec<Option<String>>> = rows
                            .iter()
                            .map(|row| {
                                (0..columns.len())
                                    .map(|i| Self::extract_value(row, i))
                                    .collect()
                            })
                            .collect();

                        SqlResult::Query(QueryResult {
                            sql: query.to_string(),
                            columns,
                            rows: data_rows,
                            elapsed_ms,
                            table_name: None,
                            editable: false,
                        })
                    }
                }
                Err(e) => SqlResult::Error(SqlErrorInfo {
                    sql: query.to_string(),
                    message: e.to_string(),
                }),
            }
        } else {
            let mut guard = self.connection.lock().await;
            let conn = guard.as_mut()
                .ok_or_else(|| DbError::ConnectionError("Not connected to database".to_string()))?;

            match sqlx::query(query).execute(conn).await {
                Ok(exec_result) => {
                    let elapsed_ms = start.elapsed().as_millis();
                    let rows_affected = exec_result.rows_affected();
                    let message = SqlStatementClassifier::format_message(query, rows_affected);

                    SqlResult::Exec(ExecResult {
                        sql: query.to_string(),
                        rows_affected,
                        elapsed_ms,
                        message: Some(message),
                    })
                }
                Err(e) => SqlResult::Error(SqlErrorInfo {
                    sql: query.to_string(),
                    message: e.to_string(),
                }),
            }
        };

        Ok(result)
    }

    async fn current_database(&self) -> Result<Option<String>, DbError> {
        // SQLite doesn't have a "current database" concept like other DBs
        // Return the database file path from config
        Ok(self.config.database.clone())
    }

    async fn switch_database(&self, _database: &str) -> Result<(), DbError> {
        // SQLite doesn't support switching databases - each database is a separate file
        // The connection must be recreated to connect to a different database file
        Err(DbError::QueryError(
            "SQLite does not support switching databases. Each database is a separate file connection.".to_string()
        ))
    }

    async fn execute_streaming(
        &self, plugin: Arc<dyn DatabasePlugin>,
        script: &str,
        options: ExecOptions,
        sender: mpsc::Sender<StreamingProgress>,
    ) -> Result<(), DbError> {
        let statements: Vec<String> = plugin.split_statements(script)
            .into_iter()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        let total = statements.len();

        if options.transactional {
            {
                let mut guard = self.connection.lock().await;
                let conn = guard.as_mut()
                    .ok_or_else(|| DbError::ConnectionError("Not connected to database".to_string()))?;
                sqlx::query("BEGIN").execute(&mut *conn).await
                    .map_err(|e| DbError::QueryError(format!("Failed to begin transaction: {}", e)))?;
            }

            let mut has_error = false;

            for (index, sql) in statements.into_iter().enumerate() {
                let current = index + 1;

                let modified_sql = if let Some(max_rows) = options.max_rows {
                    if SqlStatementClassifier::is_query_statement(&sql)
                        && !sql.to_uppercase().contains(" LIMIT ")
                    {
                        format!("{} LIMIT {}", sql, max_rows)
                    } else {
                        sql.clone()
                    }
                } else {
                    sql.clone()
                };

                let is_query = plugin.is_query_statement(&modified_sql);
                let start = Instant::now();

                let result = if is_query {
                    let mut guard = self.connection.lock().await;
                    let conn = guard.as_mut()
                        .ok_or_else(|| DbError::ConnectionError("Not connected to database".to_string()))?;

                    match sqlx::query(&modified_sql).fetch_all(conn).await {
                        Ok(rows) => {
                            let elapsed_ms = start.elapsed().as_millis();

                            if rows.is_empty() {
                                SqlResult::Query(QueryResult {
                                    sql: sql.clone(),
                                    columns: Vec::new(),
                                    rows: Vec::new(),
                                    elapsed_ms,
                                    table_name: None,
                                    editable: false,
                                })
                            } else {
                                let columns: Vec<String> = rows[0]
                                    .columns()
                                    .iter()
                                    .map(|col| col.name().to_string())
                                    .collect();

                                let data_rows: Vec<Vec<Option<String>>> = rows
                                    .iter()
                                    .map(|row| {
                                        (0..columns.len())
                                            .map(|i| Self::extract_value(row, i))
                                            .collect()
                                    })
                                    .collect();

                                SqlResult::Query(QueryResult {
                                    sql: sql.clone(),
                                    columns,
                                    rows: data_rows,
                                    elapsed_ms,
                                    table_name: None,
                                    editable: false,
                                })
                            }
                        }
                        Err(e) => SqlResult::Error(SqlErrorInfo {
                            sql: sql.clone(),
                            message: e.to_string(),
                        }),
                    }
                } else {
                    let mut guard = self.connection.lock().await;
                    let conn = guard.as_mut()
                        .ok_or_else(|| DbError::ConnectionError("Not connected to database".to_string()))?;

                    match sqlx::query(&modified_sql).execute(conn).await {
                        Ok(exec_result) => {
                            let elapsed_ms = start.elapsed().as_millis();
                            let rows_affected = exec_result.rows_affected();
                            let message = SqlStatementClassifier::format_message(&sql, rows_affected);

                            SqlResult::Exec(ExecResult {
                                sql: sql.clone(),
                                rows_affected,
                                elapsed_ms,
                                message: Some(message),
                            })
                        }
                        Err(e) => SqlResult::Error(SqlErrorInfo {
                            sql: sql.clone(),
                            message: e.to_string(),
                        }),
                    }
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

            {
                let mut guard = self.connection.lock().await;
                let conn = guard.as_mut()
                    .ok_or_else(|| DbError::ConnectionError("Not connected to database".to_string()))?;

                if has_error {
                    sqlx::query("ROLLBACK").execute(&mut *conn).await
                        .map_err(|e| DbError::QueryError(format!("Failed to rollback: {}", e)))?;
                } else {
                    sqlx::query("COMMIT").execute(&mut *conn).await
                        .map_err(|e| DbError::QueryError(format!("Failed to commit: {}", e)))?;
                }
            }
        } else {
            for (index, sql) in statements.into_iter().enumerate() {
                let current = index + 1;

                let modified_sql = if let Some(max_rows) = options.max_rows {
                    if plugin.is_query_statement(&sql)
                        && !sql.to_uppercase().contains(" LIMIT ")
                    {
                        format!("{} LIMIT {}", sql, max_rows)
                    } else {
                        sql.clone()
                    }
                } else {
                    sql.clone()
                };

                let is_query = SqlStatementClassifier::is_query_statement(&modified_sql);
                let start = Instant::now();

                let result = if is_query {
                    let mut guard = self.connection.lock().await;
                    let conn = guard.as_mut()
                        .ok_or_else(|| DbError::ConnectionError("Not connected to database".to_string()))?;

                    match sqlx::query(&modified_sql).fetch_all(conn).await {
                        Ok(rows) => {
                            let elapsed_ms = start.elapsed().as_millis();

                            if rows.is_empty() {
                                SqlResult::Query(QueryResult {
                                    sql: sql.clone(),
                                    columns: Vec::new(),
                                    rows: Vec::new(),
                                    elapsed_ms,
                                    table_name: None,
                                    editable: false,
                                })
                            } else {
                                let columns: Vec<String> = rows[0]
                                    .columns()
                                    .iter()
                                    .map(|col| col.name().to_string())
                                    .collect();

                                let data_rows: Vec<Vec<Option<String>>> = rows
                                    .iter()
                                    .map(|row| {
                                        (0..columns.len())
                                            .map(|i| Self::extract_value(row, i))
                                            .collect()
                                    })
                                    .collect();

                                SqlResult::Query(QueryResult {
                                    sql: sql.clone(),
                                    columns,
                                    rows: data_rows,
                                    elapsed_ms,
                                    table_name: None,
                                    editable: false,
                                })
                            }
                        }
                        Err(e) => SqlResult::Error(SqlErrorInfo {
                            sql: sql.clone(),
                            message: e.to_string(),
                        }),
                    }
                } else {
                    let mut guard = self.connection.lock().await;
                    let conn = guard.as_mut()
                        .ok_or_else(|| DbError::ConnectionError("Not connected to database".to_string()))?;

                    match sqlx::query(&modified_sql).execute(conn).await {
                        Ok(exec_result) => {
                            let elapsed_ms = start.elapsed().as_millis();
                            let rows_affected = exec_result.rows_affected();
                            let message = SqlStatementClassifier::format_message(&sql, rows_affected);

                            SqlResult::Exec(ExecResult {
                                sql: sql.clone(),
                                rows_affected,
                                elapsed_ms,
                                message: Some(message),
                            })
                        }
                        Err(e) => SqlResult::Error(SqlErrorInfo {
                            sql: sql.clone(),
                            message: e.to_string(),
                        }),
                    }
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
}
