use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use one_core::storage::DbConnectionConfig;
use tokio::sync::Mutex;
use tokio::sync::mpsc;

use crate::connection::{DbConnection, DbError, StreamingProgress};
use crate::executor::{
    ExecOptions, ExecResult, QueryResult, SqlErrorInfo, SqlResult, SqlScriptSplitter,
    SqlStatementClassifier,
};
use crate::SqlValue;

pub struct OracleDbConnection {
    config: DbConnectionConfig,
    conn: Arc<Mutex<Option<oracle::Connection>>>,
}

impl OracleDbConnection {
    pub fn new(config: DbConnectionConfig) -> Self {
        Self {
            config,
            conn: Arc::new(Mutex::new(None)),
        }
    }

    fn build_connect_string(config: &DbConnectionConfig) -> String {
        // Oracle uses service_name, stored in database field
        if let Some(ref service) = config.database {
            format!("//{}:{}/{}", config.host, config.port, service)
        } else {
            format!("//{}:{}", config.host, config.port)
        }
    }

    fn extract_value(row: &oracle::Row, index: usize) -> Option<String> {
        // Try different types
        row.get::<usize, Option<String>>(index)
            .ok()
            .flatten()
            .or_else(|| {
                row.get::<usize, Option<i64>>(index)
                    .ok()
                    .flatten()
                    .map(|v| v.to_string())
            })
            .or_else(|| {
                row.get::<usize, Option<f64>>(index)
                    .ok()
                    .flatten()
                    .map(|v| v.to_string())
            })
    }

    fn apply_max_rows_limit(sql: &str, max_rows: Option<usize>) -> String {
        if let Some(max) = max_rows {
            if SqlStatementClassifier::is_query_statement(sql) {
                let upper = sql.to_uppercase();
                // Oracle 12c+ uses FETCH FIRST, older uses ROWNUM
                if !upper.contains(" FETCH ") && !upper.contains("ROWNUM") {
                    return format!("{} FETCH FIRST {} ROWS ONLY", sql.trim_end_matches(';'), max);
                }
            }
        }
        sql.to_string()
    }

    fn execute_query_sync(
        conn: &oracle::Connection,
        sql: &str,
    ) -> Result<SqlResult, DbError> {
        let start = Instant::now();
        let sql_string = sql.to_string();
        let table_name = SqlStatementClassifier::analyze_select_editability(sql);

        match conn.query(sql, &[]) {
            Ok(rows) => {
                let elapsed_ms = start.elapsed().as_millis();

                let column_info = rows.column_info();
                let columns: Vec<String> = column_info.iter()
                    .map(|col| col.name().to_string())
                    .collect();

                let mut data_rows = Vec::new();
                for row_result in rows {
                    match row_result {
                        Ok(row) => {
                            let mut row_data = Vec::new();
                            for i in 0..columns.len() {
                                row_data.push(Self::extract_value(&row, i));
                            }
                            data_rows.push(row_data);
                        }
                        Err(_) => continue,
                    }
                }

                let editable = table_name.is_some();
                Ok(SqlResult::Query(QueryResult {
                    sql: sql_string,
                    columns,
                    rows: data_rows,
                    elapsed_ms,
                    table_name,
                    editable,
                }))
            }
            Err(e) => Ok(SqlResult::Error(SqlErrorInfo {
                sql: sql_string,
                message: e.to_string(),
            })),
        }
    }

    fn execute_non_query_sync(
        conn: &oracle::Connection,
        sql: &str,
    ) -> Result<SqlResult, DbError> {
        let start = Instant::now();
        let sql_string = sql.to_string();

        match conn.execute(sql, &[]) {
            Ok(stmt) => {
                let elapsed_ms = start.elapsed().as_millis();
                let rows_affected = stmt.row_count().unwrap_or(0);
                let message = SqlStatementClassifier::format_message(sql, rows_affected);

                Ok(SqlResult::Exec(ExecResult {
                    sql: sql_string,
                    rows_affected,
                    elapsed_ms,
                    message: Some(message),
                }))
            }
            Err(e) => Ok(SqlResult::Error(SqlErrorInfo {
                sql: sql_string,
                message: e.to_string(),
            })),
        }
    }
}

unsafe impl Send for OracleDbConnection {}
unsafe impl Sync for OracleDbConnection {}

#[async_trait]
impl DbConnection for OracleDbConnection {
    fn config(&self) -> &DbConnectionConfig {
        &self.config
    }

    fn set_config_database(&mut self, database: Option<String>) {
        self.config.database = database;
    }

    async fn connect(&mut self) -> Result<(), DbError> {
        let config = self.config.clone();

        let connect_string = Self::build_connect_string(&config);
        let username = config.username.clone();
        let password = config.password.clone();

        let conn = tokio::task::spawn_blocking(move || {
            oracle::Connection::connect(&username, &password, &connect_string)
                .map_err(|e| DbError::ConnectionError(format!("Failed to connect: {}", e)))
        })
        .await
        .map_err(|e| DbError::ConnectionError(format!("Task error: {}", e)))??;

        {
            let mut guard = self.conn.lock().await;
            *guard = Some(conn);
        }

        Ok(())
    }

    async fn current_database(&self) -> Result<Option<String>, DbError> {
        let conn_arc = self.conn.clone();

        tokio::task::spawn_blocking(move || {
            let guard = conn_arc.blocking_lock();
            let conn = guard.as_ref()
                .ok_or_else(|| DbError::ConnectionError("Not connected".to_string()))?;

            match conn.query_row_as::<String>("SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') FROM DUAL", &[]) {
                Ok(schema) => Ok(Some(schema)),
                Err(_) => Ok(None),
            }
        })
        .await
        .map_err(|e| DbError::QueryError(format!("Task error: {}", e)))?
    }

    async fn disconnect(&mut self) -> Result<(), DbError> {
        let conn_opt = {
            let mut guard = self.conn.lock().await;
            guard.take()
        };

        if let Some(conn) = conn_opt {
            tokio::task::spawn_blocking(move || {
                let _ = conn.close();
            })
            .await
            .map_err(|e| DbError::ConnectionError(format!("Task error: {}", e)))?;
        }

        Ok(())
    }

    async fn execute(&self, script: &str, options: ExecOptions) -> Result<Vec<SqlResult>, DbError> {
        let statements: Vec<String> = SqlScriptSplitter::split(script)
            .into_iter()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        let mut results = Vec::new();
        let conn_arc = self.conn.clone();
        let max_rows = options.max_rows;
        let stop_on_error = options.stop_on_error;

        for sql in statements {
            let modified_sql = Self::apply_max_rows_limit(&sql, max_rows);
            let is_query = SqlStatementClassifier::is_query_statement(&modified_sql);

            let conn_clone = conn_arc.clone();
            let sql_clone = modified_sql.clone();

            let result = tokio::task::spawn_blocking(move || {
                let guard = conn_clone.blocking_lock();
                let conn = guard.as_ref()
                    .ok_or_else(|| DbError::ConnectionError("Not connected to database".to_string()))?;

                if is_query {
                    Self::execute_query_sync(conn, &sql_clone)
                } else {
                    Self::execute_non_query_sync(conn, &sql_clone)
                }
            })
            .await
            .map_err(|e| DbError::QueryError(format!("Task error: {}", e)))??;

            let is_error = result.is_error();
            results.push(result);

            if is_error && stop_on_error {
                break;
            }
        }

        Ok(results)
    }

    async fn query(
        &self,
        query: &str,
        _params: Option<Vec<SqlValue>>,
        options: ExecOptions,
    ) -> Result<SqlResult, DbError> {
        let modified_sql = Self::apply_max_rows_limit(query, options.max_rows);
        let is_query = SqlStatementClassifier::is_query_statement(&modified_sql);
        let conn_arc = self.conn.clone();

        tokio::task::spawn_blocking(move || {
            let guard = conn_arc.blocking_lock();
            let conn = guard.as_ref()
                .ok_or_else(|| DbError::ConnectionError("Not connected to database".to_string()))?;

            if is_query {
                Self::execute_query_sync(conn, &modified_sql)
            } else {
                Self::execute_non_query_sync(conn, &modified_sql)
            }
        })
        .await
        .map_err(|e| DbError::QueryError(format!("Task error: {}", e)))?
    }

    async fn execute_streaming(
        &self,
        script: &str,
        options: ExecOptions,
        sender: mpsc::Sender<StreamingProgress>,
    ) -> Result<(), DbError> {
        let statements: Vec<String> = SqlScriptSplitter::split(script)
            .into_iter()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        let total = statements.len();
        let max_rows = options.max_rows;
        let stop_on_error = options.stop_on_error;

        for (index, sql) in statements.into_iter().enumerate() {
            let current = index + 1;
            let modified_sql = Self::apply_max_rows_limit(&sql, max_rows);
            let is_query = SqlStatementClassifier::is_query_statement(&modified_sql);

            let conn_arc = self.conn.clone();
            let sql_clone = modified_sql.clone();

            let result = match tokio::task::spawn_blocking(move || {
                let guard = conn_arc.blocking_lock();
                let conn = guard.as_ref()
                    .ok_or_else(|| DbError::ConnectionError("Not connected to database".to_string()))?;

                if is_query {
                    Self::execute_query_sync(conn, &sql_clone)
                } else {
                    Self::execute_non_query_sync(conn, &sql_clone)
                }
            })
            .await
            {
                Ok(Ok(r)) => r,
                Ok(Err(e)) => SqlResult::Error(SqlErrorInfo {
                    sql: sql.clone(),
                    message: e.to_string(),
                }),
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

            if is_error && stop_on_error {
                break;
            }
        }

        Ok(())
    }

    async fn switch_database(&self, schema: &str) -> Result<(), DbError> {
        // Oracle uses schemas instead of databases
        // Use ALTER SESSION SET CURRENT_SCHEMA to switch
        let sql = format!("ALTER SESSION SET CURRENT_SCHEMA = \"{}\"", schema.replace("\"", "\"\""));
        let conn_arc = self.conn.clone();

        tokio::task::spawn_blocking(move || {
            let guard = conn_arc.blocking_lock();
            let conn = guard.as_ref()
                .ok_or_else(|| DbError::ConnectionError("Not connected to database".to_string()))?;

            conn.execute(&sql, &[])
                .map_err(|e| DbError::QueryError(format!("Failed to switch schema: {}", e)))?;

            Ok(())
        })
        .await
        .map_err(|e| DbError::QueryError(format!("Task error: {}", e)))?
    }
}
