use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use mysql_async::{prelude::*, Conn, Opts, OptsBuilder, Row, Value};
use one_core::storage::DbConnectionConfig;
use tokio::sync::Mutex;
use tokio::sync::mpsc;

use crate::connection::{DbConnection, DbError, StreamingProgress};
use crate::executor::{ExecOptions, ExecResult, QueryResult, SqlErrorInfo, SqlResult, SqlStatementClassifier};
use crate::{DatabasePlugin, SqlValue};

pub struct MysqlDbConnection {
    config: DbConnectionConfig,
    conn: Arc<Mutex<Option<Conn>>>,
}

impl MysqlDbConnection {
    pub fn new(config: DbConnectionConfig) -> Self {
        Self {
            config,
            conn: Arc::new(Mutex::new(None)),
        }
    }

    /// Extract value from mysql_async::Value
    fn extract_value(value: &Value) -> Option<String> {
        match value {
            Value::NULL => None,
            Value::Bytes(b) => Some(String::from_utf8_lossy(b).to_string()),
            Value::Int(i) => Some(i.to_string()),
            Value::UInt(u) => Some(u.to_string()),
            Value::Float(f) => Some(f.to_string()),
            Value::Double(d) => Some(d.to_string()),
            Value::Date(year, month, day, hour, min, sec, micro) => {
                if *hour == 0 && *min == 0 && *sec == 0 && *micro == 0 {
                    Some(format!("{:04}-{:02}-{:02}", year, month, day))
                } else {
                    Some(format!(
                        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
                        year, month, day, hour, min, sec
                    ))
                }
            }
            Value::Time(is_neg, days, hours, minutes, seconds, _micros) => {
                let sign = if *is_neg { "-" } else { "" };
                if *days == 0 {
                    Some(format!("{}{}:{:02}:{:02}", sign, hours, minutes, seconds))
                } else {
                    Some(format!(
                        "{}{} {:02}:{:02}:{:02}",
                        sign, days, hours, minutes, seconds
                    ))
                }
            }
        }
    }

    fn convert_param(param: &SqlValue) -> Value {
        match param {
            SqlValue::Null => Value::NULL,
            SqlValue::Bool(v) => Value::Int(*v as i64),
            SqlValue::Int(v) => Value::Int(*v),
            SqlValue::Float(v) => Value::Double(*v),
            SqlValue::String(v) => Value::Bytes(v.as_bytes().to_vec()),
            SqlValue::Bytes(v) => Value::Bytes(v.clone()),
            SqlValue::Json(v) => Value::Bytes(v.to_string().as_bytes().to_vec()),
        }
    }

    fn apply_max_rows_limit(sql: &str, max_rows: Option<usize>) -> String {
        if let Some(max) = max_rows {
            if SqlStatementClassifier::is_query_statement(sql)
                && !sql.to_uppercase().contains(" LIMIT ")
            {
                return format!("{} LIMIT {}", sql, max);
            }
        }
        sql.to_string()
    }

    fn rows_to_query_result(
        rows: Vec<Row>,
        sql: String,
        elapsed_ms: u128,
        table_name: Option<String>,
    ) -> SqlResult {
        if rows.is_empty() {
            return SqlResult::Query(QueryResult {
                sql,
                columns: Vec::new(),
                rows: Vec::new(),
                elapsed_ms,
                table_name: None,
                editable: false,
            });
        }

        let columns: Vec<String> = rows[0]
            .columns_ref()
            .iter()
            .map(|col| col.name_str().to_string())
            .collect();

        let all_rows: Vec<Vec<Option<String>>> = rows
            .iter()
            .map(|row| {
                (0..row.len())
                    .map(|i| Self::extract_value(&row[i]))
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
        conn: &mut Conn,
        sql: &str,
        is_query: bool,
    ) -> Result<SqlResult, DbError> {
        let start = Instant::now();
        let sql_string = sql.to_string();

        if is_query {
            let table_name = SqlStatementClassifier::analyze_select_editability(sql);

            match conn.query::<Row, _>(sql).await {
                Ok(rows) => {
                    let elapsed_ms = start.elapsed().as_millis();
                    Ok(Self::rows_to_query_result(rows, sql_string, elapsed_ms, table_name))
                }
                Err(e) => Ok(SqlResult::Error(SqlErrorInfo {
                    sql: sql_string,
                    message: e.to_string(),
                })),
            }
        } else {
            match conn.query_drop(sql).await {
                Ok(_) => {
                    let elapsed_ms = start.elapsed().as_millis();
                    Ok(Self::build_exec_result(sql_string, conn.affected_rows(), elapsed_ms))
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
impl DbConnection for MysqlDbConnection {
    fn config(&self) -> &DbConnectionConfig {
        &self.config
    }

    fn set_config_database(&mut self, database: Option<String>) {
        self.config.database = database;
    }

    async fn connect(&mut self) -> anyhow::Result<(), DbError> {
        let config = &self.config;

        let mut opts_builder = OptsBuilder::default()
            .ip_or_hostname(&config.host)
            .tcp_port(config.port)
            .user(Some(&config.username))
            .pass(Some(&config.password));

        if let Some(ref db) = config.database {
            opts_builder = opts_builder.db_name(Some(db));
        }

        // Apply extra params
        if let Some(timeout) = config.get_param_as::<u64>("connect_timeout") {
            opts_builder = opts_builder.conn_ttl(Some(std::time::Duration::from_secs(timeout)));
        }
        if let Some(wait_timeout) = config.get_param_as::<usize>("read_timeout") {
            opts_builder = opts_builder.wait_timeout(Some(wait_timeout));
        }

        let opts = Opts::from(opts_builder);
        let conn = Conn::new(opts)
            .await
            .map_err(|e| DbError::ConnectionError(format!("Failed to connect: {}", e)))?;

        {
            let mut guard = self.conn.lock().await;
            *guard = Some(conn);
        }

        Ok(())
    }

    async fn disconnect(&mut self) -> Result<(), DbError> {
        let conn_opt = {
            let mut guard = self.conn.lock().await;
            guard.take()
        };

        if let Some(conn) = conn_opt {
            conn.disconnect()
                .await
                .map_err(|e| DbError::ConnectionError(format!("Failed to disconnect: {}", e)))?;
        }

        Ok(())
    }

    async fn execute(&self, plugin: Arc<dyn DatabasePlugin>, script: &str, options: ExecOptions) -> Result<Vec<SqlResult>, DbError> {
        let mut guard = self.conn.lock().await;
        let conn = guard.as_mut()
            .ok_or_else(|| DbError::ConnectionError("Not connected to database".to_string()))?;

        let statements = plugin.split_statements(script);
        let mut results = Vec::new();

        if options.transactional {
            let mut tx = conn
                .start_transaction(Default::default())
                .await
                .map_err(|e| DbError::QueryError(format!("Failed to begin transaction: {}", e)))?;

            for sql in statements {
                let sql = sql.trim();
                if sql.is_empty() {
                    continue;
                }

                let modified_sql = Self::apply_max_rows_limit(sql, options.max_rows);
                let is_query = plugin.is_query_statement(&modified_sql);
                let start = Instant::now();

                let result = if is_query {
                    let table_name = plugin.analyze_select_editability(&modified_sql);
                    match tx.query::<Row, _>(&modified_sql).await {
                        Ok(rows) => {
                            let elapsed_ms = start.elapsed().as_millis();
                            Self::rows_to_query_result(rows, sql.to_string(), elapsed_ms, table_name)
                        }
                        Err(e) => SqlResult::Error(SqlErrorInfo {
                            sql: sql.to_string(),
                            message: e.to_string(),
                        }),
                    }
                } else {
                    match tx.query_drop(&modified_sql).await {
                        Ok(_) => {
                            let elapsed_ms = start.elapsed().as_millis();
                            Self::build_exec_result(sql.to_string(), tx.affected_rows(), elapsed_ms)
                        }
                        Err(e) => SqlResult::Error(SqlErrorInfo {
                            sql: sql.to_string(),
                            message: e.to_string(),
                        }),
                    }
                };

                let is_error = result.is_error();
                results.push(result);

                if is_error {
                    break;
                }
            }

            let has_error = results.iter().any(|r| r.is_error());
            if has_error {
                tx.rollback()
                    .await
                    .map_err(|e| DbError::QueryError(format!("Failed to rollback: {}", e)))?;
            } else {
                tx.commit()
                    .await
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
                let result = Self::execute_single(conn, &modified_sql, is_query).await?;

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
        params: Option<Vec<SqlValue>>,
        _options: ExecOptions,
    ) -> Result<SqlResult, DbError> {
        let mut guard = self.conn.lock().await;
        let conn = guard.as_mut()
            .ok_or_else(|| DbError::ConnectionError("Not connected to database".to_string()))?;

        let start = Instant::now();
        let is_query = SqlStatementClassifier::is_query_statement(query);
        let query_string = query.to_string();

        if let Some(params) = params {
            let mysql_params: Vec<Value> = params.iter().map(Self::convert_param).collect();

            if is_query {
                let table_name = SqlStatementClassifier::analyze_select_editability(query);

                match conn.exec::<Row, _, _>(query, mysql_params).await {
                    Ok(rows) => {
                        let elapsed_ms = start.elapsed().as_millis();
                        Ok(Self::rows_to_query_result(rows, query_string, elapsed_ms, table_name))
                    }
                    Err(e) => Ok(SqlResult::Error(SqlErrorInfo {
                        sql: query_string,
                        message: e.to_string(),
                    })),
                }
            } else {
                match conn.exec_drop(query, mysql_params).await {
                    Ok(_) => {
                        let elapsed_ms = start.elapsed().as_millis();
                        Ok(Self::build_exec_result(query_string, conn.affected_rows(), elapsed_ms))
                    }
                    Err(e) => Ok(SqlResult::Error(SqlErrorInfo {
                        sql: query_string,
                        message: e.to_string(),
                    })),
                }
            }
        } else {
            Self::execute_single(conn, query, is_query).await
        }
    }

    async fn current_database(&self) -> Result<Option<String>, DbError> {
        let mut guard = self.conn.lock().await;
        let conn = guard.as_mut()
            .ok_or_else(|| DbError::ConnectionError("Not connected to database".to_string()))?;

        let result: Option<Option<String>> = conn
            .query_first("SELECT DATABASE()")
            .await
            .map_err(|e| DbError::QueryError(format!("Failed to get current database: {}", e)))?;

        Ok(result.flatten())
    }

    async fn switch_database(&self, database: &str) -> Result<(), DbError> {
        let mut guard = self.conn.lock().await;
        let conn = guard.as_mut()
            .ok_or_else(|| DbError::ConnectionError("Not connected to database".to_string()))?;

        let sql = format!("USE `{}`", database.replace("`", "``"));
        conn.query_drop(&sql)
            .await
            .map_err(|e| DbError::QueryError(format!("Failed to switch database: {}", e)))?;

        Ok(())
    }

    async fn execute_streaming(
        &self, plugin: Arc<dyn DatabasePlugin>,
        script: &str,
        options: ExecOptions,
        sender: mpsc::Sender<StreamingProgress>,
    ) -> Result<(), DbError> {
        let mut guard = self.conn.lock().await;
        let conn = guard.as_mut()
            .ok_or_else(|| DbError::ConnectionError("Not connected to database".to_string()))?;

        let statements: Vec<String> = plugin.split_statements(script);

        let total = statements.len();

        if options.transactional {
            let mut tx = conn
                .start_transaction(Default::default())
                .await
                .map_err(|e| DbError::QueryError(format!("Failed to begin transaction: {}", e)))?;

            let mut has_error = false;

            for (index, sql) in statements.into_iter().enumerate() {
                let current = index + 1;
                let modified_sql = Self::apply_max_rows_limit(&sql, options.max_rows);
                let is_query = plugin.is_query_statement(&modified_sql);
                let start = Instant::now();

                let result = if is_query {
                    let table_name = plugin.analyze_select_editability(&modified_sql);
                    match tx.query::<Row, _>(&modified_sql).await {
                        Ok(rows) => {
                            let elapsed_ms = start.elapsed().as_millis();
                            Self::rows_to_query_result(rows, sql.clone(), elapsed_ms, table_name)
                        }
                        Err(e) => SqlResult::Error(SqlErrorInfo {
                            sql: sql.clone(),
                            message: e.to_string(),
                        }),
                    }
                } else {
                    match tx.query_drop(&modified_sql).await {
                        Ok(_) => {
                            let elapsed_ms = start.elapsed().as_millis();
                            Self::build_exec_result(sql.clone(), tx.affected_rows(), elapsed_ms)
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

            if has_error {
                tx.rollback()
                    .await
                    .map_err(|e| DbError::QueryError(format!("Failed to rollback: {}", e)))?;
            } else {
                tx.commit()
                    .await
                    .map_err(|e| DbError::QueryError(format!("Failed to commit: {}", e)))?;
            }
        } else {
            for (index, sql) in statements.into_iter().enumerate() {
                let current = index + 1;
                let modified_sql = Self::apply_max_rows_limit(&sql, options.max_rows);
                let is_query = SqlStatementClassifier::is_query_statement(&modified_sql);

                let result = match Self::execute_single(conn, &modified_sql, is_query).await {
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
}
