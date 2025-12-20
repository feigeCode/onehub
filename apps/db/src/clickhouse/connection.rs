use crate::connection::{DbConnection, DbError, StreamingProgress};
use crate::executor::{ExecOptions, ExecResult, QueryResult, SqlErrorInfo, SqlResult, SqlScriptSplitter, SqlStatementClassifier};
use crate::SqlValue;

use clickhouse::{Client, Row};
use serde::Deserialize;
use std::time::Instant;
use async_trait::async_trait;
use one_core::storage::DbConnectionConfig;
use tokio::sync::mpsc;

pub struct ClickHouseDbConnection {
    config: DbConnectionConfig,
    client: Option<Client>,
}

impl ClickHouseDbConnection {
    pub fn new(config: DbConnectionConfig) -> Self {
        Self {
            config,
            client: None,
        }
    }

    fn ensure_connected(&self) -> Result<&Client, DbError> {
        self.client
            .as_ref()
            .ok_or_else(|| DbError::ConnectionError("Not connected to database".to_string()))
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
        client: &Client,
        sql: &str,
        is_query: bool,
    ) -> Result<SqlResult, DbError> {
        let start = Instant::now();
        let sql_string = sql.to_string();

        if is_query {
            let table_name = SqlStatementClassifier::analyze_select_editability(sql);

            // Use dynamic row deserialization
            match client.query(sql).fetch_all::<DynamicRow>().await {
                Ok(rows) => {
                    let elapsed_ms = start.elapsed().as_millis();

                    if rows.is_empty() {
                        return Ok(SqlResult::Query(QueryResult {
                            sql: sql_string,
                            columns: Vec::new(),
                            rows: Vec::new(),
                            elapsed_ms,
                            table_name: None,
                            editable: false,
                        }));
                    }

                    // Extract column names from first row
                    let columns = rows[0].column_names.clone();

                    // Convert rows to Vec<Vec<Option<String>>>
                    let all_rows: Vec<Vec<Option<String>>> = rows
                        .iter()
                        .map(|row| row.values.clone())
                        .collect();

                    let editable = table_name.is_some();
                    Ok(SqlResult::Query(QueryResult {
                        sql: sql_string,
                        columns,
                        rows: all_rows,
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
        } else {
            match client.query(sql).execute().await {
                Ok(_) => {
                    let elapsed_ms = start.elapsed().as_millis();
                    // ClickHouse execute() doesn't return affected rows count easily
                    // We'll return 0 for now
                    Ok(Self::build_exec_result(sql_string, 0, elapsed_ms))
                }
                Err(e) => Ok(SqlResult::Error(SqlErrorInfo {
                    sql: sql_string,
                    message: e.to_string(),
                })),
            }
        }
    }
}

// Dynamic row struct to deserialize any ClickHouse query result
#[derive(Debug, Clone, Row, Deserialize)]
struct DynamicRow {
    #[serde(flatten)]
    values_map: serde_json::Map<String, serde_json::Value>,
    #[serde(skip)]
    column_names: Vec<String>,
    #[serde(skip)]
    values: Vec<Option<String>>,
}

impl DynamicRow {
    fn from_map(map: serde_json::Map<String, serde_json::Value>) -> Self {
        let mut column_names = Vec::new();
        let mut values = Vec::new();

        for (key, value) in &map {
            column_names.push(key.clone());
            let str_value = match value {
                serde_json::Value::Null => None,
                serde_json::Value::String(s) => Some(s.clone()),
                _ => Some(value.to_string()),
            };
            values.push(str_value);
        }

        Self {
            values_map: map,
            column_names,
            values,
        }
    }
}

#[async_trait]
impl DbConnection for ClickHouseDbConnection {
    fn config(&self) -> &DbConnectionConfig {
        &self.config
    }

    fn set_config_database(&mut self, database: Option<String>) {
        self.config.database = database;
    }

    async fn connect(&mut self) -> anyhow::Result<(), DbError> {
        let config = &self.config;

        let url = format!("http://{}:{}", config.host, config.port);

        let mut client = Client::default()
            .with_url(&url)
            .with_user(&config.username)
            .with_password(&config.password);

        if let Some(ref db) = config.database {
            client = client.with_database(db);
        }

        // Test the connection
        client
            .query("SELECT 1")
            .fetch_all::<u8>()
            .await
            .map_err(|e| DbError::ConnectionError(format!("Failed to connect: {}", e)))?;

        self.client = Some(client);

        Ok(())
    }

    async fn current_database(&self) -> Result<Option<String>, DbError> {
        let client = self.ensure_connected()?;

        #[derive(Row, Deserialize)]
        struct DbName {
            name: String,
        }

        match client.query("SELECT currentDatabase() as name").fetch_one::<DbName>().await {
            Ok(row) => Ok(Some(row.name)),
            Err(_) => Ok(self.config.database.clone()),
        }
    }

    async fn disconnect(&mut self) -> Result<(), DbError> {
        self.client = None;
        Ok(())
    }

    async fn execute(&self, script: &str, options: ExecOptions) -> Result<Vec<SqlResult>, DbError> {
        let client = self.ensure_connected()?;

        let statements = SqlScriptSplitter::split(script);
        let mut results = Vec::new();

        // ClickHouse doesn't have traditional transactions like MySQL
        // We'll execute statements sequentially
        for sql in statements {
            let sql = sql.trim();
            if sql.is_empty() {
                continue;
            }

            let modified_sql = Self::apply_max_rows_limit(sql, options.max_rows);
            let is_query = SqlStatementClassifier::is_query_statement(&modified_sql);
            let result = Self::execute_single(client, &modified_sql, is_query).await?;

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
        params: Option<Vec<SqlValue>>,
        options: ExecOptions,
    ) -> Result<SqlResult, DbError> {
        let client = self.ensure_connected()?;

        // ClickHouse client doesn't support parameterized queries in the same way
        // For now, we'll execute the query directly
        // TODO: Implement proper parameter binding
        if params.is_some() {
            return Err(DbError::QueryError(
                "Parameterized queries not yet supported for ClickHouse".to_string(),
            ));
        }

        let modified_sql = Self::apply_max_rows_limit(query, options.max_rows);
        let is_query = SqlStatementClassifier::is_query_statement(&modified_sql);

        Self::execute_single(client, &modified_sql, is_query).await
    }

    async fn execute_streaming(
        &self,
        script: &str,
        options: ExecOptions,
        sender: mpsc::Sender<StreamingProgress>,
    ) -> Result<(), DbError> {
        let client = self.ensure_connected()?;

        let statements: Vec<String> = SqlScriptSplitter::split(script)
            .into_iter()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        let total = statements.len();

        for (index, sql) in statements.into_iter().enumerate() {
            let current = index + 1;
            let modified_sql = Self::apply_max_rows_limit(&sql, options.max_rows);
            let is_query = SqlStatementClassifier::is_query_statement(&modified_sql);

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

        Ok(())
    }

    async fn switch_database(&self, database: &str) -> Result<(), DbError> {
        let client = self.ensure_connected()?;

        let sql = format!("USE `{}`", database.replace("`", "``"));
        client
            .query(&sql)
            .execute()
            .await
            .map_err(|e| DbError::QueryError(format!("Failed to switch database: {}", e)))?;

        Ok(())
    }
}
