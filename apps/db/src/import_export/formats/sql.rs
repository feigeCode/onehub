use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use async_trait::async_trait;

use crate::connection::DbConnection;
use crate::DatabasePlugin;
use crate::executor::{ExecOptions, SqlResult};
use crate::import_export::{ExportConfig, ExportResult, FormatHandler, ImportConfig, ImportResult};

pub struct SqlFormatHandler;

#[async_trait]
impl FormatHandler for SqlFormatHandler {
    async fn import(
        &self,
        plugin: Arc<dyn DatabasePlugin>,
        connection: &dyn DbConnection,
        config: &ImportConfig,
        data: &str,
    ) -> Result<ImportResult> {
        let start = Instant::now();
        let mut errors = Vec::new();
        let mut total_rows = 0u64;

        // TRUNCATE表（如果需要）
        if config.truncate_before_import {
            if let Some(table) = &config.table {
                let truncate_sql = format!("TRUNCATE TABLE `{}`", table);
                let results = connection.execute(plugin.clone(), &truncate_sql, ExecOptions::default()).await
                    .map_err(|e| anyhow::anyhow!("Truncate failed: {}", e))?;
                
                for result in results {
                    if let SqlResult::Error(err) = result {
                        errors.push(format!("Truncate failed: {}", err.message));
                        if config.stop_on_error {
                            return Ok(ImportResult {
                                success: false,
                                rows_imported: 0,
                                errors,
                                elapsed_ms: start.elapsed().as_millis(),
                            });
                        }
                    }
                }
            }
        }

        // 执行SQL脚本
        let exec_options = ExecOptions {
            stop_on_error: config.stop_on_error,
            transactional: config.use_transaction,
            max_rows: None,
        };

        let results = connection.execute(plugin.clone(), data, exec_options).await
            .map_err(|e| anyhow::anyhow!("Execute failed: {}", e))?;

        for result in results {
            match result {
                SqlResult::Exec(exec_result) => {
                    total_rows += exec_result.rows_affected;
                }
                SqlResult::Error(err) => {
                    errors.push(err.message);
                }
                _ => {}
            }
        }

        Ok(ImportResult {
            success: errors.is_empty(),
            rows_imported: total_rows,
            errors,
            elapsed_ms: start.elapsed().as_millis(),
        })
    }

    async fn export(
        &self,
        connection: &dyn DbConnection,
        config: &ExportConfig,
    ) -> Result<ExportResult> {
        let start = Instant::now();
        let mut output = String::new();
        let mut total_rows = 0u64;

        for table in &config.tables {
            // 导出表结构
            if config.include_schema {
                let show_create = format!("SHOW CREATE TABLE `{}`", table);
                let result = connection.query(&show_create, None, ExecOptions::default()).await
                    .map_err(|e| anyhow::anyhow!("Query failed: {}", e))?;

                if let SqlResult::Query(query_result) = result {
                    if let Some(row) = query_result.rows.first() {
                        if let Some(Some(create_sql)) = row.get(1) {
                            output.push_str("-- Table structure for ");
                            output.push_str(table);
                            output.push('\n');
                            output.push_str(create_sql);
                            output.push_str(";\n\n");
                        }
                    }
                }
            }

            // 导出数据
            if config.include_data {
                let mut select_sql = format!("SELECT * FROM `{}`", table);
                if let Some(where_clause) = &config.where_clause {
                    select_sql.push_str(" WHERE ");
                    select_sql.push_str(where_clause);
                }
                if let Some(limit) = config.limit {
                    select_sql.push_str(&format!(" LIMIT {}", limit));
                }

                let result = connection.query(&select_sql, None, ExecOptions::default()).await
                    .map_err(|e| anyhow::anyhow!("Query failed: {}", e))?;

                if let SqlResult::Query(query_result) = result {
                    if !query_result.rows.is_empty() {
                        output.push_str("-- Data for table ");
                        output.push_str(table);
                        output.push('\n');

                        for row in &query_result.rows {
                            output.push_str("INSERT INTO `");
                            output.push_str(table);
                            output.push_str("` VALUES (");

                            for (i, value) in row.iter().enumerate() {
                                if i > 0 {
                                    output.push_str(", ");
                                }
                                match value {
                                    Some(v) => {
                                        output.push('\'');
                                        output.push_str(&v.replace('\'', "''"));
                                        output.push('\'');
                                    }
                                    None => output.push_str("NULL"),
                                }
                            }

                            output.push_str(");\n");
                            total_rows += 1;
                        }
                        output.push('\n');
                    }
                }
            }
        }

        Ok(ExportResult {
            success: true,
            output,
            rows_exported: total_rows,
            elapsed_ms: start.elapsed().as_millis(),
        })
    }
}
