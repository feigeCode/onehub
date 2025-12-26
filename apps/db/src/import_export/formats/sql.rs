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

        if config.truncate_before_import {
            if let Some(table) = &config.table {
                let truncate_sql = format!("TRUNCATE TABLE {}", plugin.quote_identifier(table));
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
        plugin: Arc<dyn DatabasePlugin>,
        connection: &dyn DbConnection,
        config: &ExportConfig,
    ) -> Result<ExportResult> {
        let start = Instant::now();
        let mut output = String::new();
        let mut total_rows = 0u64;

        for table in &config.tables {
            if config.include_schema {
                match plugin.export_table_create_sql(connection, &config.database, table).await {
                    Ok(schema_sql) => {
                        if !schema_sql.is_empty() {
                            output.push_str("-- Table structure for ");
                            output.push_str(table);
                            output.push('\n');
                            output.push_str(&schema_sql);
                            output.push_str(";\n\n");
                        }
                    }
                    Err(e) => {
                        output.push_str(&format!("-- Failed to export structure for {}: {}\n\n", table, e));
                    }
                }
            }

            if config.include_data {
                match plugin.export_table_data_sql(
                    connection,
                    &config.database,
                    table,
                    config.where_clause.as_deref(),
                    config.limit,
                ).await {
                    Ok(data_sql) => {
                        if !data_sql.is_empty() {
                            output.push_str("-- Data for table ");
                            output.push_str(table);
                            output.push('\n');
                            output.push_str(&data_sql);
                            output.push('\n');
                            total_rows += data_sql.lines().filter(|l| l.starts_with("INSERT")).count() as u64;
                        }
                    }
                    Err(e) => {
                        output.push_str(&format!("-- Failed to export data for {}: {}\n\n", table, e));
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
