use std::sync::Arc;
use std::time::Instant;

use anyhow::{anyhow, Result};
use async_trait::async_trait;

use crate::connection::DbConnection;
use crate::DatabasePlugin;
use crate::executor::{ExecOptions, SqlResult};
use crate::import_export::{ExportConfig, ExportResult, FormatHandler, ImportConfig, ImportResult};

pub struct CsvFormatHandler;

impl CsvFormatHandler {
    fn parse_csv_line_with_config(line: &str, delimiter: char, qualifier: Option<char>) -> Vec<String> {
        let mut fields = Vec::new();
        let mut current_field = String::new();
        let mut in_quotes = false;
        let mut chars = line.chars().peekable();

        while let Some(ch) = chars.next() {
            if let Some(q) = qualifier {
                if ch == q {
                    if in_quotes {
                        if chars.peek() == Some(&q) {
                            chars.next();
                            current_field.push(q);
                        } else {
                            in_quotes = false;
                        }
                    } else {
                        in_quotes = true;
                    }
                    continue;
                }
            }

            if ch == delimiter && !in_quotes {
                fields.push(current_field.clone());
                current_field.clear();
            } else {
                current_field.push(ch);
            }
        }
        fields.push(current_field);
        fields
    }

    fn escape_csv_field(field: &str) -> String {
        if field.contains(',') || field.contains('"') || field.contains('\n') {
            format!("\"{}\"", field.replace('"', "\"\""))
        } else {
            field.to_string()
        }
    }
}

#[async_trait]
impl FormatHandler for CsvFormatHandler {
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

        let table = config.table.as_ref()
            .ok_or_else(|| anyhow!("Table name required for CSV import"))?;

        let csv_config = config.csv_config.clone().unwrap_or_default();
        let delimiter = csv_config.field_delimiter;
        let qualifier = csv_config.text_qualifier;
        let has_header = csv_config.has_header;

        let lines: Vec<&str> = data.lines().collect();
        if lines.is_empty() {
            return Ok(ImportResult {
                success: true,
                rows_imported: 0,
                errors,
                elapsed_ms: start.elapsed().as_millis(),
            });
        }

        let columns: Vec<String>;
        let data_start_line: usize;

        if has_header {
            columns = Self::parse_csv_line_with_config(lines[0], delimiter, qualifier);
            data_start_line = 1;
        } else {
            let first_row = Self::parse_csv_line_with_config(lines[0], delimiter, qualifier);
            columns = (0..first_row.len())
                .map(|i| format!("col{}", i + 1))
                .collect();
            data_start_line = 0;
        }

        if columns.is_empty() {
            return Err(anyhow!("CSV header is empty"));
        }

        if config.truncate_before_import {
            let truncate_sql = format!("TRUNCATE TABLE {}", plugin.quote_identifier(table));
            let results = connection.execute(plugin.clone(), &truncate_sql, ExecOptions::default()).await
                .map_err(|e| anyhow!("Truncate failed: {}", e))?;

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

        for (line_num, line) in lines.iter().skip(data_start_line).enumerate() {
            if line.trim().is_empty() {
                continue;
            }

            let values = Self::parse_csv_line_with_config(line, delimiter, qualifier);
            if values.len() != columns.len() {
                errors.push(format!("Line {}: column count mismatch", line_num + data_start_line + 1));
                if config.stop_on_error {
                    break;
                }
                continue;
            }

            let mut insert_sql = format!("INSERT INTO {} (", plugin.quote_identifier(table));
            for (i, col) in columns.iter().enumerate() {
                if i > 0 {
                    insert_sql.push_str(", ");
                }
                insert_sql.push_str(&plugin.quote_identifier(col));
            }
            insert_sql.push_str(") VALUES (");

            for (i, val) in values.iter().enumerate() {
                if i > 0 {
                    insert_sql.push_str(", ");
                }
                if val.is_empty() || val.eq_ignore_ascii_case("null") {
                    insert_sql.push_str("NULL");
                } else {
                    insert_sql.push('\'');
                    insert_sql.push_str(&val.replace('\'', "''"));
                    insert_sql.push('\'');
                }
            }
            insert_sql.push(')');

            match connection.execute(plugin.clone(), &insert_sql, ExecOptions::default()).await {
                Ok(results) => {
                    for result in results {
                        match result {
                            SqlResult::Exec(exec_result) => {
                                total_rows += exec_result.rows_affected;
                            }
                            SqlResult::Error(err) => {
                                errors.push(format!("Line {}: {}", line_num + data_start_line + 1, err.message));
                                if config.stop_on_error {
                                    break;
                                }
                            }
                            _ => {}
                        }
                    }
                }
                Err(e) => {
                    errors.push(format!("Line {}: {}", line_num + data_start_line + 1, e));
                    if config.stop_on_error {
                        break;
                    }
                }
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

        for (table_idx, table) in config.tables.iter().enumerate() {
            let table_ref = plugin.format_table_reference(&config.database, None, table);
            let mut select_sql = format!("SELECT * FROM {}", table_ref);
            if let Some(where_clause) = &config.where_clause {
                select_sql.push_str(" WHERE ");
                select_sql.push_str(where_clause);
            }
            if let Some(limit) = config.limit {
                let pagination = plugin.format_pagination(limit, 0, "");
                select_sql.push_str(&pagination);
            }

            let result = connection.query(&select_sql, None, ExecOptions::default()).await
                .map_err(|e| anyhow!("Query failed: {}", e))?;

            if let SqlResult::Query(query_result) = result {
                if table_idx > 0 {
                    output.push_str("\n\n");
                }

                // 写入表头
                for (i, col) in query_result.columns.iter().enumerate() {
                    if i > 0 {
                        output.push(',');
                    }
                    output.push_str(&Self::escape_csv_field(col));
                }
                output.push('\n');

                // 写入数据
                for row in &query_result.rows {
                    for (i, val) in row.iter().enumerate() {
                        if i > 0 {
                            output.push(',');
                        }
                        if let Some(v) = val {
                            output.push_str(&Self::escape_csv_field(v));
                        }
                    }
                    output.push('\n');
                    total_rows += 1;
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
