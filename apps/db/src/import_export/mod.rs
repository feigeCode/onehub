use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::connection::DbConnection;

pub mod formats;
pub mod importer;
pub mod exporter;

// Re-exports
pub use importer::DataImporter;
pub use exporter::DataExporter;

/// 数据格式枚举
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataFormat {
    Sql,
    Json,
    Csv,
}

impl DataFormat {
    pub fn from_extension(ext: &str) -> Option<Self> {
        match ext.to_lowercase().as_str() {
            "sql" => Some(Self::Sql),
            "json" => Some(Self::Json),
            "csv" => Some(Self::Csv),
            _ => None,
        }
    }

    pub fn extension(&self) -> &str {
        match self {
            Self::Sql => "sql",
            Self::Json => "json",
            Self::Csv => "csv",
        }
    }
}

/// CSV导入配置
#[derive(Debug, Clone)]
pub struct CsvImportConfig {
    pub field_delimiter: char,
    pub text_qualifier: Option<char>,
    pub has_header: bool,
    pub record_terminator: String,
}

impl Default for CsvImportConfig {
    fn default() -> Self {
        Self {
            field_delimiter: ',',
            text_qualifier: Some('"'),
            has_header: true,
            record_terminator: "\n".to_string(),
        }
    }
}

/// 导入配置
#[derive(Debug, Clone)]
pub struct ImportConfig {
    pub format: DataFormat,
    pub database: String,
    pub table: Option<String>,
    pub stop_on_error: bool,
    pub use_transaction: bool,
    pub truncate_before_import: bool,
    pub csv_config: Option<CsvImportConfig>,
}

impl Default for ImportConfig {
    fn default() -> Self {
        Self {
            format: DataFormat::Sql,
            database: String::new(),
            table: None,
            stop_on_error: true,
            use_transaction: true,
            truncate_before_import: false,
            csv_config: None,
        }
    }
}

/// 导出配置
#[derive(Debug, Clone)]
pub struct ExportConfig {
    pub format: DataFormat,
    pub database: String,
    pub tables: Vec<String>,
    pub include_schema: bool,
    pub include_data: bool,
    pub where_clause: Option<String>,
    pub limit: Option<usize>,
}

impl Default for ExportConfig {
    fn default() -> Self {
        Self {
            format: DataFormat::Sql,
            database: String::new(),
            tables: Vec::new(),
            include_schema: true,
            include_data: true,
            where_clause: None,
            limit: None,
        }
    }
}

/// 导入结果
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImportResult {
    pub success: bool,
    pub rows_imported: u64,
    pub errors: Vec<String>,
    pub elapsed_ms: u128,
}

/// 导出结果
#[derive(Debug, Clone)]
pub struct ExportResult {
    pub success: bool,
    pub output: String,
    pub rows_exported: u64,
    pub elapsed_ms: u128,
}

/// 格式处理器trait
#[async_trait]
pub trait FormatHandler: Send + Sync {
    /// 导入数据
    async fn import(
        &self,
        connection: &dyn DbConnection,
        config: &ImportConfig,
        data: &str,
    ) -> Result<ImportResult>;

    /// 导出数据
    async fn export(
        &self,
        connection: &dyn DbConnection,
        config: &ExportConfig,
    ) -> Result<ExportResult>;
}
