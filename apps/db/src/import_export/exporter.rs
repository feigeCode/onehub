use std::sync::Arc;
use anyhow::Result;

use crate::connection::DbConnection;
use crate::import_export::{DataFormat, ExportConfig, ExportResult, FormatHandler};
use crate::import_export::formats::{CsvFormatHandler, JsonFormatHandler, SqlFormatHandler};
use crate::DatabasePlugin;

pub struct DataExporter;

impl DataExporter {
    pub async fn export(
        plugin: Arc<dyn DatabasePlugin>,
        connection: &dyn DbConnection,
        config: ExportConfig,
    ) -> Result<ExportResult> {
        match config.format {
            DataFormat::Sql => {
                SqlFormatHandler.export(plugin, connection, &config).await
            }
            DataFormat::Json => {
                JsonFormatHandler.export(plugin, connection, &config).await
            }
            DataFormat::Csv => {
                CsvFormatHandler.export(plugin, connection, &config).await
            }
        }
    }
}
