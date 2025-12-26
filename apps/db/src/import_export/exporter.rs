use std::sync::Arc;
use anyhow::Result;

use crate::connection::DbConnection;
use crate::import_export::{DataFormat, ExportConfig, ExportResult, FormatHandler, ExportProgressSender};
use crate::import_export::formats::{CsvFormatHandler, JsonFormatHandler, SqlFormatHandler};
use crate::DatabasePlugin;

pub struct DataExporter;

impl DataExporter {
    pub async fn export(
        plugin: Arc<dyn DatabasePlugin>,
        connection: &dyn DbConnection,
        config: ExportConfig,
    ) -> Result<ExportResult> {
        Self::export_with_progress(plugin, connection, config, None).await
    }

    pub async fn export_with_progress(
        plugin: Arc<dyn DatabasePlugin>,
        connection: &dyn DbConnection,
        config: ExportConfig,
        progress_tx: Option<ExportProgressSender>,
    ) -> Result<ExportResult> {
        match config.format {
            DataFormat::Sql => {
                SqlFormatHandler.export_with_progress(plugin, connection, &config, progress_tx).await
            }
            DataFormat::Json => {
                JsonFormatHandler.export_with_progress(plugin, connection, &config, progress_tx).await
            }
            DataFormat::Csv => {
                CsvFormatHandler.export_with_progress(plugin, connection, &config, progress_tx).await
            }
        }
    }
}
