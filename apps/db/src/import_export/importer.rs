use std::sync::Arc;
use anyhow::Result;

use crate::connection::DbConnection;
use crate::DatabasePlugin;
use crate::import_export::{DataFormat, FormatHandler, ImportConfig, ImportResult, ImportProgressSender};
use crate::import_export::formats::{CsvFormatHandler, JsonFormatHandler, SqlFormatHandler};

pub struct DataImporter;

impl DataImporter {
    pub async fn import(
        plugin: Arc<dyn DatabasePlugin>,
        connection: &dyn DbConnection,
        config: ImportConfig,
        data: String,
    ) -> Result<ImportResult> {
        Self::import_with_progress(plugin, connection, config, data, "", None).await
    }

    pub async fn import_with_progress(
        plugin: Arc<dyn DatabasePlugin>,
        connection: &dyn DbConnection,
        config: ImportConfig,
        data: String,
        file_name: &str,
        progress_tx: Option<ImportProgressSender>,
    ) -> Result<ImportResult> {
        match config.format {
            DataFormat::Sql => {
                SqlFormatHandler.import_with_progress(plugin, connection, &config, &data, file_name, progress_tx).await
            }
            DataFormat::Json => {
                JsonFormatHandler.import_with_progress(plugin, connection, &config, &data, file_name, progress_tx).await
            }
            DataFormat::Csv => {
                CsvFormatHandler.import_with_progress(plugin, connection, &config, &data, file_name, progress_tx).await
            }
        }
    }
}
