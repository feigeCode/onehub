use std::sync::Arc;
use anyhow::Result;

use crate::connection::DbConnection;
use crate::DatabasePlugin;
use crate::import_export::{DataFormat, FormatHandler, ImportConfig, ImportResult};
use crate::import_export::formats::{CsvFormatHandler, JsonFormatHandler, SqlFormatHandler};

pub struct DataImporter;

impl DataImporter {
    pub async fn import(
        plugin: Arc<dyn DatabasePlugin>,
        connection: &dyn DbConnection,
        config: ImportConfig,
        data: String,
    ) -> Result<ImportResult> {
        match config.format {
            DataFormat::Sql => {
                SqlFormatHandler.import(plugin, connection, &config, &data).await
            }
            DataFormat::Json => {
                JsonFormatHandler.import(plugin, connection, &config, &data).await
            }
            DataFormat::Csv => {
                CsvFormatHandler.import(plugin, connection, &config, &data).await
            }
        }
    }
}
