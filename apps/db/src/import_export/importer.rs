use anyhow::Result;

use crate::connection::DbConnection;
use crate::import_export::{DataFormat, FormatHandler, ImportConfig, ImportResult};
use crate::import_export::formats::{CsvFormatHandler, JsonFormatHandler, SqlFormatHandler};

pub struct DataImporter;

impl DataImporter {
    pub async fn import(
        connection: &dyn DbConnection,
        config: ImportConfig,
        data: String,
    ) -> Result<ImportResult> {
        match config.format {
            DataFormat::Sql => {
                SqlFormatHandler.import(connection, &config, &data).await
            }
            DataFormat::Json => {
                JsonFormatHandler.import(connection, &config, &data).await
            }
            DataFormat::Csv => {
                CsvFormatHandler.import(connection, &config, &data).await
            }
        }
    }
}
