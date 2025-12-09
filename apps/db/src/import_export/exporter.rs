use anyhow::Result;

use crate::connection::DbConnection;
use crate::import_export::{DataFormat, ExportConfig, ExportResult, FormatHandler};
use crate::import_export::formats::{CsvFormatHandler, JsonFormatHandler, SqlFormatHandler};

pub struct DataExporter;

impl DataExporter {
    pub async fn export(
        connection: &dyn DbConnection,
        config: ExportConfig,
    ) -> Result<ExportResult> {
        match config.format {
            DataFormat::Sql => {
                SqlFormatHandler.export(connection, &config).await
            }
            DataFormat::Json => {
                JsonFormatHandler.export(connection, &config).await
            }
            DataFormat::Csv => {
                CsvFormatHandler.export(connection, &config).await
            }
        }
    }
}
