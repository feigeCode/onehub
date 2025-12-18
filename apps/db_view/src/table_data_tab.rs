use std::any::Any;
use gpui::{AnyElement, App, AppContext, Entity, IntoElement, SharedString, Window};
use gpui_component::IconName;

use crate::data_grid::{DataGrid, DataGridConfig};
use one_core::tab_container::{TabContent, TabContentType};

pub use crate::data_grid::{Page500, Page1000, Page2000, PageAll};

pub struct TableDataTabContent {
    pub data_grid: Entity<DataGrid>,
    database_name: String,
    table_name: String,
}

impl TableDataTabContent {
    pub fn new(
        database_name: String,
        table_name: String,
        connection_id: impl Into<String>,
        database_type: one_core::storage::DatabaseType,
        window: &mut Window,
        cx: &mut App,
    ) -> Self {
        let config = DataGridConfig::new(
            database_name.clone(),
            table_name.clone(),
            connection_id,
            database_type,
        )
        .editable(true)
        .show_toolbar(true);

        let data_grid = cx.new(|cx| DataGrid::new(config, window, cx));

        Self {
            data_grid,
            database_name,
            table_name,
        }
    }
}

impl TabContent for TableDataTabContent {
    fn title(&self) -> SharedString {
        format!("{}.{} - Data", self.database_name, self.table_name).into()
    }

    fn icon(&self) -> Option<IconName> {
        Some(IconName::Folder)
    }

    fn closeable(&self) -> bool {
        true
    }

    fn render_content(&self, _: &mut Window, _: &mut App) -> AnyElement {
        self.data_grid.clone().into_any_element()
    }

    fn content_type(&self) -> TabContentType {
        TabContentType::TableData(format!("{}.{}", self.database_name, self.table_name))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Clone for TableDataTabContent {
    fn clone(&self) -> Self {
        Self {
            data_grid: self.data_grid.clone(),
            database_name: self.database_name.clone(),
            table_name: self.table_name.clone(),
        }
    }
}
