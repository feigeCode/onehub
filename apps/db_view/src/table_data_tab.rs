use std::any::Any;

use gpui::{div, AnyElement, App, AppContext, ClickEvent, Entity, FocusHandle, Focusable, IntoElement, ParentElement, SharedString, Styled, Window};
use gpui_component::{
    button::{Button, ButtonVariants as _},
    h_flex,
    table::Column,
    v_flex, ActiveTheme as _, IconName, Sizable as _, Size,
};

use crate::data_grid::{DataGrid, DataGridConfig};
use crate::filter_editor::{ColumnSchema, TableFilterEditor, TableSchema};
use db::{GlobalDbState, TableDataRequest};
use one_core::tab_container::{TabContent, TabContentType};

// ============================================================================
// Table Data Tab Content - Display table rows
// ============================================================================

pub struct TableDataTabContent {
    database_name: String,
    table_name: String,
    connection_id: String,
    database_type: one_core::storage::DatabaseType,
    /// DataGrid组件用于显示和编辑数据
    data_grid: DataGrid,
    status_msg: Entity<String>,
    focus_handle: FocusHandle,
    /// Filter editor with WHERE and ORDER BY inputs
    filter_editor: Entity<TableFilterEditor>,
}

impl TableDataTabContent {
    pub fn new(
        database_name: impl Into<String>,
        table_name: impl Into<String>,
        connection_id: impl Into<String>,
        database_type: one_core::storage::DatabaseType,
        window: &mut Window,
        cx: &mut App,
    ) -> Self {
        let database_name = database_name.into();
        let table_name = table_name.into();
        let connection_id = connection_id.into();

        // 创建DataGrid配置
        let config = DataGridConfig::new(
            database_name.clone(),
            table_name.clone(),
            connection_id.clone(),
            database_type,
        )
        .editable(true)
        .show_toolbar(true)
        .show_pagination(true);

        let data_grid = DataGrid::new(config, window, cx);
        let status_msg = cx.new(|_| "Loading...".to_string());
        let focus_handle = cx.focus_handle();

        // Create filter editor with empty schema initially
        let filter_editor = cx.new(|cx| TableFilterEditor::new(window, cx));

        let result = Self {
            database_name: database_name.clone(),
            table_name: table_name.clone(),
            connection_id,
            database_type,
            data_grid,
            status_msg,
            focus_handle,
            filter_editor,
        };

        // Load data initially
        eprintln!("TableDataTabContent::new - about to load data for table: {}.{}", database_name, table_name);
        result.load_data_with_clauses(1, cx);

        result
    }

    fn update_status(&self, message: String, cx: &mut App) {
        self.status_msg.update(cx, |s, cx| {
            *s = message;
            cx.notify();
        });
    }

    fn load_data_with_clauses(&self, page: usize, cx: &mut App) {
        eprintln!("load_data_with_clauses called for page: {}, table: {}.{}", page, self.database_name, self.table_name);
        let global_state = cx.global::<GlobalDbState>().clone();
        let connection_id = self.connection_id.clone();
        let table_name = self.table_name.clone();
        let database_name = self.database_name.clone();
        let status_msg = self.status_msg.clone();
        let data_grid = self.data_grid.clone();
        let page_size = *self.data_grid.page_size().read(cx);
        let where_clause = self.filter_editor.read(cx).get_where_clause(cx);
        let order_by_clause = self.filter_editor.read(cx).get_order_by_clause(cx);
        let filter_editor = self.filter_editor.clone();

        cx.spawn(async move |cx| {
            let start_time = std::time::Instant::now();

            let (plugin, conn_arc) = match global_state.get_plugin_and_connection(&connection_id).await {
                Ok(result) => result,
                Err(e) => {
                    cx.update(|cx| {
                        status_msg.update(cx, |s, cx| {
                            *s = format!("Failed to get connection: {}", e);
                            cx.notify();
                        });
                    }).ok();
                    return;
                }
            };

            let conn = conn_arc.read().await;

            // Build request with raw where/order by clauses
            let request = if page_size == 0 {
                TableDataRequest::new(&database_name, &table_name)
                    .with_where_clause(where_clause.clone())
                    .with_order_by_clause(order_by_clause.clone())
            } else {
                TableDataRequest::new(&database_name, &table_name)
                    .with_page(page, page_size)
                    .with_where_clause(where_clause.clone())
                    .with_order_by_clause(order_by_clause.clone())
            };

            match plugin.query_table_data(&**conn, &request).await {
                Ok(response) => {
                    let columns: Vec<Column> = response
                        .columns
                        .iter()
                        .map(|col| Column::new(col.name.clone(), col.name.clone()))
                        .collect();

                    let rows: Vec<Vec<String>> = response
                        .rows
                        .iter()
                        .map(|row| {
                            row.iter()
                                .map(|cell| cell.as_ref().map(|s| s.to_string()).unwrap_or_else(|| "NULL".to_string()))
                                .collect()
                        })
                        .collect();

                    let row_count = rows.len();
                    let total = response.total_count;
                    let total_pages = if page_size == 0 { 1 } else { (total + page_size - 1) / page_size };
                    let pk_columns = response.primary_key_indices;
                    let duration = start_time.elapsed().as_millis();
                    let sql_str = response.executed_sql;

                    // Build column schema for completion providers
                    let column_schemas: Vec<ColumnSchema> = response
                        .columns
                        .iter()
                        .map(|col| ColumnSchema {
                            name: col.name.clone(),
                            data_type: col.db_type.clone(),
                            is_nullable: col.nullable,
                        })
                        .collect();

                    cx.update(|cx| {
                        // Update filter editor schema
                        filter_editor.update(cx, |editor, cx| {
                            editor.set_schema(TableSchema {
                                table_name: table_name.clone(),
                                columns: column_schemas,
                            }, cx);
                        });

                        // Debug: 打印数据信息
                        eprintln!("Loading data: {} columns, {} rows", columns.len(), rows.len());
                        if !columns.is_empty() {
                            eprintln!("First column: {}", columns[0].name);
                        }
                        if !rows.is_empty() && !rows[0].is_empty() {
                            eprintln!("First row first cell: {}", rows[0][0]);
                        }

                        // Update DataGrid
                        data_grid.update_data(columns, rows, pk_columns, cx);
                        data_grid.update_pagination(page, total, cx);
                        data_grid.update_query_info(duration, sql_str, cx);
                        data_grid.update_status(
                            format!("Page {}/{} ({} rows, {} total)", page, total_pages.max(1), row_count, total),
                            cx,
                        );

                        status_msg.update(cx, |s, cx| {
                            *s = format!("Page {}/{} ({} rows, {} total)", page, total_pages.max(1), row_count, total);
                            cx.notify();
                        });
                    }).ok();
                }
                Err(e) => {
                    cx.update(|cx| {
                        status_msg.update(cx, |s, cx| {
                            *s = format!("Query failed: {}", e);
                            cx.notify();
                        });
                    }).ok();
                }
            }
        }).detach();
    }

    fn handle_refresh(&self, cx: &mut App) {
        let page = *self.data_grid.current_page().read(cx);
        self.load_data_with_clauses(page, cx);
    }

    fn handle_prev_page(&self, cx: &mut App) {
        let page = *self.data_grid.current_page().read(cx);
        if page > 1 {
            self.load_data_with_clauses(page - 1, cx);
        }
    }

    fn handle_next_page(&self, cx: &mut App) {
        let page = *self.data_grid.current_page().read(cx);
        let total = *self.data_grid.total_count().read(cx);
        let page_size = *self.data_grid.page_size().read(cx);
        if page_size == 0 {
            return;
        }
        let total_pages = (total + page_size - 1) / page_size;
        if page < total_pages {
            self.load_data_with_clauses(page + 1, cx);
        }
    }

    fn handle_page_size_change(&self, new_size: usize, cx: &mut App) {
        self.data_grid.page_size().update(cx, |size, cx| {
            *size = new_size;
            cx.notify();
        });
        self.load_data_with_clauses(1, cx);
    }

    fn handle_apply_query(&self, cx: &mut App) {
        self.load_data_with_clauses(1, cx);
    }

    fn handle_save_changes(&self, cx: &mut App) {
        let save_request = match self.data_grid.create_save_request(cx) {
            Some(req) => req,
            None => {
                self.update_status("No changes to save".to_string(), cx);
                return;
            }
        };

        let change_count = save_request.changes.len();
        self.update_status(format!("Saving {} changes...", change_count), cx);

        let global_state = cx.global::<GlobalDbState>().clone();
        let connection_id = self.connection_id.clone();
        let status_msg = self.status_msg.clone();
        let data_grid = self.data_grid.clone();

        cx.spawn(async move |cx| {
            let (plugin, conn_arc) = match global_state.get_plugin_and_connection(&connection_id).await {
                Ok(result) => result,
                Err(e) => {
                    cx.update(|cx| {
                        status_msg.update(cx, |s, cx| {
                            *s = format!("Failed to get connection: {}", e);
                            cx.notify();
                        });
                    }).ok();
                    return;
                }
            };

            let conn = conn_arc.read().await;

            match plugin.apply_table_changes(&**conn, save_request).await {
                Ok(response) => {
                    cx.update(|cx| {
                        if response.errors.is_empty() {
                            data_grid.clear_changes(cx);
                            status_msg.update(cx, |s, cx| {
                                *s = format!("Successfully saved {} changes", response.success_count);
                                cx.notify();
                            });
                        } else {
                            status_msg.update(cx, |s, cx| {
                                *s = format!(
                                    "Saved {} changes, {} errors: {}",
                                    response.success_count,
                                    response.errors.len(),
                                    response.errors.first().unwrap_or(&String::new())
                                );
                                cx.notify();
                            });
                        }
                    }).ok();
                }
                Err(e) => {
                    cx.update(|cx| {
                        status_msg.update(cx, |s, cx| {
                            *s = format!("Failed to save changes: {}", e);
                            cx.notify();
                        });
                    }).ok();
                }
            }
        }).detach();
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

    fn render_content(&self, window: &mut Window, cx: &mut App) -> AnyElement {
        // 处理单元格选择（如果编辑器可见）
        let is_editor_visible = *self.data_grid.editor_visible().read(cx);
        if is_editor_visible {
            let selected_cell = self.data_grid.table().read(cx).selected_cell();
            let last_editing_pos = *self.data_grid.editing_large_text().read(cx);

            if let Some((row_ix, col_ix)) = selected_cell {
                if Some((row_ix, col_ix)) != last_editing_pos {
                    self.data_grid.handle_cell_selection(row_ix, col_ix, window, cx);
                }
            }
        }

        let this = self.clone();
        let this_refresh = self.clone();
        let this_save = self.clone();
        let this_prev = self.clone();
        let this_next = self.clone();

        v_flex()
            .size_full()
            .gap_2()
            .pt_2()
            // 工具栏
            .child(
                self.data_grid.render_toolbar(
                    move |cx| this_refresh.handle_refresh(cx),
                    move |cx| this_save.handle_save_changes(cx),
                    window,
                    cx,
                )
            )
            // 过滤器栏
            .child(
                h_flex()
                    .gap_2()
                    .items_center()
                    .w_full()
                    .px_2()
                    .child(self.filter_editor.clone())
                    .child(
                        Button::new("apply-query")
                            .with_size(Size::Small)
                            .primary()
                            .label("Apply")
                            .icon(IconName::Check)
                            .on_click({
                                let this = self.clone();
                                move |_, _, cx| this.handle_apply_query(cx)
                            }),
                    ),
            )
            // 表格区域
            .child(
                div()
                    .flex_1()
                    .w_full()
                    .overflow_hidden()
                    .child(self.data_grid.render_table_area(window, cx))
            )
            // 状态栏
            .child(
                self.data_grid.render_status_bar(
                    move |cx| this_prev.handle_prev_page(cx),
                    move |cx| this_next.handle_next_page(cx),
                    {
                        let this = this.clone();
                        move |size, cx| this.handle_page_size_change(size, cx)
                    },
                    cx,
                )
            )
            .into_any_element()
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
            database_name: self.database_name.clone(),
            table_name: self.table_name.clone(),
            connection_id: self.connection_id.clone(),
            database_type: self.database_type,
            data_grid: self.data_grid.clone(),
            status_msg: self.status_msg.clone(),
            focus_handle: self.focus_handle.clone(),
            filter_editor: self.filter_editor.clone(),
        }
    }
}

impl Focusable for TableDataTabContent {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}


