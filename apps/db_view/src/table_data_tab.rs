use std::any::Any;
use std::marker::PhantomData;

use gpui::{div, AnyElement, App, AppContext, ClickEvent, Corner, Entity, FocusHandle, Focusable, IntoElement, ParentElement, Pixels, SharedString, Styled, Subscription, Window, px};
use gpui_component::{
    button::{Button, ButtonVariants as _},
    h_flex,
    menu::{DropdownMenu as _, PopupMenuItem},
    resizable::{resizable_panel, v_resizable},
    table::{Column, Table, TableState},
    v_flex, ActiveTheme as _, IconName, Sizable as _, Size,
};

use crate::filter_editor::{ColumnSchema, TableFilterEditor, TableSchema};
use crate::multi_text_editor::{create_multi_text_editor_with_content, MultiTextEditor};
use crate::results_delegate::{EditorTableDelegate, RowChange};
use db::{GlobalDbState, TableCellChange, TableDataRequest, TableRowChange, TableSaveRequest};
use one_core::tab_container::{TabContent, TabContentType};
// ============================================================================
// Table Data Tab Content - Display table rows
// ============================================================================

pub struct TableDataTabContent {
    database_name: String,
    table_name: String,
    connection_id: String,
    table: Entity<TableState<EditorTableDelegate>>,
    status_msg: Entity<String>,
    focus_handle: FocusHandle,
    /// Text editor for large text editing (MultiTextEditor)
    text_editor: Entity<MultiTextEditor>,
    /// Currently editing cell position
    editing_large_text: Entity<Option<(usize, usize)>>,
    /// Current page (1-based)
    current_page: Entity<usize>,
    /// Page size (dynamic, supports 500, 1000, 2000, or 0 for all)
    page_size: Entity<usize>,
    /// Total row count
    total_count: Entity<usize>,
    /// Filter editor with WHERE and ORDER BY inputs
    filter_editor: Entity<TableFilterEditor>,
    /// Editor visibility state
    editor_visible: Entity<bool>,
    /// Subscription to table events (stored but not used directly)
    _table_subscription: Option<Subscription>,
    /// Query duration in milliseconds
    query_duration: Entity<u128>,
    /// Current query SQL
    current_sql: Entity<String>,
    /// Marker to make the struct Send + Sync
    _phantom: PhantomData<*const ()>,
}

impl TableDataTabContent {
    pub fn new(
        database_name: impl Into<String>,
        table_name: impl Into<String>,
        connection_id: impl Into<String>,
        window: &mut Window,
        cx: &mut App,
    ) -> Self {
        let database_name = database_name.into();
        let table_name = table_name.into();
        let connection_id = connection_id.into();
        let table = cx.new(|cx| TableState::new(EditorTableDelegate::new(vec![], vec![], window, cx), window, cx));
        let status_msg = cx.new(|_| "Loading...".to_string());
        let focus_handle = cx.focus_handle();
        let editing_large_text = cx.new(|_| None);
        let current_page = cx.new(|_| 1usize);
        let page_size = cx.new(|_| 500usize);
        let total_count = cx.new(|_| 0usize);

        // Create filter editor with empty schema initially
        let filter_editor = cx.new(|cx| TableFilterEditor::new(window, cx));


        // Editor visibility state (default hidden)
        let editor_visible = cx.new(|_| false);

        // Create multi text editor for cell editing
        let text_editor = create_multi_text_editor_with_content(None, window, cx);

        // Query duration and SQL
        let query_duration = cx.new(|_| 0u128);
        let current_sql = cx.new(|_| String::new());

        let result = Self {
            database_name: database_name.clone(),
            table_name: table_name.clone(),
            connection_id,
            table: table.clone(),
            status_msg: status_msg.clone(),
            focus_handle,
            text_editor: text_editor.clone(),
            editing_large_text: editing_large_text.clone(),
            current_page,
            page_size,
            total_count,
            filter_editor,
            editor_visible: editor_visible.clone(),
            _table_subscription: None,
            query_duration,
            current_sql,
            _phantom: PhantomData,
        };
        
        // Load data initially
        result.load_data_with_clauses(1, cx);

        result
    }

    fn update_status(status_msg: &Entity<String>, message: String, cx: &mut App) {
        status_msg.update(cx, |s, cx| {
            *s = message;
            cx.notify();
        });
    }

    fn load_data_with_clauses(&self, page: usize, cx: &mut App) {
        // Clear all column filters when loading new data (Requirements: 7.1, 7.2, 7.3)
        // TODO self.clear_all_filters(cx);

        let global_state = cx.global::<GlobalDbState>().clone();
        let connection_id = self.connection_id.clone();
        let table_name = self.table_name.clone();
        let database_name = self.database_name.clone();
        let status_msg = self.status_msg.clone();
        let table_state = self.table.clone();
        let current_page = self.current_page.clone();
        let total_count = self.total_count.clone();
        let page_size = *self.page_size.read(cx);
        let where_clause = self.filter_editor.read(cx).get_where_clause(cx);
        let order_by_clause = self.filter_editor.read(cx).get_order_by_clause(cx);
        let filter_editor = self.filter_editor.clone();
        let query_duration = self.query_duration.clone();
        let current_sql = self.current_sql.clone();

        cx.spawn(async move |cx| {
            let start_time = std::time::Instant::now();

            let (plugin, conn_arc) = match global_state.get_plugin_and_connection(&connection_id).await {
                Ok(result) => result,
                Err(e) => {
                    cx.update(|cx| {
                        Self::update_status(&status_msg, format!("Failed to get connection: {}", e), cx);
                    }).ok();
                    return;
                }
            };

            let conn = conn_arc.read().await;

            // Build request with raw where/order by clauses
            // page_size == 0 means fetch all records
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

                        table_state.update(cx, |state, cx| {
                            state.delegate_mut().update_data(columns, rows,cx);
                            state.delegate_mut().set_primary_keys(pk_columns);
                            state.refresh(cx);
                        });

                        current_page.update(cx, |p, cx| {
                            *p = page;
                            cx.notify();
                        });

                        total_count.update(cx, |t, cx| {
                            *t = total;
                            cx.notify();
                        });

                        query_duration.update(cx, |d, cx| {
                            *d = duration;
                            cx.notify();
                        });

                        current_sql.update(cx, |sql, cx| {
                            *sql = sql_str;
                            cx.notify();
                        });

                        Self::update_status(
                            &status_msg,
                            format!("Page {}/{} ({} rows, {} total)", page, total_pages.max(1), row_count, total),
                            cx,
                        );
                    })
                    .ok();
                }
                Err(e) => {
                    cx.update(|cx| {
                        Self::update_status(&status_msg, format!("Query failed: {}", e), cx);
                    }).ok();
                }
            }
        })
        .detach();
    }

    fn handle_refresh(&self, _: &ClickEvent, _: &mut Window, cx: &mut App) {
        let page = *self.current_page.read(cx);
        self.load_data_with_clauses(page, cx);
    }

    fn handle_prev_page(&self, cx: &mut App) {
        let page = *self.current_page.read(cx);
        if page > 1 {
            self.load_data_with_clauses(page - 1, cx);
        }
    }

    fn handle_next_page(&self, cx: &mut App) {
        let page = *self.current_page.read(cx);
        let total = *self.total_count.read(cx);
        let page_size = *self.page_size.read(cx);
        if page_size == 0 {
            return; // No pagination when showing all
        }
        let total_pages = (total + page_size - 1) / page_size;
        if page < total_pages {
            self.load_data_with_clauses(page + 1, cx);
        }
    }

    fn handle_page_size_change(&self, new_size: usize, cx: &mut App) {
        self.page_size.update(cx, |size, cx| {
            *size = new_size;
            cx.notify();
        });
        self.load_data_with_clauses(1, cx);
    }

    fn handle_apply_query(&self, cx: &mut App) {
        self.load_data_with_clauses(1, cx);
    }

    fn handle_save_changes(&self, cx: &mut App) {
        let (changes, column_names, pk_columns) = {
            let delegate = self.table.read(cx).delegate();
            (
                delegate.get_changes(),
                delegate.column_names(),
                delegate.primary_key_columns().to_vec(),
            )
        };

        if changes.is_empty() {
            Self::update_status(&self.status_msg, "No changes to save".to_string(), cx);
            return;
        }

        let table_changes = Self::convert_row_changes(changes, &column_names);
        if table_changes.is_empty() {
            Self::update_status(&self.status_msg, "No valid changes to save".to_string(), cx);
            return;
        }

        let save_request = Self::create_table_save_request(
            self.database_name.clone(),
            self.table_name.clone(),
            column_names,
            pk_columns,
            table_changes,
        );

        let change_count = save_request.changes.len();
        Self::update_status(
            &self.status_msg,
            format!("Saving {} changes...", change_count),
            cx,
        );

        let global_state = cx.global::<GlobalDbState>().clone();
        let connection_id = self.connection_id.clone();
        let status_msg = self.status_msg.clone();
        let table_state = self.table.clone();

        cx.spawn(async move |cx| {
            let (plugin, conn_arc) = match global_state.get_plugin_and_connection(&connection_id).await {
                Ok(result) => result,
                Err(e) => {
                    cx.update(|cx| {
                        Self::update_status(&status_msg, format!("Failed to get connection: {}", e), cx);
                    }).ok();
                    return;
                }
            };

            let conn = conn_arc.read().await;

            match plugin.apply_table_changes(&**conn, save_request).await {
                Ok(response) => {
                    cx.update(|cx| {
                        if response.errors.is_empty() {
                            table_state.update(cx, |state, cx| {
                                state.delegate_mut().clear_changes();
                                cx.notify();
                            });
                            Self::update_status(
                                &status_msg,
                                format!("Successfully saved {} changes", response.success_count),
                                cx,
                            );
                        } else {
                            Self::update_status(
                                &status_msg,
                                format!(
                                    "Saved {} changes, {} errors: {}",
                                    response.success_count,
                                    response.errors.len(),
                                    response.errors.first().unwrap_or(&String::new())
                                ),
                                cx,
                            );
                        }
                    }).ok();
                }
                Err(e) => {
                    cx.update(|cx| {
                        Self::update_status(&status_msg, format!("Failed to save changes: {}", e), cx);
                    }).ok();
                }
            }
        })
        .detach();
    }

    fn create_table_save_request(
        database_name: String,
        table_name: String,
        column_names: Vec<String>,
        pk_columns: Vec<usize>,
        changes: Vec<TableRowChange>,
    ) -> TableSaveRequest {
        TableSaveRequest {
            database: database_name,
            table: table_name,
            column_names,
            primary_key_indices: pk_columns,
            changes,
        }
    }

    fn convert_row_changes(changes: Vec<RowChange>, column_names: &[String]) -> Vec<TableRowChange> {
        changes
            .into_iter()
            .filter_map(|change| match change {
                RowChange::Added { data } => Some(TableRowChange::Added { data }),
                RowChange::Updated { original_data, changes } => {
                    let converted_changes: Vec<TableCellChange> = changes
                        .into_iter()
                        .map(|c| TableCellChange {
                            column_index: c.col_ix,
                            column_name: if c.col_name.is_empty() {
                                column_names.get(c.col_ix).cloned().unwrap_or_default()
                            } else {
                                c.col_name
                            },
                            old_value: c.old_value,
                            new_value: c.new_value,
                        })
                        .collect();

                    if converted_changes.is_empty() {
                        None
                    } else {
                        Some(TableRowChange::Updated {
                            original_data,
                            changes: converted_changes,
                        })
                    }
                }
                RowChange::Deleted { original_data } => Some(TableRowChange::Deleted { original_data }),
            })
            .collect()
    }

    fn load_cell_to_editor(&self, window: &mut Window, cx: &mut App) {
        let table = self.table.read(cx);
        let selected_row = table.selected_cell();
        let editing_cell = table.editing_cell();

        // Get the cell to load - prefer editing cell, then selected cell
        let (row_ix, col_ix) = if let Some((r, c)) = editing_cell {
            (r, c)
        } else if let Some((r, c)) = selected_row {
            (r, c)
        } else {
            Self::update_status(&self.status_msg, "Please select a cell first".to_string(), cx);
            return;
        };

        // Get current cell value
        let value = table
            .delegate()
            .rows
            .get(row_ix)
            .and_then(|r| r.get(col_ix - 1))
            .cloned()
            .unwrap_or_default();

        // Set the value in MultiTextEditor
        self.text_editor.update(cx, |editor, cx| {
            editor.set_active_text(value, window, cx);
        });

        // Store the editing position
        self.editing_large_text.update(cx, |pos, cx| {
            *pos = Some((row_ix, col_ix));
            cx.notify();
        });
    }

    fn handle_cell_selection(&self, row_ix: usize, col_ix: usize, window: &mut Window, cx: &mut App) {
        let is_visible = *self.editor_visible.read(cx);
        if !is_visible {
            return;
        }

        // Step 1: Save old content back to table if there was a previous editing position
        let old_pos = *self.editing_large_text.read(cx);
        if let Some((old_row, old_col)) = old_pos {
            // Get content from editor
            let editor_content = self.text_editor.read(cx).get_active_text(cx);
            
            // Update table cell directly
            self.table.update(cx, |state, cx| {
                let delegate = state.delegate_mut();
                if let Some(row) = delegate.rows.get_mut(old_row) {
                    if let Some(cell) = row.get_mut(old_col - 1) {
                        if *cell != editor_content {
                            *cell = editor_content.clone();
                            delegate.modified_cells.insert((old_row, old_col - 1));
                        }
                    }
                }
                state.refresh(cx);
            });
        }

        // Step 2: Load new cell value to editor
        let value = self.table.read(cx)
            .delegate()
            .rows
            .get(row_ix)
            .and_then(|r| r.get(col_ix - 1))
            .cloned()
            .unwrap_or_default();

        self.text_editor.update(cx, |editor, cx| {
            editor.set_active_text(value, window, cx);
        });

        // Step 3: Update editing position
        self.editing_large_text.update(cx, |pos, cx| {
            *pos = Some((row_ix, col_ix));
            cx.notify();
        });
    }

    fn toggle_editor(&self, window: &mut Window, cx: &mut App) {
        let is_visible = *self.editor_visible.read(cx);
        
        if is_visible {
            // Hide editor
            self.editor_visible.update(cx, |visible, cx| {
                *visible = false;
                cx.notify();
            });
            
            // Clear editing position
            self.editing_large_text.update(cx, |pos, cx| {
                *pos = None;
                cx.notify();
            });
        } else {
            // Show editor and load cell
            self.load_cell_to_editor(window, cx);

            // Show editor
            self.editor_visible.update(cx, |visible, cx| {
                *visible = true;
                cx.notify();
            });
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

    fn render_content(&self, window: &mut Window, cx: &mut App) -> AnyElement {
        let status_msg_render = self.status_msg.clone();
        let query_duration_render = self.query_duration.clone();
        let current_sql_render = self.current_sql.clone();
        let total_count_render = self.total_count.clone();
        let current_page_render = self.current_page.clone();
        
        // Get filtered row count for status display (Requirements: 5.5)
        let filtered_count = self.table.read(cx).delegate().filtered_row_count();
        let total_rows = self.table.read(cx).delegate().rows.len();

        // Handle cell selection if editor is visible
        let selected_cell = self.table.read(cx).selected_cell();
        let last_editing_pos = *self.editing_large_text.read(cx);

        // Check if selection changed and editor is visible
        if let Some((row_ix, col_ix)) = selected_cell {
            if Some((row_ix, col_ix)) != last_editing_pos && *self.editor_visible.read(cx) {
                self.handle_cell_selection(row_ix, col_ix, window, cx);
            }
        }

        v_flex()
            .size_full()
            .gap_2()
            .pt_2()
            .child(
                // Top Toolbar - Action buttons
                h_flex()
                    .gap_2()
                    .items_center()
                    .px_2()
                    .child(
                        Button::new("refresh-data")
                            .with_size(Size::Small)
                            .label("Refresh")
                            .icon(IconName::ArrowDown)
                            .on_click({
                                let this = self.clone();
                                move |e, w, cx| this.handle_refresh(e, w, cx)
                            }),
                    )
                    .child(
                        Button::new("add-row")
                            .with_size(Size::Small)
                            .label("Add Row")
                            .icon(IconName::Plus)
                            .on_click({
                                let table = self.table.clone();
                                move |_, w, cx| {
                                    table.update(cx, |state, cx| {
                                        state.add_row(w, cx);
                                    });
                                }
                            }),
                    )
                    .child(
                        Button::new("delete-row")
                            .with_size(Size::Small)
                            .label("Delete Row")
                            .icon(IconName::Delete)
                            .on_click({
                                let table = self.table.clone();
                                move |_, w, cx| {
                                    table.update(cx, |state, cx| {
                                        if let Some(row_ix) = state.selected_row() {
                                            state.delete_row(row_ix, w, cx);
                                        }
                                    });
                                }
                            }),
                    )
                    .child(
                        Button::new("save-changes")
                            .with_size(Size::Small)
                            .label("Save Changes")
                            .icon(IconName::Check)
                            .on_click({
                                let this = self.clone();
                                move |_, _, cx| {
                                    this.handle_save_changes(cx);
                                }
                            }),
                    )
                    .child({
                        let is_editor_visible = *self.editor_visible.read(cx);
                        let mut btn = Button::new("load-to-editor")
                            .with_size(Size::Small)
                            .label("Load to Editor")
                            .icon(IconName::ArrowDown);

                        if is_editor_visible {
                            btn = btn.primary();
                        }

                        btn.on_click({
                            let this = self.clone();
                            move |_, w, cx| {
                                this.toggle_editor(w, cx);
                            }
                        })
                    }),
            )
            .child(
                // Query bar with filter editor and apply button
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
            .child({
                let is_editor_visible = *self.editor_visible.read(cx);

                if is_editor_visible {
                    // Resizable split: Table (top) and Editor (bottom)
                    div()
                        .flex_1()
                        .w_full()
                        .overflow_hidden()
                        .child(
                            v_resizable("table-editor-split")
                                .child(
                                    resizable_panel()
                                        .size(px(400.))
                                        .size_range(px(200.)..Pixels::MAX)
                                        .child(
                                            div()
                                                .size_full()
                                                .bg(cx.theme().background)
                                                .border_1()
                                                .border_color(cx.theme().border)
                                                .overflow_hidden()
                                                .child(
                                                    Table::new(&self.table)
                                                        .stripe(true)
                                                        .bordered(false)
                                                ),
                                        ),
                                )
                                .child(
                                    resizable_panel()
                                        .size(px(200.))
                                        .size_range(px(100.)..Pixels::MAX)
                                        .child(
                                            div()
                                                .size_full()
                                                .bg(cx.theme().background)
                                                .border_1()
                                                .border_color(cx.theme().border)
                                                .overflow_hidden()
                                                .child(self.text_editor.clone()),
                                        ),
                                ),
                        )
                } else {
                    // Only show table with scrollbars
                    div()
                        .flex_1()
                        .w_full()
                        .bg(cx.theme().background)
                        .border_1()
                        .border_color(cx.theme().border)
                        .overflow_hidden()
                        .child(
                            Table::new(&self.table)
                                .stripe(false)
                                .bordered(true)
                        )
                }
            })
            .child(
                // Bottom Toolbar - Status and pagination
                h_flex()
                    .gap_3()
                    .items_center()
                    .px_2()
                    .py_1()
                    .border_t_1()
                    .border_color(cx.theme().border)
                    .bg(cx.theme().background)
                    // Record count display (Requirements: 5.5)
                    .child(
                        div()
                            .text_sm()
                            .text_color(cx.theme().foreground)
                            .child({
                                // Show filtered count if filtering is active
                                if filtered_count < total_rows {
                                    format!(
                                        "显示 {} 条（共 {} 条，总计 {} 条）",
                                        filtered_count,
                                        total_rows,
                                        total_count_render.read(cx)
                                    )
                                } else {
                                    format!(
                                        "第 {} 条记录（共 {} 条）",
                                        current_page_render.read(cx),
                                        total_count_render.read(cx)
                                    )
                                }
                            }),
                    )
                    // Query duration display
                    .child(
                        div()
                            .text_sm()
                            .text_color(cx.theme().muted_foreground)
                            .child(format!(
                                "查询耗时 {}ms",
                                query_duration_render.read(cx)
                            )),
                    )
                    // SQL statement display (truncated if too long)
                    .child(
                        div()
                            .text_sm()
                            .text_color(cx.theme().muted_foreground)
                            .flex_1()
                            .overflow_hidden()
                            .text_ellipsis()
                            .child(format!(
                                "SQL: {}",
                                current_sql_render.read(cx)
                            )),
                    )
                    // Pagination controls
                    .child(
                        h_flex()
                            .gap_1()
                            .items_center()
                            .child(
                                Button::new("prev-page")
                                    .with_size(Size::Small)
                                    .icon(IconName::ChevronLeft)
                                    .on_click({
                                        let this = self.clone();
                                        move |_, _, cx| this.handle_prev_page(cx)
                                    }),
                            )
                            .child({
                                let current_page_size = *self.page_size.read(cx);
                                let label = match current_page_size {
                                    0 => "全部".to_string(),
                                    n => format!("{}", n),
                                };
                                let this = self.clone();
                                Button::new("page-size-selector")
                                    .with_size(Size::Small)
                                    .label(label)
                                    .dropdown_menu_with_anchor(Corner::TopRight, move |menu, _, _| {
                                        let this500 = this.clone();
                                        let this1000 = this.clone();
                                        let this2000 = this.clone();
                                        let this_all = this.clone();
                                        menu.item(
                                            PopupMenuItem::new("500")
                                                .checked(current_page_size == 500)
                                                .on_click(move |_, _, cx| {
                                                    this500.handle_page_size_change(500, cx);
                                                })
                                        )
                                        .item(
                                            PopupMenuItem::new("1000")
                                                .checked(current_page_size == 1000)
                                                .on_click(move |_, _, cx| {
                                                    this1000.handle_page_size_change(1000, cx);
                                                })
                                        )
                                        .item(
                                            PopupMenuItem::new("2000")
                                                .checked(current_page_size == 2000)
                                                .on_click(move |_, _, cx| {
                                                    this2000.handle_page_size_change(2000, cx);
                                                })
                                        )
                                        .item(
                                            PopupMenuItem::new("全部")
                                                .checked(current_page_size == 0)
                                                .on_click(move |_, _, cx| {
                                                    this_all.handle_page_size_change(0, cx);
                                                })
                                        )
                                    })
                            })
                            .child(
                                Button::new("next-page")
                                    .with_size(Size::Small)
                                    .icon(IconName::ChevronRight)
                                    .on_click({
                                        let this = self.clone();
                                        move |_, _, cx| this.handle_next_page(cx)
                                    }),
                            ),
                    ),
            ).into_any_element()
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
            table: self.table.clone(),
            status_msg: self.status_msg.clone(),
            focus_handle: self.focus_handle.clone(),
            text_editor: self.text_editor.clone(),
            editing_large_text: self.editing_large_text.clone(),
            current_page: self.current_page.clone(),
            page_size: self.page_size.clone(),
            total_count: self.total_count.clone(),
            filter_editor: self.filter_editor.clone(),
            editor_visible: self.editor_visible.clone(),
            _table_subscription: None,
            query_duration: self.query_duration.clone(),
            current_sql: self.current_sql.clone(),
            _phantom: PhantomData,
        }
    }
}

// SAFETY: TableDataTabContent is safe to send across threads because:
// - All Entity<T> types are Send + Sync
// - The Subscription is only used for cleanup and doesn't affect thread safety
// - PhantomData is used to opt-out of auto Send/Sync
unsafe impl Send for TableDataTabContent {}
unsafe impl Sync for TableDataTabContent {}

impl Focusable for TableDataTabContent {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}


