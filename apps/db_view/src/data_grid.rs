use gpui::prelude::*;
use gpui::{actions, div, px, AnyElement, App, AsyncApp, ClickEvent, Context, Corner, Entity, FocusHandle, Focusable, IntoElement, ParentElement, SharedString, Styled, Subscription, Window};
use tracing::log::trace;
use gpui_component::{
    button::Button,
    h_flex,
    table::{Column, Table, TableEvent, TableState},
    v_flex,
    ActiveTheme as _, IconName, Sizable as _, Size, WindowExt,
};

use crate::multi_text_editor::create_multi_text_editor_with_content;
use crate::results_delegate::{EditorTableDelegate, RowChange};
use crate::sql_editor::SqlEditor;
use crate::filter_editor::{ColumnSchema, FilterEditorEvent, TableFilterEditor, TableSchema};
use db::{ExecOptions, GlobalDbState, SqlResult, TableCellChange, TableRowChange, TableSaveRequest, TableDataRequest};
use gpui_component::dialog::DialogButtonProps;
use gpui_component::menu::DropdownMenu;

actions!(data_grid, [Page500, Page1000, Page2000, PageAll]);

/// 数据表格使用场景
#[derive(Clone, Debug, PartialEq)]
pub enum DataGridUsage {
    /// 在表格数据页签中使用（编辑器高度较低）
    TableData,
    /// 在SQL结果页签中使用（编辑器高度较高）
    SqlResult,
}

/// 数据表格配置
#[derive(Clone, Debug, PartialEq)]
pub struct DataGridConfig {
    /// 数据库名称
    pub database_name: String,
    /// Schema名称（用于支持schema的数据库如PostgreSQL、MSSQL）
    pub schema_name: Option<String>,
    /// 表名称
    pub table_name: String,
    /// 数据库连接ID
    pub connection_id: String,
    /// 数据库类型
    pub database_type: one_core::storage::DatabaseType,
    /// 是否允许编辑
    pub editable: bool,
    /// 是否显示工具栏
    pub show_toolbar: bool,
    /// 使用场景
    pub usage: DataGridUsage,
    /// 原始 SQL（SqlResult 场景使用）
    pub sql: Option<String>,
}

impl DataGridConfig {
    pub fn new(
        database_name: impl Into<String>,
        table_name: impl Into<String>,
        connection_id: impl Into<String>,
        database_type: one_core::storage::DatabaseType,
    ) -> Self {
        Self {
            database_name: database_name.into(),
            schema_name: None,
            table_name: table_name.into(),
            connection_id: connection_id.into(),
            database_type,
            editable: true,
            show_toolbar: true,
            usage: DataGridUsage::TableData,
            sql: None,
        }
    }

    pub fn with_schema(mut self, schema: impl Into<String>) -> Self {
        self.schema_name = Some(schema.into());
        self
    }

    pub fn editable(mut self, editable: bool) -> Self {
        self.editable = editable;
        self
    }

    pub fn show_toolbar(mut self, show: bool) -> Self {
        self.show_toolbar = show;
        self
    }

    pub fn usage(mut self, usage: DataGridUsage) -> Self {
        self.usage = usage;
        self
    }

    pub fn sql(mut self, sql: impl Into<String>) -> Self {
        self.sql = Some(sql.into());
        self
    }
}

#[derive(Clone)]
pub struct TableDataInfo {
    pub current_page: usize,
    pub page_size: usize,
    pub total_count: usize,
    pub duration: u128,
    pub current_sql: String,
}

impl Default for TableDataInfo {
    fn default() -> Self {
        Self {
            current_page: 1,
            page_size: 500,
            total_count: 0,
            duration: 0,
            current_sql: String::new(),
        }
    }
}

/// 数据表格组件
pub struct DataGrid {
    /// 组件配置
    config: DataGridConfig,
    /// 内部表格状态
    pub(crate) table: Entity<TableState<EditorTableDelegate>>,
    /// 表格事件订阅
    _table_sub: Option<Subscription>,
    /// 焦点句柄
    focus_handle: FocusHandle,
    /// 表格数据信息（分页、总数等）
    table_data_info: Entity<TableDataInfo>,
    /// 过滤器编辑器
    filter_editor: Entity<TableFilterEditor>,
    /// 过滤器事件订阅
    _filter_sub: Option<Subscription>,
}

impl DataGrid {
    pub fn new(config: DataGridConfig, window: &mut Window, cx: &mut Context<Self>) -> Self {
        let editable = config.editable;
        let is_table_data = config.usage == DataGridUsage::TableData;
        let table = cx.new(|cx| {
            TableState::new(EditorTableDelegate::new(vec![], vec![], editable, window, cx), window, cx)
        });
        let focus_handle = cx.focus_handle();
        let filter_editor = cx.new(|cx| TableFilterEditor::new(window, cx));
        let table_data_info = cx.new(|_| TableDataInfo::default());

        let mut result = Self {
            config,
            table,
            _table_sub: None,
            focus_handle,
            table_data_info,
            filter_editor,
            _filter_sub: None,
        };
        result.bind_table_event(window, cx);
        result.bind_filter_event(window, cx);
        if is_table_data {
            result.load_data_with_clauses(1, cx);
        }
        result
    }

    fn bind_table_event(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let sub = cx.subscribe_in(&self.table, window, |_this, _, evt: &TableEvent, _window, _cx| {
            if let TableEvent::SelectCell(row, col) = evt {
               trace!("select cell: {:?}", (row, col))
            }
        });
        self._table_sub = Some(sub);
    }

    fn bind_filter_event(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let sub = cx.subscribe_in(&self.filter_editor, window, |this: &mut DataGrid, _, evt: &FilterEditorEvent, _, cx| {
            match evt {
                FilterEditorEvent::QueryApply => {
                    match this.config.usage {
                        DataGridUsage::TableData => {
                            this.load_data_with_clauses(1, cx);
                        }
                        DataGridUsage::SqlResult => {
                            if let Some(sql) = &this.config.sql {
                                this.load_data_with_sql_filtered(sql.clone(), cx);
                            }
                        }
                    }
                    cx.notify()
                },
            }
        });
        self._filter_sub = Some(sub);
    }

    // ========== 公共访问器 ==========

    pub fn table(&self) -> &Entity<TableState<EditorTableDelegate>> {
        &self.table
    }

    pub fn update_data(
        &self,
        columns: Vec<Column>,
        rows: Vec<Vec<String>>,
        cx: &mut App,
    ) {
        self.table.update(cx, |state, cx| {
            state.delegate_mut().update_data(columns, rows, cx);
            state.refresh(cx);
        });
    }

    pub fn set_filter_schema(&self, column_names: Vec<String>, cx: &mut App) {
        let column_schemas: Vec<ColumnSchema> = column_names
            .iter()
            .map(|col| ColumnSchema {
                name: col.clone(),
                data_type: String::new(),
                is_nullable: true,
            })
            .collect();

        self.filter_editor.update(cx, |editor, cx| {
            editor.set_schema(
                TableSchema {
                    table_name: self.config.table_name.clone(),
                    columns: column_schemas,
                },
                cx,
            );
        });
    }

    // ========== 数据加载 ==========

    fn load_data_with_clauses(&self, page: usize, cx: &mut App) {
        let global_state = cx.global::<GlobalDbState>().clone();
        let connection_id = self.config.connection_id.clone();
        let table_name = self.config.table_name.clone();
        let database_name = self.config.database_name.clone();
        let schema_name = self.config.schema_name.clone();
        let table = self.table.clone();
        let table_data_info = self.table_data_info.clone();
        let where_clause = self.filter_editor.read(cx).get_where_clause(cx);
        let order_by_clause = self.filter_editor.read(cx).get_order_by_clause(cx);
        let filter_editor = self.filter_editor.clone();
        let page_size = self.table_data_info.read(cx).page_size;

        tracing::info!("load_data_with_clauses: connection_id={}, database={}, table={}",
            connection_id, database_name, table_name);

        cx.spawn(async move |cx: &mut AsyncApp| {
            let table_name_for_schema = table_name.clone();

            let mut request = if page_size == 0 {
                TableDataRequest::new(&database_name, &table_name)
                    .with_where_clause(where_clause.clone())
                    .with_order_by_clause(order_by_clause.clone())
            } else {
                TableDataRequest::new(&database_name, &table_name)
                    .with_page(page, page_size)
                    .with_where_clause(where_clause.clone())
                    .with_order_by_clause(order_by_clause.clone())
            };
            if let Some(schema) = schema_name {
                request = request.with_schema(schema);
            }

            let result = global_state.query_table_data(cx, connection_id.clone(), request).await;
            match result {
                Err(err) => {
                    tracing::error!("load_data_with_clauses failed for connection_id={}: {}", connection_id, err);
                    cx.update(|cx| {
                        notification(cx, format!("Failed to get connection:{}", err));
                    }).ok();
                }
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

                    cx.update(|cx| {
                        table_data_info.update(cx, |info, cx| {
                            info.total_count = response.total_count;
                            info.current_sql = response.executed_sql;
                            info.duration = response.duration;
                            info.current_page = response.page;
                            cx.notify();
                        });
                    }).ok();

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
                        filter_editor.update(cx, |editor, cx| {
                            editor.set_schema(TableSchema {
                                table_name: table_name_for_schema.clone(),
                                columns: column_schemas,
                            }, cx);
                        });

                        table.update(cx, |state, cx| {
                            state.delegate_mut().update_data(columns, rows, cx);
                            state.refresh(cx);
                        });
                    }).ok();
                }
            }
        }).detach();
    }

    fn handle_refresh(&self, cx: &mut App) {
        match self.config.usage {
            DataGridUsage::TableData => {
                let page = self.table_data_info.read(cx).current_page;
                self.load_data_with_clauses(page, cx);
            }
            DataGridUsage::SqlResult => {
                if let Some(sql) = &self.config.sql {
                    self.load_data_with_sql(sql.clone(), cx);
                }
            }
        }
    }

    fn load_data_with_sql(&self, sql: String, cx: &mut App) {
        let global_state = cx.global::<GlobalDbState>().clone();
        let connection_id = self.config.connection_id.clone();
        let database_name = self.config.database_name.clone();
        let table = self.table.clone();

        cx.spawn(async move |cx: &mut AsyncApp| {
            let result = global_state
                .execute_script(cx, connection_id.clone(), sql.clone(), Some(database_name.clone()), None)
                .await;

            match result {
                Err(err) => {
                    cx.update(|cx| {
                        notification(cx, format!("Failed to execute SQL: {}", err));
                    }).ok();
                }
                Ok(results) => {
                    for result in results {
                        if let SqlResult::Query(query_result) = result {
                            let columns: Vec<Column> = query_result.columns.iter()
                                .map(|col| Column::new(col.clone(), col.clone()))
                                .collect();

                            let rows: Vec<Vec<String>> = query_result.rows.iter()
                                .map(|row| {
                                    row.iter()
                                        .map(|cell| cell.clone().unwrap_or_else(|| "NULL".to_string()))
                                        .collect()
                                })
                                .collect();

                            cx.update(|cx| {
                                table.update(cx, |state, cx| {
                                    state.delegate_mut().update_data(columns, rows, cx);
                                    state.refresh(cx);
                                });
                            }).ok();
                            break;
                        }
                    }
                }
            }
        }).detach();
    }

    fn load_data_with_sql_filtered(&self, base_sql: String, cx: &mut App) {
        let global_state = cx.global::<GlobalDbState>().clone();
        let connection_id = self.config.connection_id.clone();
        let database_name = self.config.database_name.clone();
        let table = self.table.clone();
        let filter_editor = self.filter_editor.clone();

        let where_clause = self.filter_editor.read(cx).get_where_clause(cx);
        let order_by_clause = self.filter_editor.read(cx).get_order_by_clause(cx);

        let sql = if where_clause.is_empty() && order_by_clause.is_empty() {
            base_sql
        } else {
            let base_sql_trimmed = base_sql.trim().trim_end_matches(';');
            let mut sql = format!("SELECT * FROM ({}) AS _subquery", base_sql_trimmed);
            if !where_clause.is_empty() {
                sql.push_str(&format!(" WHERE {}", where_clause));
            }
            if !order_by_clause.is_empty() {
                sql.push_str(&format!(" ORDER BY {}", order_by_clause));
            }
            sql
        };

        cx.spawn(async move |cx: &mut AsyncApp| {
            let result = global_state
                .execute_script(cx, connection_id.clone(), sql.clone(), Some(database_name.clone()), None)
                .await;

            match result {
                Err(err) => {
                    cx.update(|cx| {
                        notification(cx, format!("Failed to execute SQL: {}", err));
                    }).ok();
                }
                Ok(results) => {
                    for result in results {
                        if let SqlResult::Query(query_result) = result {
                            let columns: Vec<Column> = query_result.columns.iter()
                                .map(|col| Column::new(col.clone(), col.clone()))
                                .collect();

                            let rows: Vec<Vec<String>> = query_result.rows.iter()
                                .map(|row| {
                                    row.iter()
                                        .map(|cell| cell.clone().unwrap_or_else(|| "NULL".to_string()))
                                        .collect()
                                })
                                .collect();

                            let column_schemas: Vec<ColumnSchema> = query_result.columns.iter()
                                .map(|col| ColumnSchema {
                                    name: col.clone(),
                                    data_type: String::new(),
                                    is_nullable: true,
                                })
                                .collect();

                            cx.update(|cx| {
                                filter_editor.update(cx, |editor, cx| {
                                    editor.set_schema(TableSchema {
                                        table_name: "_subquery".to_string(),
                                        columns: column_schemas,
                                    }, cx);
                                });

                                table.update(cx, |state, cx| {
                                    state.delegate_mut().update_data(columns, rows, cx);
                                    state.refresh(cx);
                                });
                            }).ok();
                            break;
                        }
                    }
                }
            }
        }).detach();
    }

    fn handle_prev_page(&self, cx: &mut App) {
        let page = self.table_data_info.read(cx).current_page;
        if page > 1 {
            self.load_data_with_clauses(page - 1, cx);
        }
    }

    fn handle_next_page(&self, cx: &mut App) {
        let info = self.table_data_info.read(cx);
        let page = info.current_page;
        let total = info.total_count;
        let page_size = info.page_size;

        if page_size == 0 {
            return;
        }
        let total_pages = total.div_ceil(page_size);
        if page < total_pages {
            self.load_data_with_clauses(page + 1, cx);
        }
    }

    fn handle_page_size_change(&self, new_size: usize, cx: &mut App) {
        self.table_data_info.update(cx, |info, cx| {
            info.page_size = new_size;
            cx.notify();
        });
        self.load_data_with_clauses(1, cx);
    }

    fn handle_page_change_500(&mut self, _: &Page500, _: &mut Window, cx: &mut Context<Self>) {
        self.handle_page_size_change(500, cx)
    }

    fn handle_page_change_1000(&mut self, _: &Page1000, _: &mut Window, cx: &mut Context<Self>) {
        self.handle_page_size_change(1000, cx)
    }

    fn handle_page_change_2000(&mut self, _: &Page2000, _: &mut Window, cx: &mut Context<Self>) {
        self.handle_page_size_change(2000, cx)
    }

    fn handle_page_change_all(&mut self, _: &PageAll, _: &mut Window, cx: &mut Context<Self>) {
        self.handle_page_size_change(0, cx)
    }

    fn handle_add_row(&mut self, _: &ClickEvent, window: &mut Window, cx: &mut Context<Self>) {
        self.table.update(cx, |state, cx| state.add_row(window, cx));
    }

    fn handle_delete_row(&mut self, _: &ClickEvent, window: &mut Window, cx: &mut Context<Self>) {
        self.table.update(cx, |state, cx| {
            if let Some(row_ix) = state.selected_row() {
                state.delete_row(row_ix, window, cx);
            }
        });
    }

    fn handle_revert_changes(&mut self, _: &ClickEvent, _window: &mut Window, cx: &mut Context<Self>) {
        self.revert_changes(cx);
    }

    fn handle_sql_preview(&mut self, _: &ClickEvent, window: &mut Window, cx: &mut Context<Self>) {
        self.show_sql_preview(window, cx);
    }

    fn handle_commit_changes(&mut self, event: &ClickEvent, window: &mut Window, cx: &mut Context<Self>) {
        self.handle_save_changes(event, window, cx);
    }

    fn handle_large_text_editor(&mut self, _: &ClickEvent, window: &mut Window, cx: &mut Context<Self>) {
        self.show_large_text_editor(window, cx);
    }

    fn handle_toolbar_refresh(&mut self, _: &ClickEvent, _window: &mut Window, cx: &mut Context<Self>) {
        self.handle_refresh(cx);
    }

    fn handle_prev_page_click(&mut self, _: &ClickEvent, _window: &mut Window, cx: &mut Context<Self>) {
        self.handle_prev_page(cx);
    }

    fn handle_next_page_click(&mut self, _: &ClickEvent, _window: &mut Window, cx: &mut Context<Self>) {
        self.handle_next_page(cx);
    }

    // ========== 大文本编辑器 ==========

    fn show_large_text_editor(&self, window: &mut Window, cx: &mut App) {
        let table = self.table.read(cx);
        let Some((row_ix, col_ix)) = table.selected_cell() else {
            window.push_notification("请选择一个单元格".to_string(), cx);
            return;
        };

        let delegate = table.delegate();
        let Some(actual_row_ix) = delegate.resolve_display_row(row_ix) else {
            return;
        };
        let current_content = delegate
            .rows
            .get(actual_row_ix)
            .and_then(|r| r.get(col_ix - 1))
            .cloned()
            .unwrap_or_default();

        let column_name = self.table.read(cx)
            .delegate()
            .columns
            .get(col_ix.saturating_sub(1))
            .map(|col| col.name.to_string())
            .unwrap_or_else(|| format!("列 {}", col_ix));
        let title = format!("编辑单元格 - {} (行 {})", column_name, row_ix + 1);

        self.show_text_editor_dialog(current_content, &title, row_ix, col_ix, window, cx);
    }

    fn show_text_editor_dialog(
        &self,
        initial_text: String,
        title: &str,
        row_ix: usize,
        col_ix: usize,
        window: &mut Window,
        cx: &mut App,
    ) {
        let dialog_text_editor = create_multi_text_editor_with_content(Some(initial_text), window, cx);
        let data_grid = self.clone();
        let title = title.to_string();

        window.open_dialog(cx, move |dialog, _window, _cx| {
            let editor = dialog_text_editor.clone();
            let data_grid = data_grid.clone();

            dialog
                .title(SharedString::from(title.clone()))
                .w(px(800.0))
                .h(px(600.0))
                .child(
                    v_flex()
                        .w_full()
                        .h_full()
                        .child(editor.clone()),
                )
                .close_button(true)
                .overlay(false)
                .content_center()
                .footer(|ok, cancel, window, cx| {
                    vec![ok(window, cx), cancel(window, cx)]
                })
                .on_ok(move |_, window, cx| {
                    let content = editor.read(cx).get_active_text(cx);
                    return match content {
                        Ok(val) => {
                            data_grid.table.update(cx, |state, cx| {
                                let delegate = state.delegate_mut();
                                let Some(actual_row_ix) = delegate.resolve_display_row(row_ix) else {
                                    return false;
                                };

                                let col_index = col_ix.saturating_sub(1);
                                let changed = delegate.record_cell_change(actual_row_ix, col_index, val);

                                if changed {
                                    state.refresh(cx);
                                }

                                changed
                            });
                            true
                        }
                        Err(err) => {
                            window.push_notification(format!("错误: {}", err), cx);
                            false
                        }
                    }
                })
                .on_cancel(|_, _, _| true)
        });
    }

    // ========== 数据变更 ==========

    pub fn get_changes(&self, cx: &App) -> Vec<RowChange> {
        self.table.read(cx).delegate().get_changes()
    }

    pub fn column_names(&self, cx: &App) -> Vec<String> {
        self.table.read(cx).delegate().column_names()
    }

    pub fn clear_changes(&self, cx: &mut App) {
        self.table.update(cx, |state, cx| {
            state.delegate_mut().clear_changes();
            cx.notify();
        });
    }

    pub fn revert_changes(&self, cx: &mut App) {
        self.table.update(cx, |state, cx| {
            state.delegate_mut().revert_all_changes();
            state.refresh(cx);
            cx.notify();
        });
    }

    pub fn has_unsaved_changes(&self, cx: &App) -> bool {
        !self.get_changes(cx).is_empty()
    }

    pub fn convert_row_changes(
        changes: Vec<RowChange>,
        column_names: &[String],
    ) -> Vec<TableRowChange> {
        changes
            .into_iter()
            .filter_map(|change| match change {
                RowChange::Added { data } => Some(TableRowChange::Added { data }),
                RowChange::Updated {
                    original_data,
                    changes,
                } => {
                    let converted: Vec<TableCellChange> = changes
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

                    if converted.is_empty() {
                        None
                    } else {
                        Some(TableRowChange::Updated {
                            original_data,
                            changes: converted,
                        })
                    }
                }
                RowChange::Deleted { original_data } => {
                    Some(TableRowChange::Deleted { original_data })
                }
            })
            .collect()
    }

    pub fn create_save_request(&self, pk_columns: Vec<usize>, uk_columns: Vec<usize>, cx: &App) -> Option<TableSaveRequest> {
        let changes = self.get_changes(cx);
        if changes.is_empty() {
            return None;
        }

        let column_names = self.column_names(cx);
        let table_changes = Self::convert_row_changes(changes, &column_names);

        if table_changes.is_empty() {
            return None;
        }

        Some(TableSaveRequest {
            database: self.config.database_name.clone(),
            schema: self.config.schema_name.clone(),
            table: self.config.table_name.clone(),
            column_names,
            primary_key_indices: pk_columns,
            unique_key_indices: uk_columns,
            changes: table_changes,
        })
    }

    fn handle_save_changes(&self, _: &ClickEvent, _window: &mut Window, cx: &mut App) {
        let changes = self.get_changes(cx);
        if changes.is_empty() {
            return;
        }

        let global_state = cx.global::<GlobalDbState>().clone();
        let connection_id = self.config.connection_id.clone();
        let database_name = self.config.database_name.clone();
        let schema_name = self.config.schema_name.clone();
        let table_name = self.config.table_name.clone();
        let database_type = self.config.database_type;
        let this = self.clone();

        cx.spawn(async move |cx: &mut AsyncApp| {
            let mut request = TableDataRequest::new(&database_name, &table_name)
                .with_page(1, 1);
            if let Some(schema) = &schema_name {
                request = request.with_schema(schema.clone());
            }

            let key_result = global_state.query_table_data(cx, connection_id.clone(), request).await;
            let (pk_columns, uk_columns) = match key_result {
                Ok(response) => (response.primary_key_indices, response.unique_key_indices),
                Err(err) => {
                    cx.update(|cx| {
                        notification(cx, format!("Failed to get table keys: {}", err));
                    }).ok();
                    return;
                }
            };

            let save_result = cx.update(|cx| {
                let Some(save_request) = this.create_save_request(pk_columns, uk_columns, cx) else {
                    return Err("没有变更数据".to_string());
                };
                let change_count = save_request.changes.len();

                let global_state = cx.global::<GlobalDbState>().clone();
                match global_state.db_manager.get_plugin(&database_type) {
                    Ok(plugin) => {
                        let sql = plugin.generate_table_changes_sql(&save_request);
                        let trimmed = sql.trim();
                        if trimmed.is_empty() || trimmed == "-- 没有变更数据" {
                            Err("没有变更数据".to_string())
                        } else {
                            Ok((sql, change_count))
                        }
                    }
                    Err(_) => Err("无法获取数据库插件".to_string()),
                }
            });

            let (sql_content, change_count) = match save_result {
                Ok(Ok((sql, count))) => (sql, count),
                Ok(Err(msg)) => {
                    cx.update(|cx| notification(cx, msg)).ok();
                    return;
                }
                Err(_) => return,
            };

            let exec_options = ExecOptions {
                stop_on_error: true,
                transactional: true,
                max_rows: None,
            };

            let result = global_state
                .execute_script(
                    cx,
                    connection_id.clone(),
                    sql_content.clone(),
                    Some(database_name.clone()),
                    Some(exec_options),
                )
                .await;

            cx.update(|cx| match result {
                Ok(results) => {
                    if let Some(err_msg) = results.iter().find_map(|res| match res {
                        SqlResult::Error(err) => Some(err.message.clone()),
                        _ => None,
                    }) {
                        notification(cx, format!("Failed to save changes: {}", err_msg));
                    } else {
                        this.clear_changes(cx);
                        notification(cx, format!("Successfully saved {} changes", change_count));
                    }
                }
                Err(e) => {
                    notification(cx, format!("Failed to save changes: {}", e));
                }
            })
            .ok();
        })
        .detach();
    }

    pub fn show_sql_preview(&self, window: &mut Window, cx: &mut App) {
        let changes = self.get_changes(cx);
        if changes.is_empty() {
            window.push_notification("没有变更数据".to_string(), cx);
            return;
        }

        let global_state = cx.global::<GlobalDbState>().clone();
        let connection_id = self.config.connection_id.clone();
        let database_name = self.config.database_name.clone();
        let schema_name = self.config.schema_name.clone();
        let table_name = self.config.table_name.clone();
        let this = self.clone();

        cx.spawn(async move |cx: &mut AsyncApp| {
            let mut request = TableDataRequest::new(&database_name, &table_name)
                .with_page(1, 1);
            if let Some(schema) = &schema_name {
                request = request.with_schema(schema.clone());
            }

            let key_result = global_state.query_table_data(cx, connection_id.clone(), request).await;
            let (pk_columns, uk_columns) = match key_result {
                Ok(response) => (response.primary_key_indices, response.unique_key_indices),
                Err(err) => {
                    cx.update(|cx| {
                        notification(cx, format!("Failed to get table keys: {}", err));
                    }).ok();
                    return;
                }
            };

            cx.update(|cx| {
                if let Some(window_id) = cx.active_window() {
                    let _ = cx.update_window(window_id, |_entity, window, cx| {
                        let Some(save_request) = this.create_save_request(pk_columns.clone(), uk_columns.clone(), cx) else {
                            window.push_notification("没有变更数据".to_string(), cx);
                            return;
                        };

                        let sql_content = match this.build_changes_sql(&save_request, cx) {
                            Ok(sql) => sql,
                            Err(message) => {
                                window.push_notification(message, cx);
                                return;
                            }
                        };

                        this.show_sql_editor_dialog(sql_content, "变更SQL预览", window, cx);
                    });
                }
            }).ok();
        }).detach();
    }

    pub fn show_sql_editor_dialog(
        &self,
        initial_sql: String,
        title: &str,
        window: &mut Window,
        cx: &mut App,
    ) {
        let sql_editor = cx.new(|cx| SqlEditor::new(window, cx));
        sql_editor.update(cx, |editor, cx| {
            editor.set_value(initial_sql, window, cx);
        });

        let global_state = cx.global::<GlobalDbState>().clone();
        let connection_id = self.config.connection_id.clone();
        let database_name = self.config.database_name.clone();
        let this = self.clone();
        let title = title.to_string();

        window.open_dialog(cx, move |dialog, _window, _cx| {
            let editor = sql_editor.clone();
            let execute_state = global_state.clone();
            let execute_connection = connection_id.clone();
            let execute_database = database_name.clone();
            let data_grid = this.clone();

            dialog
                .title(SharedString::from(title.clone()))
                .w(px(800.0))
                .h(px(600.0))
                .child(
                    v_flex()
                        .w_full()
                        .h_full()
                        .child(editor.clone()),
                )
                .close_button(true)
                .overlay(false)
                .content_center()
                .button_props(DialogButtonProps::default().ok_text("执行SQL"))
                .footer(|ok, cancel, window, cx| {
                    vec![ok(window, cx), cancel(window, cx)]
                })
                .on_ok(move |_, window, cx| {
                    let sql_text = editor.read(cx).get_text_from_app(cx);
                    if sql_text.trim().is_empty() {
                        window.push_notification("SQL内容为空", cx);
                        return false;
                    }
                    data_grid.execute_sql_and_refresh(
                        sql_text,
                        execute_state.clone(),
                        execute_connection.clone(),
                        execute_database.clone(),
                        window,
                        cx,
                    );
                    false
                })
        });
    }

    async fn execute_sql_and_refresh_async(
        sql: String,
        global_state: GlobalDbState,
        connection_id: String,
        database_name: String,
        cx: &mut AsyncApp,
    ) -> Result<(), String> {
        let exec_options = ExecOptions {
            stop_on_error: true,
            transactional: true,
            max_rows: None,
        };

        let result = global_state
            .execute_script(
                cx,
                connection_id.clone(),
                sql.clone(),
                Some(database_name.clone()),
                Some(exec_options),
            )
            .await;

        match result {
            Ok(results) => {
                if let Some(err_msg) = results.iter().find_map(|res| match res {
                    SqlResult::Error(err) => Some(err.message.clone()),
                    _ => None,
                }) {
                    Err(format!("执行失败: {}", err_msg))
                } else {
                    Ok(())
                }
            }
            Err(e) => Err(format!("执行失败: {}", e)),
        }
    }

    fn execute_sql_and_refresh(
        &self,
        sql: String,
        global_state: GlobalDbState,
        connection_id: String,
        database_name: String,
        _window: &mut Window,
        cx: &mut App,
    ) {
        let data_grid = self.clone();

        cx.spawn(async move |cx: &mut AsyncApp| {
            match Self::execute_sql_and_refresh_async(
                sql,
                global_state,
                connection_id,
                database_name,
                cx,
            ).await {
                Ok(_) => {
                    cx.update(|cx| {
                        if let Some(window_id) = cx.active_window() {
                            let _ = cx.update_window(window_id, |_entity, window, cx| {
                                data_grid.clear_changes(cx);
                                window.close_dialog(cx);
                                window.push_notification("执行成功".to_string(), cx);
                            });
                        }
                    }).ok();
                }
                Err(error_msg) => {
                    cx.update(|cx| {
                        if let Some(window_id) = cx.active_window() {
                            let _ = cx.update_window(window_id, |_entity, window, cx| {
                                window.push_notification(error_msg, cx);
                            });
                        }
                    }).ok();
                }
            }
        }).detach();
    }



    fn build_changes_sql(&self, request: &TableSaveRequest, cx: &App) -> Result<String, String> {
        let global_state = cx.global::<GlobalDbState>().clone();
        match global_state.db_manager.get_plugin(&self.config.database_type) {
            Ok(plugin) => {
                let sql = plugin.generate_table_changes_sql(request);
                let trimmed = sql.trim();
                if trimmed.is_empty() || trimmed == "-- 没有变更数据" {
                    Err("没有变更数据".to_string())
                } else {
                    Ok(sql)
                }
            }
            Err(_) => Err("无法获取数据库插件".to_string()),
        }
    }

    // ========== 渲染辅助方法 ==========

    pub fn render_toolbar(&self, _window: &mut Window, cx: &Context<Self>) -> AnyElement {
        let editable = self.config.editable;

        h_flex()
            .gap_1()
            .items_center()
            .px_2()
            .py_1()
            .border_b_1()
            .border_color(cx.theme().border)
            .bg(cx.theme().background)
            .child(
                Button::new("refresh-data")
                    .with_size(Size::Medium)
                    .icon(IconName::Refresh)
                    .tooltip("刷新")
                    .on_click(cx.listener(Self::handle_toolbar_refresh)),
            )
            .when(editable, |this| {
                this.child(
                    Button::new("add-row")
                        .with_size(Size::Medium)
                        .icon(IconName::Plus)
                        .tooltip("添加行")
                        .on_click(cx.listener(Self::handle_add_row)),
                )
            })
            .when(editable, |this| {
                this.child(
                    Button::new("delete-row")
                        .with_size(Size::Medium)
                        .icon(IconName::Minus)
                        .tooltip("删除行")
                        .on_click(cx.listener(Self::handle_delete_row)),
                )
            })
            .when(editable, |this| {
                this.child(
                    Button::new("undo-changes")
                        .with_size(Size::Medium)
                        .icon(IconName::Undo)
                        .tooltip("撤销")
                        .on_click(cx.listener(Self::handle_revert_changes)),
                )
            })
            .when(editable, |this| {
                this.child(
                    Button::new("sql-preview")
                        .with_size(Size::Medium)
                        .icon(IconName::Eye)
                        .tooltip("SQL预览")
                        .on_click(cx.listener(Self::handle_sql_preview)),
                )
            })
            .when(editable, |this| {
                this.child(
                    Button::new("commit-changes")
                        .with_size(Size::Medium)
                        .icon(IconName::ArrowUp)
                        .tooltip("提交更改")
                        .on_click(cx.listener(Self::handle_commit_changes)),
                )
            })
            .child(div().flex_1())
            .child(
                Button::new("toggle-editor")
                    .with_size(Size::Medium)
                    .icon(IconName::EditBorder)
                    .tooltip("大文本编辑器")
                    .on_click(cx.listener(Self::handle_large_text_editor)),
            )
            .into_any_element()
    }

    pub fn render_table_area(&self, _window: &mut Window, cx: &App) -> AnyElement {
        let table_view = Table::new(&self.table);
        div()
            .flex_1()
            .w_full()
            .h_full()
            .bg(cx.theme().background)
            .border_1()
            .border_color(cx.theme().border)
            .child(table_view)
            .into_any_element()
    }

    fn render_status_bar(&self, cx: &Context<Self>) -> AnyElement {
        let table_data_info = self.table_data_info.read(cx);
        let table = self.table.read(cx);

        let filtered_count = table.delegate().filtered_row_count();
        let total_rows = table.delegate().rows.len();
        let current_page_size = table_data_info.page_size;

        h_flex()
            .gap_3()
            .items_center()
            .px_2()
            .py_1()
            .border_t_1()
            .border_color(cx.theme().border)
            .bg(cx.theme().background)
            .child(
                div()
                    .text_sm()
                    .text_color(cx.theme().foreground)
                    .child({
                        if filtered_count < total_rows {
                            format!(
                                "显示 {} 条（共 {} 条，总计 {} 条）",
                                filtered_count,
                                total_rows,
                                table_data_info.total_count
                            )
                        } else {
                            format!(
                                "第 {} 页（共 {} 条）",
                                table_data_info.current_page,
                                table_data_info.total_count
                            )
                        }
                    }),
            )
            .child(
                div()
                    .text_sm()
                    .text_color(cx.theme().muted_foreground)
                    .child(format!("查询耗时 {}ms", table_data_info.duration)),
            )
            .child(
                div()
                    .text_sm()
                    .text_color(cx.theme().muted_foreground)
                    .flex_1()
                    .overflow_hidden()
                    .text_ellipsis()
                    .child(table_data_info.current_sql.clone()),
            )
            .child(
                h_flex()
                    .gap_1()
                    .items_center()
                    .child(
                        Button::new("prev-page")
                            .with_size(Size::Small)
                            .icon(IconName::ChevronLeft)
                            .on_click(cx.listener(Self::handle_prev_page_click)),
                    )
                    .child({
                        let label = match current_page_size {
                            0 => "全部".to_string(),
                            n => format!("{}", n),
                        };

                        Button::new("page-size-selector")
                            .with_size(Size::Small)
                            .label(label)
                            .dropdown_menu_with_anchor(Corner::TopRight, move |menu, _, _| {
                                menu.menu("500", Box::new(Page500))
                                    .menu("1000", Box::new(Page1000))
                                    .menu("2000", Box::new(Page2000))
                                    .menu("全部", Box::new(PageAll))
                            })
                    })
                    .child(
                        Button::new("next-page")
                            .with_size(Size::Small)
                            .icon(IconName::ChevronRight)
                            .on_click(cx.listener(Self::handle_next_page_click)),
                    ),
            )
            .into_any_element()
    }

    fn render_simple_status_bar(&self, cx: &App) -> AnyElement {
        let table = self.table.read(cx);
        let row_count = table.delegate().rows.len();

        h_flex()
            .gap_3()
            .items_center()
            .px_2()
            .py_1()
            .border_t_1()
            .border_color(cx.theme().border)
            .bg(cx.theme().background)
            .child(
                div()
                    .text_sm()
                    .text_color(cx.theme().foreground)
                    .child(format!("共 {} 条记录", row_count)),
            )
            .into_any_element()
    }
}

impl Render for DataGrid {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let is_table_data = self.config.usage == DataGridUsage::TableData;

        v_flex()
            .when(is_table_data, |this| {
                this.on_action(cx.listener(Self::handle_page_change_500))
                    .on_action(cx.listener(Self::handle_page_change_1000))
                    .on_action(cx.listener(Self::handle_page_change_2000))
                    .on_action(cx.listener(Self::handle_page_change_all))
            })
            .size_full()
            .gap_0()
            .child(self.render_toolbar(window, cx))
            .child(
                h_flex()
                    .items_center()
                    .w_full()
                    .px_2()
                    .py_1()
                    .child(self.filter_editor.clone()),
            )
            .child(
                div()
                    .flex_1()
                    .w_full()
                    .overflow_hidden()
                    .child(self.render_table_area(window, cx))
            )
            .child(if is_table_data {
                self.render_status_bar(cx)
            } else {
                self.render_simple_status_bar(cx)
            })
    }
}

impl Clone for DataGrid {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            table: self.table.clone(),
            _table_sub: None,
            focus_handle: self.focus_handle.clone(),
            table_data_info: self.table_data_info.clone(),
            filter_editor: self.filter_editor.clone(),
            _filter_sub: None,
        }
    }
}

impl Focusable for DataGrid {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

#[inline]
pub fn notification(cx: &mut App, error: String) {
    if let Some(window) = cx.active_window() {
        _ = window.update(cx, |_, w, cx| {
            w.push_notification(error, cx)
        });
    };
}
