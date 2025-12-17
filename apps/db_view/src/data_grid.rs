use gpui::prelude::*;
use gpui::{div, px, AnyElement, App, AsyncApp, ClickEvent, Context, Entity, IntoElement, ParentElement, SharedString, Styled, Subscription, Window};
use tracing::log::trace;
use gpui_component::{
    button::{Button, ButtonVariants as _},
    h_flex,
    table::{Column, Table, TableEvent, TableState},
    v_flex,
    ActiveTheme as _, IconName, Sizable as _, Size, WindowExt,
};

use crate::multi_text_editor::create_multi_text_editor_with_content;
use crate::results_delegate::{EditorTableDelegate, RowChange};
use crate::sql_editor::SqlEditor;
use db::{ExecOptions, GlobalDbState, SqlResult, TableCellChange, TableRowChange, TableSaveRequest};
use gpui_component::dialog::DialogButtonProps;
// ============================================================================
// DataGrid - 可复用的数据表格组件
// ============================================================================

/// 数据表格使用场景
///
/// 定义了数据表格在不同场景下的使用模式，主要用于调整UI布局和交互行为。
#[derive(Clone, Debug, PartialEq)]
pub enum DataGridUsage {
    /// 在表格数据页签中使用（编辑器高度较低）
    ///
    /// 此模式下表格通常占据较小的空间，适合浏览和编辑少量数据。
    TableData,
    /// 在SQL结果页签中使用（编辑器高度较高）
    ///
    /// 此模式下表格通常占据更多空间，适合查看SQL查询结果。
    SqlResult,
}

/// 数据表格配置
///
/// 包含数据表格所需的各种配置信息，如数据库连接、表名、是否可编辑等。
#[derive(Clone, Debug, PartialEq)]
pub struct DataGridConfig {
    /// 数据库名称
    pub database_name: String,
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
            table_name: table_name.into(),
            connection_id: connection_id.into(),
            database_type,
            editable: true,
            show_toolbar: true,
            usage: DataGridUsage::TableData, // 默认为表格数据场景
        }
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
}

/// 数据表格组件
///
/// 提供一个可编辑的数据表格界面，支持增删改查操作，并能生成相应的SQL语句。
pub struct DataGrid {
    /// 组件配置
    config: DataGridConfig,
    /// 内部表格状态
    pub(crate) table: Entity<TableState<EditorTableDelegate>>,
    /// 表格事件订阅
    _table_sub: Option<Subscription>,
}

impl DataGrid {
    pub fn new(config: DataGridConfig, window: &mut Window, cx: &mut Context<Self>) -> Self {
        let editable = config.editable;
        let table = cx.new(|cx| {
            TableState::new(EditorTableDelegate::new(vec![], vec![], editable, window, cx), window, cx)
        });
        let mut result = Self {
            config,
            table,
            _table_sub: None,
        };
        result.bind_table_event(window, cx);
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

    // ========== 公共访问器 ==========

    pub fn table(&self) -> &Entity<TableState<EditorTableDelegate>> {
        &self.table
    }

    pub fn update_data(
        &self,
        columns: Vec<Column>,
        rows: Vec<Vec<String>>,
        pk_columns: Vec<usize>,
        uk_columns: Vec<usize>,
        cx: &mut App,
    ) {
        self.table.update(cx, |state, cx| {
            state.delegate_mut().update_data(columns, rows, cx);
            state.delegate_mut().set_primary_keys(pk_columns);
            state.delegate_mut().set_unique_keys(uk_columns);
            state.refresh(cx);
        });
    }

    /// 显示大文本编辑器对话框 - 只使用普通文本编辑器
    fn show_large_text_editor(&self, window: &mut Window, cx: &mut App) {
        // 如果没有正在编辑的单元格，使用当前选中的单元格
        let table = self.table.read(cx);
        let cell = table.selected_cell();
        if cell.is_none() {
            window.push_notification("请选择一个单元格".to_string(), cx);
            return;
        }
        let (row_ix, col_ix) = cell.unwrap();
        let delegate = table.delegate();
        let Some(actual_row_ix) = delegate.resolve_display_row(row_ix) else {
            return;
        };
        // 获取当前单元格内容
        let current_content =  delegate
            .rows
            .get(actual_row_ix)
            .and_then(|r| r.get(col_ix - 1))
            .cloned()
            .unwrap_or_default();

        // 获取列名用于标题
        let column_name = self.table.read(cx)
            .delegate()
            .columns
            .get(col_ix.saturating_sub(1))
            .map(|col| col.name.to_string())
            .unwrap_or_else(|| format!("列 {}", col_ix));
        let title = format!("编辑单元格 - {} (行 {})", column_name, row_ix + 1);

        self.show_text_editor_dialog(current_content, &title, row_ix, col_ix, window, cx);
    }

    /// 显示文本编辑器对话框
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

    pub fn primary_key_columns(&self, cx: &App) -> Vec<usize> {
        self.table.read(cx).delegate().primary_key_columns().to_vec()
    }

    pub fn unique_key_columns(&self, cx: &App) -> Vec<usize> {
        self.table.read(cx).delegate().unique_key_columns().to_vec()
    }

    pub fn clear_changes(&self, cx: &mut App) {
        self.table.update(cx, |state, cx| {
            state.delegate_mut().clear_changes();
            cx.notify();
        });
    }

    /// 撤销所有更改并恢复到原始状态
    pub fn revert_changes(&self, cx: &mut App) {
        self.table.update(cx, |state, cx| {
            state.delegate_mut().revert_all_changes();
            state.refresh(cx);
            cx.notify();
        });
    }

    /// 检查是否有未保存的更改
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

    pub fn create_save_request(&self, cx: &App) -> Option<TableSaveRequest> {
        let changes = self.get_changes(cx);
        if changes.is_empty() {
            return None;
        }

        let column_names = self.column_names(cx);
        let pk_columns = self.primary_key_columns(cx);
        let uk_columns = self.unique_key_columns(cx);
        let table_changes = Self::convert_row_changes(changes, &column_names);

        if table_changes.is_empty() {
            return None;
        }

        Some(TableSaveRequest {
            database: self.config.database_name.clone(),
            table: self.config.table_name.clone(),
            column_names,
            primary_key_indices: pk_columns,
            unique_key_indices: uk_columns,
            changes: table_changes,
        })
    }

    fn handle_save_changes(&self, _: &ClickEvent, _window: &mut Window, cx: &mut App) {
        let Some(save_request) = self.create_save_request(cx) else {
            return;
        };
        let change_count = save_request.changes.len();
        let sql_content = match self.build_changes_sql(&save_request, cx) {
            Ok(sql) => sql,
            Err(message) => {
                notification(cx, message);
                return;
            }
        };
        let global_state = cx.global::<GlobalDbState>().clone();
        let connection_id = self.config.connection_id.clone();
        let database_name = self.config.database_name.clone();
        let this = self.clone();

        cx.spawn(async move |cx: &mut AsyncApp| {
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
        let Some(save_request) = self.create_save_request(cx) else {
            window.push_notification("没有变更数据".to_string(),cx);
            return;
        };

        let sql_content = match self.build_changes_sql(&save_request, cx) {
            Ok(sql) => sql,
            Err(message) => {
                window.push_notification(message,cx);
                return;
            }
        };

        self.show_sql_editor_dialog(sql_content, "变更SQL预览", window, cx);
    }

    /// 显示 SQL 编辑器对话框，支持编辑和执行
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
                    false // 保持对话框打开，等待异步任务完成后手动关闭
                })
        });
    }

    /// 执行 SQL 并刷新数据网格
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
                                window.close_dialog(cx); // 成功后关闭对话框
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
                                // 失败时保持对话框打开，让用户看到错误并修改 SQL
                            });
                        }
                    }).ok();
                }
            }
        }).detach();
    }

    // ========== 渲染 ==========

    pub fn render_toolbar<F>(&self, on_refresh: F, _window: &mut Window, cx: &App) -> AnyElement
    where
        F: Fn(&mut App) + Clone + 'static,
    {
        let table = self.table.clone();
        let this_for_sql = self.clone();
        let this_for_editor = self.clone();
        let this_for_undo = self.clone();
        let on_save = self.clone();
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
                    .on_click({
                        let on_refresh = on_refresh.clone();
                        move |_, _, cx| on_refresh(cx)
                    }),
            )
            // 添加行按钮 - 只在可编辑时显示
            .when(editable, |this| {
                this.child(
                    Button::new("add-row")
                        .with_size(Size::Medium)
                        .icon(IconName::Plus)
                        .tooltip("添加行")
                        .on_click({
                            let table = table.clone();
                            move |_, w, cx| table.update(cx, |state, cx| state.add_row(w, cx))
                        }),
                )
            })
            // 删除行按钮 - 只在可编辑时显示
            .when(editable, |this| {
                this.child(
                    Button::new("delete-row")
                        .with_size(Size::Medium)
                        .icon(IconName::Minus)
                        .tooltip("删除行")
                        .on_click({
                            let table = table.clone();
                            move |_, w, cx| {
                                table.update(cx, |state, cx| {
                                    if let Some(row_ix) = state.selected_row() {
                                        state.delete_row(row_ix, w, cx);
                                    }
                                })
                            }
                        }),
                )
            })
            // 撤销按钮 - 只在可编辑时显示
            .when(editable, |this| {
                this.child(
                    Button::new("undo-changes")
                        .with_size(Size::Medium)
                        .icon(IconName::Undo)
                        .tooltip("撤销")
                        .on_click(move |_, _, cx| this_for_undo.clone().revert_changes(cx)),
                )
            })
            // SQL预览按钮 - 只在可编辑时显示
            .when(editable, |this| {
                this.child(
                    Button::new("sql-preview")
                        .with_size(Size::Medium)
                        .icon(IconName::Eye)
                        .tooltip("SQL预览")
                        .on_click(move |_, w, cx| this_for_sql.clone().show_sql_preview(w, cx)),
                )
            })
            // 提交更改按钮 - 只在可编辑时显示
            .when(editable, |this| {
                this.child(
                    Button::new("commit-changes")
                        .with_size(Size::Medium)
                        .icon(IconName::ArrowUp)
                        .tooltip("提交更改")
                        .on_click(move |c, window, cx| on_save.clone().handle_save_changes(c, window, cx)),
                )
            })
            .child(div().flex_1())
            .child({
                Button::new("toggle-editor")
                    .with_size(Size::Medium)
                    .icon(IconName::EditBorder)
                    .tooltip("大文本编辑器")
                    .on_click(move |_, w, cx| this_for_editor.clone().show_large_text_editor(w, cx))
            })
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
}

#[inline]
pub fn notification(cx: &mut App, error: String){
    if let Some(window) = cx.active_window() {
        _ = window.update(cx, |_, w, cx| {
            w.push_notification(error, cx)
        });
    };
}

impl Clone for DataGrid {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            table: self.table.clone(),
            _table_sub: None,
        }
    }
}

impl DataGrid {
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
}
