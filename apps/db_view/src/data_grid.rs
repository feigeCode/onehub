use gpui::{div, px, AnyElement, App, AsyncApp, ClickEvent, Context, Entity, IntoElement, ParentElement, Styled, Subscription, Window};
use gpui::prelude::*;
use gpui_component::{
    button::{Button, ButtonVariants as _},
    h_flex,
    table::{Column, Table, TableEvent, TableState},
    v_flex,
    ActiveTheme as _, IconName, Sizable as _, Size, WindowExt,
};

use crate::multi_text_editor::{create_multi_text_editor_with_content, MultiTextEditor};
use crate::results_delegate::{EditorTableDelegate, RowChange};
use crate::sql_editor::SqlEditor;
use db::{ExecOptions, GlobalDbState, SqlResult, TableCellChange, TableRowChange, TableSaveRequest};

// ============================================================================
// DataGrid - 可复用的数据表格组件
// ============================================================================

/// 数据表格使用场景
#[derive(Clone, Debug)]
pub enum DataGridUsage {
    /// 在表格数据页签中使用（编辑器高度较低）
    TableData,
    /// 在SQL结果页签中使用（编辑器高度较高）
    SqlResult,
}

/// 数据表格配置
#[derive(Clone)]
pub struct DataGridConfig {
    pub database_name: String,
    pub table_name: String,
    pub connection_id: String,
    pub database_type: one_core::storage::DatabaseType,
    pub editable: bool,
    pub show_toolbar: bool,
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

/// 编辑器状态 - 合并 editing_large_text 和 editor_visible
#[derive(Clone, Default)]
pub struct EditorState {
    /// 当前编辑的单元格位置，None 表示编辑器不可见
    editing_cell: Option<(usize, usize)>,
}

impl EditorState {
    fn is_visible(&self) -> bool {
        self.editing_cell.is_some()
    }
}

/// 数据表格组件
pub struct DataGrid {
    config: DataGridConfig,
    pub(crate) table: Entity<TableState<EditorTableDelegate>>,
    text_editor: Entity<MultiTextEditor>,
    editor_state: Entity<EditorState>,
    _table_sub: Option<Subscription>,
}

impl DataGrid {
    pub fn new(config: DataGridConfig, window: &mut Window, cx: &mut Context<Self>) -> Self {
        let table = cx.new(|cx| {
            TableState::new(EditorTableDelegate::new(vec![], vec![], window, cx), window, cx)
        });
        let editor_state = cx.new(|_| EditorState::default());
        let text_editor = create_multi_text_editor_with_content(None, window, cx);

        let mut result = Self {
            config,
            table,
            text_editor,
            editor_state,
            _table_sub: None,
        };
        result.bind_table_event(window, cx);
        result
    }

    fn bind_table_event(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let sub = cx.subscribe_in(&self.table, window, |this, _, evt: &TableEvent, window, cx| {
            if let TableEvent::SelectCell(row, col) = evt {
                let state = this.editor_state.read(cx);
                if let Some((old_row, old_col)) = state.editing_cell {
                    if *row != old_row || *col != old_col {
                        this.switch_editing_cell(*row, *col, window, cx);
                    }
                }
            }
        });
        self._table_sub = Some(sub);
    }

    // ========== 公共访问器 ==========

    pub fn table(&self) -> &Entity<TableState<EditorTableDelegate>> {
        &self.table
    }


    pub fn editor_visible(&self) -> &Entity<EditorState> {
        &self.editor_state
    }

    pub fn editing_large_text(&self) -> &Entity<EditorState> {
        &self.editor_state
    }


    pub fn update_data(
        &self,
        columns: Vec<Column>,
        rows: Vec<Vec<String>>,
        pk_columns: Vec<usize>,
        cx: &mut App,
    ) {
        self.table.update(cx, |state, cx| {
            state.delegate_mut().update_data(columns, rows, cx);
            state.delegate_mut().set_primary_keys(pk_columns);
            state.refresh(cx);
        });
    }

    // ========== 编辑器操作 ==========

    /// 保存当前编辑器内容到单元格
    fn save_editor_content(&self, cx: &mut App) {
        let state = self.editor_state.read(cx);
        let Some((row_ix, col_ix)) = state.editing_cell else {
            return;
        };

        let content = self.text_editor.read(cx).get_active_text(cx);
        self.table.update(cx, |state, cx| {
            let delegate = state.delegate_mut();
            if let Some(row) = delegate.rows.get_mut(row_ix) {
                if let Some(cell) = row.get_mut(col_ix - 1) {
                    if *cell != content {
                        *cell = content;
                        delegate.modified_cells.insert((row_ix, col_ix - 1));
                    }
                }
            }
            state.refresh(cx);
        });
    }

    /// 加载单元格内容到编辑器
    fn load_cell_to_editor(&self, row_ix: usize, col_ix: usize, window: &mut Window, cx: &mut App) {
        let value = self
            .table
            .read(cx)
            .delegate()
            .rows
            .get(row_ix)
            .and_then(|r| r.get(col_ix - 1))
            .cloned()
            .unwrap_or_default();

        self.text_editor.update(cx, |editor, cx| {
            editor.set_active_text(value, window, cx);
        });

        self.editor_state.update(cx, |state, cx| {
            state.editing_cell = Some((row_ix, col_ix));
            cx.notify();
        });
    }

    /// 切换编辑单元格（保存旧内容，加载新内容）
    fn switch_editing_cell(&self, row_ix: usize, col_ix: usize, window: &mut Window, cx: &mut App) {
        if !self.editor_state.read(cx).is_visible() {
            return;
        }
        self.save_editor_content(cx);
        self.load_cell_to_editor(row_ix, col_ix, window, cx);
    }

    /// 切换编辑器显示状态
    fn toggle_editor(&self, window: &mut Window, cx: &mut App) {
        let is_visible = self.editor_state.read(cx).is_visible();
        let text_editor = self.text_editor.clone();
        if is_visible {
            let clone_self = self.clone();
            window.open_dialog(cx, move |dialog, _window, cx| {
                let clone_self = clone_self.clone();
                dialog
                    .title("编辑单元格")
                    .w(px(800.0))
                    .h(px(600.0))
                    .child(text_editor.clone())
                    .confirm()
                    .on_ok(|_, _, cx| {
                        clone_self.clone().save_editor_content(cx);
                        true
                    })
                    .on_cancel(|_, _, _| {
                        true
                    })
            });


            // 关闭前保存内容

            self.editor_state.update(cx, |state, cx| {
                state.editing_cell = None;
                cx.notify();
            });
        } else {
            // 打开编辑器，加载当前选中单元格
            let (row_ix, col_ix) = {
                let table = self.table.read(cx);
                table
                    .editing_cell()
                    .or_else(|| table.selected_cell())
                    .unwrap_or((0, 1))
            };
            self.load_cell_to_editor(row_ix, col_ix, window, cx);
        }
    }

    pub fn handle_cell_selection(
        &self,
        row_ix: usize,
        col_ix: usize,
        window: &mut Window,
        cx: &mut App,
    ) {
        self.switch_editing_cell(row_ix, col_ix, window, cx);
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

    pub fn clear_changes(&self, cx: &mut App) {
        self.table.update(cx, |state, cx| {
            state.delegate_mut().clear_changes();
            cx.notify();
        });
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
        let table_changes = Self::convert_row_changes(changes, &column_names);

        if table_changes.is_empty() {
            return None;
        }

        Some(TableSaveRequest {
            database: self.config.database_name.clone(),
            table: self.config.table_name.clone(),
            column_names,
            primary_key_indices: pk_columns,
            changes: table_changes,
        })
    }

    fn handle_save_changes(&self, _: &ClickEvent, window: &mut Window, cx: &mut App) {
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

        window.push_notification(format!("Saving {} changes...", change_count), cx);
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

    pub fn generate_changes_sql(&self, cx: &mut App) -> String {
        let Some(save_request) = self.create_save_request(cx) else {
            return "-- 没有变更数据".to_string();
        };

        match self.build_changes_sql(&save_request, cx) {
            Ok(sql) => sql,
            Err(message) => format!("-- {}", message),
        }
    }

    pub fn show_sql_preview(&self, window: &mut Window, cx: &mut App) {
        let Some(save_request) = self.create_save_request(cx) else {
            notification(cx, "没有变更数据".to_string());
            return;
        };

        let sql_content = match self.build_changes_sql(&save_request, cx) {
            Ok(sql) => sql,
            Err(message) => {
                notification(cx, message);
                return;
            }
        };

        let sql_editor = cx.new(|cx| SqlEditor::new(window, cx));
        sql_editor.update(cx, |editor, cx| {
            editor.set_value(sql_content.clone(), window, cx);
        });

        let global_state = cx.global::<GlobalDbState>().clone();
        let connection_id = self.config.connection_id.clone();
        let database_name = self.config.database_name.clone();
        let this = self.clone();

        window.open_dialog(cx, move |dialog, _window, cx| {
            let editor = sql_editor.clone();
            let execute_state = global_state.clone();
            let execute_connection = connection_id.clone();
            let execute_database = database_name.clone();
            let data_grid = this.clone();

            dialog
                .title("变更SQL预览")
                .w(px(800.0))
                .h(px(600.0))
                .child(
                    v_flex()
                        .w_full()
                        .h_full()
                        .child(editor.clone()),
                )
                .footer(move |ok, cancel, window, cx| {
                    let execute_editor = editor.clone();
                    let execute_state = execute_state.clone();
                    let execute_connection = execute_connection.clone();
                    let execute_database = execute_database.clone();
                    let data_grid = data_grid.clone();

                    let mut buttons = Vec::new();
                    buttons.push(cancel(window, cx));
                    buttons.push(
                        Button::new("execute-preview-sql")
                            .with_size(Size::Medium)
                            .primary()
                            .icon(IconName::ArrowRight)
                            .label("执行SQL")
                            .on_click(move |_, window, cx| {
                                let sql_text = execute_editor.read(cx).get_text_from_app(cx);
                                if sql_text.trim().is_empty() {
                                    window.push_notification("SQL内容为空", cx);
                                    return;
                                }
                                window.push_notification("Executing SQL...", cx);
                                let execute_state = execute_state.clone();
                                let execute_connection = execute_connection.clone();
                                let execute_database = execute_database.clone();
                                let data_grid = data_grid.clone();
                                let sql_to_run = sql_text.clone();

                                cx.spawn(async move |cx: &mut AsyncApp| {
                                    let exec_options = ExecOptions {
                                        stop_on_error: true,
                                        transactional: true,
                                        max_rows: None,
                                    };

                                    let result = execute_state
                                        .execute_script(
                                            cx,
                                            execute_connection.clone(),
                                            sql_to_run.clone(),
                                            Some(execute_database.clone()),
                                            Some(exec_options),
                                        )
                                        .await;

                                    cx.update(|cx| match result {
                                        Ok(results) => {
                                            if let Some(err_msg) = results.iter().find_map(|res| match res {
                                                SqlResult::Error(err) => Some(err.message.clone()),
                                                _ => None,
                                            }) {
                                                notification(cx, format!("执行失败: {}", err_msg));
                                            } else {
                                                data_grid.clear_changes(cx);
                                                notification(cx, "SQL执行成功".to_string());
                                            }
                                        }
                                        Err(e) => {
                                            notification(cx, format!("执行失败: {}", e));
                                        }
                                    })
                                    .ok();
                                })
                                .detach();
                            })
                            .into_any_element(),
                    );
                    buttons.push(ok(window, cx));
                    buttons
                })
        });
    }

    // ========== 渲染 ==========

    pub fn render_toolbar<F>(&self, on_refresh: F, _window: &mut Window, cx: &App) -> AnyElement
    where
        F: Fn(&mut App) + Clone + 'static,
    {
        let table = self.table.clone();
        let this_for_sql = self.clone();
        let this_for_editor = self.clone();
        let on_save = self.clone();

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
            .child(
                Button::new("add-row")
                    .with_size(Size::Medium)
                    .icon(IconName::Plus)
                    .tooltip("添加行")
                    .on_click({
                        let table = table.clone();
                        move |_, w, cx| table.update(cx, |state, cx| state.add_row(w, cx))
                    }),
            )
            .child(
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
            .child(
                Button::new("undo-changes")
                    .with_size(Size::Medium)
                    .icon(IconName::Undo)
                    .tooltip("撤销")
                    .on_click({
                        let table = table.clone();
                        move |_, _, cx| {
                            table.update(cx, |state, cx| {
                                state.delegate_mut().clear_changes();
                                cx.notify();
                            })
                        }
                    }),
            )
            .child(
                Button::new("sql-preview")
                    .with_size(Size::Medium)
                    .icon(IconName::Eye)
                    .tooltip("SQL预览")
                    .on_click(move |_, w, cx| this_for_sql.show_sql_preview(w, cx)),
            )
            .child(
                Button::new("commit-changes")
                    .with_size(Size::Medium)
                    .icon(IconName::ArrowUp)
                    .tooltip("提交更改")
                    .on_click(move |c, window, cx| on_save.handle_save_changes(c, window, cx)),
            )
            .child(div().w(px(1.0)).h(px(20.0)).bg(cx.theme().border).mx_2())
            .child(
                Button::new("chart-view")
                    .with_size(Size::Medium)
                    .icon(IconName::ChartPie)
                    .tooltip("图表"),
            )
            .child(div().flex_1())
            .child({
                let is_visible = self.editor_state.read(cx).is_visible();
                let btn = Button::new("toggle-editor")
                    .with_size(Size::Medium)
                    .icon(IconName::EditBorder)
                    .tooltip("编辑器");

                let btn = if is_visible { btn.primary() } else { btn };
                btn.on_click(move |_, w, cx| this_for_editor.toggle_editor(w, cx))
            })
            .into_any_element()
    }

    pub fn render_table_area(&self, _window: &mut Window, cx: &App) -> AnyElement {
        let table_view = Table::new(&self.table)
            .stripe(false)
            .bordered(true);
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
            text_editor: self.text_editor.clone(),
            editor_state: self.editor_state.clone(),
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
