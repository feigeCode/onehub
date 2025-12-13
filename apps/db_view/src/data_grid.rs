use gpui::{div, px, AnyElement, App, AppContext, Context, Entity, IntoElement, ParentElement, Pixels, Render, SharedString, Styled, Subscription, Window};
use gpui_component::{
    button::{Button, ButtonVariants as _},
    h_flex
    ,
    resizable::{resizable_panel, v_resizable},
    table::{Column, Table, TableState},
    ActiveTheme as _, IconName, Sizable as _, Size, WindowExt,
};

use crate::multi_text_editor::{create_multi_text_editor_with_content, MultiTextEditor};
use crate::results_delegate::{EditorTableDelegate, RowChange};
use db::{TableCellChange, TableRowChange, TableSaveRequest};
use gpui_component::table::TableEvent;
// ============================================================================
// DataGrid - 可复用的数据表格组件
// ============================================================================

/// 数据表格配置
#[derive(Clone)]
pub struct DataGridConfig {
    /// 数据库名称
    pub database_name: String,
    /// 表名称
    pub table_name: String,
    /// 连接ID
    pub connection_id: String,
    /// 数据库类型
    pub database_type: one_core::storage::DatabaseType,
    /// 是否可编辑
    pub editable: bool,
    /// 是否显示工具栏
    pub show_toolbar: bool
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
            show_toolbar: true
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

}

/// 数据表格组件
pub struct DataGrid {
    config: DataGridConfig,
    pub(crate) table: Entity<TableState<EditorTableDelegate>>,
    status_msg: Entity<String>,
    /// Text editor for large text editing
    text_editor: Entity<MultiTextEditor>,
    /// Currently editing cell position
    editing_large_text: Entity<Option<(usize, usize)>>,
    /// Editor visibility state
    editor_visible: Entity<bool>,

    _table_sub: Option<Subscription>
}

impl DataGrid {
    pub fn new(config: DataGridConfig, window: &mut Window, cx: &mut Context<Self>) -> Self {
        let table = cx.new(|cx| {
            TableState::new(EditorTableDelegate::new(vec![], vec![], window, cx), window, cx)
        });

        let status_msg = cx.new(|_| "Ready".to_string());
        let editing_large_text = cx.new(|_| None);
        let editor_visible = cx.new(|_| false);
        let text_editor = create_multi_text_editor_with_content(None, window, cx);

        let mut result = Self {
            config,
            table,
            status_msg,
            text_editor,
            editing_large_text,
            editor_visible,
            _table_sub: None,
        };

        result.bind_table_event(window, cx);

        result
    }

    pub fn bind_table_event(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let _sub = cx.subscribe_in(&self.table, window, |this, _,evt:& TableEvent , window, cx| {
            match evt {
                TableEvent::SelectCell(row, col) => {
                    let is_editor_visible = *this.editor_visible().read(cx);
                    if is_editor_visible {
                        let last_editing_pos = *this.editing_large_text().read(cx);

                        if let Some((row_ix, col_ix)) = last_editing_pos {

                            if *row != row_ix || col_ix != *col {
                                this.handle_cell_selection(*row, *col, window, cx);
                            }

                        }
                    }

                }
                _ => {}
            }
        });
        self._table_sub = Some(_sub);
    }

    /// 获取表格状态
    pub fn table(&self) -> &Entity<TableState<EditorTableDelegate>> {
        &self.table
    }

    /// 获取状态消息
    pub fn status_msg(&self) -> &Entity<String> {
        &self.status_msg
    }


    /// 获取编辑器可见状态
    pub fn editor_visible(&self) -> &Entity<bool> {
        &self.editor_visible
    }

    /// 获取当前编辑位置
    pub fn editing_large_text(&self) -> &Entity<Option<(usize, usize)>> {
        &self.editing_large_text
    }

    /// 更新状态消息
    pub fn update_status(&self, message: String, cx: &mut App) {
        self.status_msg.update(cx, |s, cx| {
            *s = message;
            cx.notify();
        });
    }

    /// 更新数据
    pub fn update_data(
        &self,
        columns: Vec<Column>,
        rows: Vec<Vec<String>>,
        pk_columns: Vec<usize>,
        cx: &mut App,
    ) {
        eprintln!("DataGrid::update_data called with {} columns, {} rows", columns.len(), rows.len());
        self.table.update(cx, |state, cx| {
            eprintln!("Before update_data: delegate has {} columns, {} rows", 
                state.delegate().columns.len(), state.delegate().rows.len());
            state.delegate_mut().update_data(columns, rows, cx);
            eprintln!("After update_data: delegate has {} columns, {} rows", 
                state.delegate().columns.len(), state.delegate().rows.len());
            state.delegate_mut().set_primary_keys(pk_columns);
            state.refresh(cx);
            eprintln!("Called state.refresh()");
        });
    }
    /// 获取变更数据
    pub fn get_changes(&self, cx: &App) -> Vec<RowChange> {
        self.table.read(cx).delegate().get_changes()
    }

    /// 获取列名
    pub fn column_names(&self, cx: &App) -> Vec<String> {
        self.table.read(cx).delegate().column_names()
    }

    /// 获取主键列
    pub fn primary_key_columns(&self, cx: &App) -> Vec<usize> {
        self.table.read(cx).delegate().primary_key_columns().to_vec()
    }

    /// 清除变更
    pub fn clear_changes(&self, cx: &mut App) {
        self.table.update(cx, |state, cx| {
            state.delegate_mut().clear_changes();
            cx.notify();
        });
    }

    /// 转换行变更为表变更
    pub fn convert_row_changes(changes: Vec<RowChange>, column_names: &[String]) -> Vec<TableRowChange> {
        changes
            .into_iter()
            .filter_map(|change| match change {
                RowChange::Added { data } => Some(TableRowChange::Added { data }),
                RowChange::Updated {
                    original_data,
                    changes,
                } => {
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
                RowChange::Deleted { original_data } => {
                    Some(TableRowChange::Deleted { original_data })
                }
            })
            .collect()
    }

    /// 创建保存请求
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

    /// 生成变更SQL
    pub fn generate_changes_sql(&self, cx: &mut App) -> String {
        let save_request = match self.create_save_request(cx) {
            Some(req) => req,
            None => return "-- 没有变更数据".to_string(),
        };

        let global_state = cx.global::<db::GlobalDbState>().clone();

        if let Ok(plugin) = global_state.db_manager.get_plugin(&self.config.database_type) {
            plugin.generate_table_changes_sql(&save_request)
        } else {
            "-- 无法获取数据库插件".to_string()
        }
    }

    /// 显示SQL预览
    pub fn show_sql_preview(&self, window: &mut Window, cx: &mut App) {
        let sql_content = self.generate_changes_sql(cx);
        let sql_shared: SharedString = sql_content.into();

        window.open_dialog(cx, move |dialog, _window, cx| {
            dialog
                .title("变更SQL预览")
                .w(px(800.0))
                .h(px(600.0))
                .child(
                    div()
                        .w_full()
                        .h_full()
                        .p_4()
                        .bg(cx.theme().background)
                        .border_1()
                        .border_color(cx.theme().border)
                        .rounded_lg()
                        .overflow_hidden()
                        .child(
                            div()
                                .text_sm()
                                .text_color(cx.theme().foreground)
                                .child(sql_shared.clone()),
                        ),
                )
                .confirm()
                .on_ok(|_, _, _| true)
        });
    }

    fn load_cell_to_editor(&self, window: &mut Window, cx: &mut App) {
        let table = self.table.read(cx);
        let selected_row = table.selected_cell();
        let editing_cell = table.editing_cell();

        let (row_ix, col_ix) = if let Some((r, c)) = editing_cell {
            (r, c)
        } else if let Some((r, c)) = selected_row {
            (r, c)
        } else {
            self.update_status("Please select a cell first".to_string(), cx);
            return;
        };

        let value = table
            .delegate()
            .rows
            .get(row_ix)
            .and_then(|r| r.get(col_ix - 1))
            .cloned()
            .unwrap_or_default();

        self.text_editor.update(cx, |editor, cx| {
            editor.set_active_text(value, window, cx);
        });

        self.editing_large_text.update(cx, |pos, cx| {
            *pos = Some((row_ix, col_ix));
            cx.notify();
        });
    }

    pub fn handle_cell_selection(&self, row_ix: usize, col_ix: usize, window: &mut Window, cx: &mut App) {
        let is_visible = *self.editor_visible.read(cx);
        if !is_visible {
            return;
        }

        let old_pos = *self.editing_large_text.read(cx);
        if let Some((old_row, old_col)) = old_pos {
            let editor_content = self.text_editor.read(cx).get_active_text(cx);

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

        self.editing_large_text.update(cx, |pos, cx| {
            *pos = Some((row_ix, col_ix));
            cx.notify();
        });
    }

    fn toggle_editor(&self, window: &mut Window, cx: &mut App) {
        let is_visible = *self.editor_visible.read(cx);

        if is_visible {
            self.editor_visible.update(cx, |visible, cx| {
                *visible = false;
                cx.notify();
            });

            self.editing_large_text.update(cx, |pos, cx| {
                *pos = None;
                cx.notify();
            });
        } else {
            self.load_cell_to_editor(window, cx);

            self.editor_visible.update(cx, |visible, cx| {
                *visible = true;
                cx.notify();
            });
        }
    }


    /// 渲染工具栏
    pub fn render_toolbar<F, G>(
        &self,
        on_refresh: F,
        on_save: G,
        _window: &mut Window,
        cx: & App,
    ) -> AnyElement
    where
        F: Fn(&mut App) + Clone + 'static,
        G: Fn(&mut App) + Clone + 'static,
    {
        let table = self.table.clone();
        let this_for_sql = self.clone();
        let this_for_editor = self.clone();
        let on_refresh_clone = on_refresh.clone();
        let on_save_clone = on_save.clone();

        h_flex()
            .gap_1()
            .items_center()
            .px_2()
            .py_1()
            .border_b_1()
            .border_color(cx.theme().border)
            .bg(cx.theme().background)
            // 刷新按钮
            .child(
                Button::new("refresh-data")
                    .with_size(Size::Medium)
                    .icon(IconName::Refresh)
                    .tooltip("刷新")
                    .on_click(move |_, _, cx| {
                        on_refresh_clone(cx);
                    }),
            )
            // 添加按钮
            .child(
                Button::new("add-row")
                    .with_size(Size::Medium)
                    .icon(IconName::Plus)
                    .tooltip("添加行")
                    .on_click({
                        let table = table.clone();
                        move |_, w, cx| {
                            table.update(cx, |state, cx| {
                                state.add_row(w, cx);
                            });
                        }
                    }),
            )
            // 删除按钮
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
                            });
                        }
                    }),
            )
            // 撤销按钮
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
                            });
                        }
                    }),
            )
            // SQL预览按钮
            .child(
                Button::new("sql-preview")
                    .with_size(Size::Medium)
                    .icon(IconName::Eye)
                    .tooltip("SQL预览")
                    .on_click(move |_, w, cx| {
                        this_for_sql.show_sql_preview(w, cx);
                    }),
            )
            // 提交更改按钮
            .child(
                Button::new("commit-changes")
                    .with_size(Size::Medium)
                    .icon(IconName::ArrowUp)
                    .tooltip("提交更改")
                    .on_click(move |_, _, cx| {
                        on_save_clone(cx);
                    }),
            )
            // 分隔线
            .child(div().w(px(1.0)).h(px(20.0)).bg(cx.theme().border).mx_2())
            // 图表按钮
            .child(
                Button::new("chart-view")
                    .with_size(Size::Medium)
                    .icon(IconName::ChartPie)
                    .tooltip("图表"),
            )
            // 弹性空间
            .child(div().flex_1())
            // 编辑器切换按钮
            .child({
                let is_editor_visible = *self.editor_visible.read(cx);
                let mut btn = Button::new("toggle-editor")
                    .with_size(Size::Medium)
                    .icon(IconName::EditBorder)
                    .tooltip("编辑器");

                if is_editor_visible {
                    btn = btn.primary();
                }

                btn.on_click(move |_, w, cx| {
                    this_for_editor.toggle_editor(w, cx);
                })
            })
            .into_any_element()
    }

    /// 渲染表格区域（包含可选的编辑器）
    pub fn render_table_area(&self, _window: &mut Window, cx: & App) -> AnyElement {
        let is_editor_visible = *self.editor_visible.read(cx);

        if is_editor_visible {
            div()
                .flex_1()
                .w_full()
                .overflow_hidden()
                .child(
                    v_resizable("table-editor-split")
                        .child(
                            resizable_panel()
                                .size(px(300.))
                                .size_range(px(150.)..Pixels::MAX)
                                .child(
                                    div()
                                        .size_full()
                                        .bg(cx.theme().background)
                                        .border_1()
                                        .border_color(cx.theme().border)
                                        .overflow_hidden()
                                        .child(Table::new(&self.table).stripe(true).bordered(true)),
                                ),
                        )
                        .child(
                            resizable_panel()
                                .size(px(300.))
                                .size_range(px(150.)..Pixels::MAX)
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
                .into_any_element()
        } else {
            div()
                .flex_1()
                .w_full()
                .h_full()
                .bg(cx.theme().background)
                .border_1()
                .border_color(cx.theme().border)
                // .overflow_hidden()
                .child(Table::new(&self.table).stripe(true).bordered(true))
                .into_any_element()
        }
    }
}
//
// impl Render for DataGrid {
//     fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
//
//     }
// }

impl Clone for DataGrid {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            table: self.table.clone(),
            status_msg: self.status_msg.clone(),
            text_editor: self.text_editor.clone(),
            editing_large_text: self.editing_large_text.clone(),
            editor_visible: self.editor_visible.clone(),
            _table_sub: None,
        }
    }
}
