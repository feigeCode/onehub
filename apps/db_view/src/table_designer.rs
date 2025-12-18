use std::any::Any;

use gpui::prelude::*;
use gpui::{
    div, px, AnyElement, App, AsyncApp, Context, Entity, EventEmitter, FocusHandle, Focusable,
    IntoElement, InteractiveElement, MouseButton, ParentElement, Render, SharedString,
    StatefulInteractiveElement, Styled, Subscription, Window,
};
use gpui_component::{
    button::{Button, ButtonVariants},
    form::{field, h_form},
    h_flex,
    input::{Input, InputEvent, InputState},
    select::{Select, SelectItem, SelectState},
    tab::{Tab, TabBar},
    v_flex, ActiveTheme, IconName, IndexPath, Sizable, Size, StyledExt, WindowExt,
};

use db::plugin::DatabasePlugin;
use db::types::{
    CharsetInfo, CollationInfo, ColumnDefinition, DataTypeInfo, IndexDefinition, TableDesign,
    TableOptions,
};
use db::GlobalDbState;
use one_core::storage::DatabaseType;
use one_core::tab_container::{TabContent, TabContentType};

#[derive(Clone, Debug, PartialEq)]
pub enum DesignerTab {
    Columns,
    Indexes,
    Options,
    SqlPreview,
}

#[derive(Clone)]
pub struct TableDesignerConfig {
    pub connection_id: String,
    pub database_name: String,
    pub database_type: DatabaseType,
    pub table_name: Option<String>,
}

impl TableDesignerConfig {
    pub fn new(
        connection_id: impl Into<String>,
        database_name: impl Into<String>,
        database_type: DatabaseType,
    ) -> Self {
        Self {
            connection_id: connection_id.into(),
            database_name: database_name.into(),
            database_type,
            table_name: None,
        }
    }

    pub fn with_table_name(mut self, name: impl Into<String>) -> Self {
        self.table_name = Some(name.into());
        self
    }
}

pub struct TableDesigner {
    focus_handle: FocusHandle,
    config: TableDesignerConfig,
    active_tab: DesignerTab,
    table_name_input: Entity<InputState>,
    columns_editor: Entity<ColumnsEditor>,
    indexes_editor: Entity<IndexesEditor>,
    options_editor: Entity<TableOptionsEditor>,
    sql_preview_text: String,
    _subscriptions: Vec<Subscription>,
}

impl TableDesigner {
    pub fn new(config: TableDesignerConfig, window: &mut Window, cx: &mut Context<Self>) -> Self {
        let focus_handle = cx.focus_handle();

        let table_name_input = cx.new(|cx| {
            let mut input = InputState::new(window, cx).placeholder("输入表名");
            if let Some(name) = &config.table_name {
                input.set_value(name.clone(), window, cx);
            }
            input
        });

        let columns_editor = cx.new(|cx| ColumnsEditor::new(config.database_type.clone(), window, cx));
        let indexes_editor = cx.new(|cx| IndexesEditor::new(window, cx));
        let options_editor = cx.new(|cx| TableOptionsEditor::new(config.database_type.clone(), window, cx));

        let name_sub = cx.subscribe_in(&table_name_input, window, |this, _, event: &InputEvent, _window, cx| {
            if let InputEvent::Change = event {
                this.update_sql_preview(cx);
            }
        });

        let cols_sub = cx.subscribe(&columns_editor, |this, _, _: &ColumnsEditorEvent, cx| {
            this.update_sql_preview(cx);
        });

        let idx_sub = cx.subscribe(&indexes_editor, |this, _, _: &IndexesEditorEvent, cx| {
            this.update_sql_preview(cx);
        });

        let opts_sub = cx.subscribe(&options_editor, |this, _, _: &TableOptionsEvent, cx| {
            this.update_sql_preview(cx);
        });

        let mut designer = Self {
            focus_handle,
            config,
            active_tab: DesignerTab::Columns,
            table_name_input,
            columns_editor,
            indexes_editor,
            options_editor,
            sql_preview_text: String::new(),
            _subscriptions: vec![name_sub, cols_sub, idx_sub, opts_sub],
        };

        designer.update_sql_preview(cx);
        designer
    }

    fn collect_design(&self, cx: &App) -> TableDesign {
        let table_name = self.table_name_input.read(cx).text().to_string();
        let columns = self.columns_editor.read(cx).get_columns(cx);
        let indexes = self.indexes_editor.read(cx).get_indexes(cx);
        let options = self.options_editor.read(cx).get_options(cx);

        TableDesign {
            database_name: self.config.database_name.clone(),
            table_name,
            columns,
            indexes,
            foreign_keys: vec![],
            options,
        }
    }

    fn update_sql_preview(&mut self, cx: &mut Context<Self>) {
        let design = self.collect_design(cx);
        let global_state = cx.global::<GlobalDbState>().clone();

        if let Ok(plugin) = global_state.db_manager.get_plugin(&self.config.database_type) {
            self.sql_preview_text = plugin.build_create_table_sql(&design);
        } else {
            self.sql_preview_text = String::new();
        }
        cx.notify();
    }

    fn handle_execute(&mut self, _: &gpui::ClickEvent, window: &mut Window, cx: &mut Context<Self>) {
        let design = self.collect_design(cx);
        if design.table_name.is_empty() {
            window.push_notification("请输入表名", cx);
            return;
        }
        if design.columns.is_empty() {
            window.push_notification("请至少添加一列", cx);
            return;
        }

        let global_state = cx.global::<GlobalDbState>().clone();
        let connection_id = self.config.connection_id.clone();
        let database_name = self.config.database_name.clone();
        let database_type = self.config.database_type.clone();

        cx.spawn(async move |_this, cx: &mut AsyncApp| {
            let sql = {
                let plugin_result = cx.update(|cx: &mut App| {
                    let global_state = cx.global::<GlobalDbState>().clone();
                    global_state.db_manager.get_plugin(&database_type)
                });
                match plugin_result {
                    Ok(Ok(plugin)) => plugin.build_create_table_sql(&design),
                    _ => return,
                }
            };

            let result = global_state
                .execute_script(cx, connection_id, sql, Some(database_name), None)
                .await;

            let _ = cx.update(|cx: &mut App| {
                if let Some(window_id) = cx.active_window() {
                    let _ = cx.update_window(window_id, |_, window, cx| {
                        match &result {
                            Ok(_) => {
                                window.push_notification("表创建成功", cx);
                            }
                            Err(e) => {
                                window.push_notification(format!("创建表失败: {}", e), cx);
                            }
                        }
                    });
                }
            });
        }).detach();
    }

    fn render_toolbar(&self, cx: &Context<Self>) -> AnyElement {
        h_flex()
            .gap_2()
            .items_center()
            .px_2()
            .py_1()
            .border_b_1()
            .border_color(cx.theme().border)
            .bg(cx.theme().background)
            .child(
                field()
                    .label("表名")
                    .items_center()
                    .child(
                        Input::new(&self.table_name_input)
                            .w(px(200.))
                            .with_size(Size::Small)
                    )
            )
            .child(div().flex_1())
            .child(
                Button::new("execute")
                    .with_size(Size::Small)
                    .primary()
                    .label("创建表")
                    .on_click(cx.listener(Self::handle_execute))
            )
            .into_any_element()
    }

    fn render_tabs(&self, cx: &Context<Self>) -> AnyElement {
        let active_idx = match self.active_tab {
            DesignerTab::Columns => 0,
            DesignerTab::Indexes => 1,
            DesignerTab::Options => 2,
            DesignerTab::SqlPreview => 3,
        };

        TabBar::new("designer-tabs")
            .underline()
            .with_size(Size::Small)
            .selected_index(active_idx)
            .on_click(cx.listener(|this, ix: &usize, _window, cx| {
                this.active_tab = match ix {
                    0 => DesignerTab::Columns,
                    1 => DesignerTab::Indexes,
                    2 => DesignerTab::Options,
                    3 => DesignerTab::SqlPreview,
                    _ => DesignerTab::Columns,
                };
                cx.notify();
            }))
            .child(Tab::new().label("列"))
            .child(Tab::new().label("索引"))
            .child(Tab::new().label("选项"))
            .child(Tab::new().label("SQL 预览"))
            .into_any_element()
    }

    fn render_active_tab(&self, _window: &mut Window, cx: &Context<Self>) -> AnyElement {
        match self.active_tab {
            DesignerTab::Columns => self.columns_editor.clone().into_any_element(),
            DesignerTab::Indexes => self.indexes_editor.clone().into_any_element(),
            DesignerTab::Options => self.options_editor.clone().into_any_element(),
            DesignerTab::SqlPreview => self.render_sql_preview(cx),
        }
    }

    fn render_sql_preview(&self, cx: &Context<Self>) -> AnyElement {
        v_flex()
            .size_full()
            .p_4()
            .gap_2()
            .child(
                div()
                    .id("sql-preview-scroll")
                    .flex_1()
                    .overflow_y_scroll()
                    .p_3()
                    .rounded_md()
                    .border_1()
                    .border_color(cx.theme().border)
                    .bg(cx.theme().muted)
                    .text_sm()
                    .font_family("monospace")
                    .child(self.sql_preview_text.clone())
            )
            .into_any_element()
    }
}

impl Focusable for TableDesigner {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl Render for TableDesigner {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        v_flex()
            .size_full()
            .child(self.render_toolbar(cx))
            .child(self.render_tabs(cx))
            .child(
                div()
                    .flex_1()
                    .w_full()
                    .overflow_hidden()
                    .child(self.render_active_tab(window, cx))
            )
    }
}

// === Data Type Select Item ===

#[derive(Clone, Debug)]
pub struct DataTypeSelectItem {
    pub info: DataTypeInfo,
}

impl DataTypeSelectItem {
    pub fn new(info: DataTypeInfo) -> Self {
        Self { info }
    }
}

impl SelectItem for DataTypeSelectItem {
    type Value = String;

    fn title(&self) -> SharedString {
        self.info.name.clone().into()
    }

    fn value(&self) -> &Self::Value {
        &self.info.name
    }
}

// === Columns Editor ===

pub enum ColumnsEditorEvent {
    Changed,
}

pub struct ColumnsEditor {
    focus_handle: FocusHandle,
    columns: Vec<ColumnEditorRow>,
    selected_index: Option<usize>,
    data_types: Vec<DataTypeInfo>,
    _database_type: DatabaseType,
    _subscriptions: Vec<Subscription>,
}

struct ColumnEditorRow {
    name_input: Entity<InputState>,
    type_select: Entity<SelectState<Vec<DataTypeSelectItem>>>,
    length_input: Entity<InputState>,
    nullable: bool,
    is_pk: bool,
    auto_increment: bool,
    default_input: Entity<InputState>,
    comment_input: Entity<InputState>,
}

impl ColumnsEditor {
    pub fn new(database_type: DatabaseType, _window: &mut Window, cx: &mut Context<Self>) -> Self {
        let focus_handle = cx.focus_handle();
        let data_types = Self::get_data_types(&database_type, cx);

        Self {
            focus_handle,
            columns: vec![],
            selected_index: None,
            data_types,
            _database_type: database_type,
            _subscriptions: vec![],
        }
    }

    fn get_data_types(database_type: &DatabaseType, cx: &App) -> Vec<DataTypeInfo> {
        let global_state = cx.global::<GlobalDbState>();
        if let Ok(plugin) = global_state.db_manager.get_plugin(database_type) {
            plugin.get_data_types()
        } else {
            vec![
                DataTypeInfo::new("INT", "整数"),
                DataTypeInfo::new("VARCHAR", "变长字符串"),
                DataTypeInfo::new("TEXT", "长文本"),
                DataTypeInfo::new("DATE", "日期"),
                DataTypeInfo::new("DATETIME", "日期时间"),
            ]
        }
    }

    fn add_column(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let name_input = cx.new(|cx| InputState::new(window, cx).placeholder("列名"));
        let type_items: Vec<DataTypeSelectItem> = self.data_types
            .iter()
            .cloned()
            .map(DataTypeSelectItem::new)
            .collect();
        let type_select = cx.new(|cx| {
            SelectState::new(type_items, Some(IndexPath::new(0)), window, cx)
        });
        let length_input = cx.new(|cx| InputState::new(window, cx).placeholder("长度"));
        let default_input = cx.new(|cx| InputState::new(window, cx).placeholder("默认值"));
        let comment_input = cx.new(|cx| InputState::new(window, cx).placeholder("注释"));

        // Subscribe to input changes
        let name_sub = cx.subscribe_in(&name_input, window, |_this, _, event: &InputEvent, _window, cx| {
            if let InputEvent::Change = event {
                cx.emit(ColumnsEditorEvent::Changed);
            }
        });
        let length_sub = cx.subscribe_in(&length_input, window, |_this, _, event: &InputEvent, _window, cx| {
            if let InputEvent::Change = event {
                cx.emit(ColumnsEditorEvent::Changed);
            }
        });
        let default_sub = cx.subscribe_in(&default_input, window, |_this, _, event: &InputEvent, _window, cx| {
            if let InputEvent::Change = event {
                cx.emit(ColumnsEditorEvent::Changed);
            }
        });
        let comment_sub = cx.subscribe_in(&comment_input, window, |_this, _, event: &InputEvent, _window, cx| {
            if let InputEvent::Change = event {
                cx.emit(ColumnsEditorEvent::Changed);
            }
        });
        let type_sub = cx.observe(&type_select, |_this, _, cx| {
            cx.emit(ColumnsEditorEvent::Changed);
        });

        self._subscriptions.extend([name_sub, length_sub, default_sub, comment_sub, type_sub]);

        self.columns.push(ColumnEditorRow {
            name_input,
            type_select,
            length_input,
            nullable: true,
            is_pk: false,
            auto_increment: false,
            default_input,
            comment_input,
        });

        cx.emit(ColumnsEditorEvent::Changed);
        cx.notify();
    }

    fn remove_column(&mut self, cx: &mut Context<Self>) {
        if let Some(idx) = self.selected_index {
            if idx < self.columns.len() {
                self.columns.remove(idx);
                self.selected_index = None;
                cx.emit(ColumnsEditorEvent::Changed);
                cx.notify();
            }
        }
    }

    fn toggle_nullable(&mut self, idx: usize, cx: &mut Context<Self>) {
        if let Some(col) = self.columns.get_mut(idx) {
            col.nullable = !col.nullable;
            cx.emit(ColumnsEditorEvent::Changed);
            cx.notify();
        }
    }

    fn toggle_pk(&mut self, idx: usize, cx: &mut Context<Self>) {
        if let Some(col) = self.columns.get_mut(idx) {
            col.is_pk = !col.is_pk;
            cx.emit(ColumnsEditorEvent::Changed);
            cx.notify();
        }
    }

    fn toggle_auto_increment(&mut self, idx: usize, cx: &mut Context<Self>) {
        if let Some(col) = self.columns.get_mut(idx) {
            col.auto_increment = !col.auto_increment;
            cx.emit(ColumnsEditorEvent::Changed);
            cx.notify();
        }
    }

    pub fn get_columns(&self, cx: &App) -> Vec<ColumnDefinition> {
        self.columns.iter().map(|row| {
            let name = row.name_input.read(cx).text().to_string();
            let data_type = row.type_select.read(cx)
                .selected_value()
                .cloned()
                .unwrap_or_else(|| "VARCHAR".to_string());
            let length_str = row.length_input.read(cx).text().to_string();
            let length = length_str.parse::<u32>().ok();
            let default_value = {
                let val = row.default_input.read(cx).text().to_string();
                if val.is_empty() { None } else { Some(val) }
            };
            let comment = row.comment_input.read(cx).text().to_string();

            ColumnDefinition {
                name,
                data_type,
                length,
                precision: None,
                scale: None,
                is_nullable: row.nullable,
                is_primary_key: row.is_pk,
                is_auto_increment: row.auto_increment,
                is_unsigned: false,
                default_value,
                comment,
            }
        }).collect()
    }

    fn select_row(&mut self, idx: usize, cx: &mut Context<Self>) {
        self.selected_index = Some(idx);
        cx.notify();
    }

    fn render_header(&self, cx: &Context<Self>) -> AnyElement {
        h_flex()
            .gap_1()
            .items_center()
            .px_2()
            .py_1()
            .border_b_1()
            .border_color(cx.theme().border)
            .child(
                Button::new("add-col")
                    .with_size(Size::Small)
                    .icon(IconName::Plus)
                    .tooltip("添加列")
                    .on_click(cx.listener(|this, _, window, cx| this.add_column(window, cx)))
            )
            .child(
                Button::new("remove-col")
                    .with_size(Size::Small)
                    .icon(IconName::Minus)
                    .tooltip("删除列")
                    .on_click(cx.listener(|this, _, _window, cx| this.remove_column(cx)))
            )
            .into_any_element()
    }

    fn render_table_header(&self, cx: &Context<Self>) -> AnyElement {
        h_flex()
            .gap_3()
            .px_3()
            .py_2()
            .bg(cx.theme().muted)
            .border_b_1()
            .border_color(cx.theme().border)
            .child(div().w(px(150.)).text_sm().font_semibold().child("列名"))
            .child(div().w(px(140.)).text_sm().font_semibold().child("类型"))
            .child(div().w(px(80.)).text_sm().font_semibold().child("长度"))
            .child(div().w(px(60.)).text_sm().font_semibold().text_center().child("可空"))
            .child(div().w(px(60.)).text_sm().font_semibold().text_center().child("主键"))
            .child(div().w(px(60.)).text_sm().font_semibold().text_center().child("自增"))
            .child(div().w(px(120.)).text_sm().font_semibold().child("默认值"))
            .child(div().flex_1().text_sm().font_semibold().child("注释"))
            .into_any_element()
    }

    fn render_row(&self, idx: usize, row: &ColumnEditorRow, cx: &Context<Self>) -> AnyElement {
        let is_selected = self.selected_index == Some(idx);

        h_flex()
            .id(("col-row", idx))
            .gap_3()
            .px_3()
            .py_1()
            .when(is_selected, |this| this.bg(cx.theme().accent.opacity(0.3)))
            .border_b_1()
            .border_color(cx.theme().border)
            .on_mouse_down(MouseButton::Left, cx.listener(move |this, _, _window, cx| {
                this.select_row(idx, cx);
            }))
            .child(
                div().w(px(150.)).child(
                    Input::new(&row.name_input)
                        .w_full()
                        .with_size(Size::Small)
                )
            )
            .child(
                div().w(px(140.)).child(
                    Select::new(&row.type_select)
                        .w_full()
                        .with_size(Size::Small)
                )
            )
            .child(
                div().w(px(80.)).child(
                    Input::new(&row.length_input)
                        .w_full()
                        .with_size(Size::Small)
                )
            )
            .child(
                div().w(px(60.)).flex().justify_center().child(
                    Button::new(("null", idx))
                        .with_size(Size::Small)
                        .when(row.nullable, |btn| btn.primary())
                        .label(if row.nullable { "✓" } else { "" })
                        .on_click(cx.listener(move |this, _, _window, cx| this.toggle_nullable(idx, cx)))
                )
            )
            .child(
                div().w(px(60.)).flex().justify_center().child(
                    Button::new(("pk", idx))
                        .with_size(Size::Small)
                        .when(row.is_pk, |btn| btn.primary())
                        .label(if row.is_pk { "✓" } else { "" })
                        .on_click(cx.listener(move |this, _, _window, cx| this.toggle_pk(idx, cx)))
                )
            )
            .child(
                div().w(px(60.)).flex().justify_center().child(
                    Button::new(("ai", idx))
                        .with_size(Size::Small)
                        .when(row.auto_increment, |btn| btn.primary())
                        .label(if row.auto_increment { "✓" } else { "" })
                        .on_click(cx.listener(move |this, _, _window, cx| this.toggle_auto_increment(idx, cx)))
                )
            )
            .child(
                div().w(px(120.)).child(
                    Input::new(&row.default_input)
                        .w_full()
                        .with_size(Size::Small)
                )
            )
            .child(
                div().flex_1().child(
                    Input::new(&row.comment_input)
                        .w_full()
                        .with_size(Size::Small)
                )
            )
            .into_any_element()
    }
}

impl EventEmitter<ColumnsEditorEvent> for ColumnsEditor {}

impl Focusable for ColumnsEditor {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl Render for ColumnsEditor {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let rows: Vec<AnyElement> = self.columns
            .iter()
            .enumerate()
            .map(|(idx, row)| self.render_row(idx, row, cx))
            .collect();

        v_flex()
            .size_full()
            .child(self.render_header(cx))
            .child(self.render_table_header(cx))
            .child(
                v_flex()
                    .id("columns-scroll")
                    .flex_1()
                    .overflow_y_scroll()
                    .children(rows)
            )
    }
}

// === Indexes Editor ===

pub enum IndexesEditorEvent {
    Changed,
}

pub struct IndexesEditor {
    focus_handle: FocusHandle,
    indexes: Vec<IndexEditorRow>,
    selected_index: Option<usize>,
    _subscriptions: Vec<Subscription>,
}

struct IndexEditorRow {
    name_input: Entity<InputState>,
    columns_input: Entity<InputState>,
    is_unique: bool,
}

impl IndexesEditor {
    pub fn new(_window: &mut Window, cx: &mut Context<Self>) -> Self {
        let focus_handle = cx.focus_handle();
        Self {
            focus_handle,
            indexes: vec![],
            selected_index: None,
            _subscriptions: vec![],
        }
    }

    fn add_index(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let name_input = cx.new(|cx| InputState::new(window, cx).placeholder("索引名"));
        let columns_input = cx.new(|cx| InputState::new(window, cx).placeholder("列名(逗号分隔)"));

        // Subscribe to input changes
        let name_sub = cx.subscribe_in(&name_input, window, |_this, _, event: &InputEvent, _window, cx| {
            if let InputEvent::Change = event {
                cx.emit(IndexesEditorEvent::Changed);
            }
        });
        let columns_sub = cx.subscribe_in(&columns_input, window, |_this, _, event: &InputEvent, _window, cx| {
            if let InputEvent::Change = event {
                cx.emit(IndexesEditorEvent::Changed);
            }
        });

        self._subscriptions.extend([name_sub, columns_sub]);

        self.indexes.push(IndexEditorRow {
            name_input,
            columns_input,
            is_unique: false,
        });

        cx.emit(IndexesEditorEvent::Changed);
        cx.notify();
    }

    fn remove_index(&mut self, cx: &mut Context<Self>) {
        if let Some(idx) = self.selected_index {
            if idx < self.indexes.len() {
                self.indexes.remove(idx);
                self.selected_index = None;
                cx.emit(IndexesEditorEvent::Changed);
                cx.notify();
            }
        }
    }

    fn toggle_unique(&mut self, idx: usize, cx: &mut Context<Self>) {
        if let Some(index) = self.indexes.get_mut(idx) {
            index.is_unique = !index.is_unique;
            cx.emit(IndexesEditorEvent::Changed);
            cx.notify();
        }
    }

    fn select_row(&mut self, idx: usize, cx: &mut Context<Self>) {
        self.selected_index = Some(idx);
        cx.notify();
    }

    pub fn get_indexes(&self, cx: &App) -> Vec<IndexDefinition> {
        self.indexes.iter().map(|row| {
            let name = row.name_input.read(cx).text().to_string();
            let columns_str = row.columns_input.read(cx).text().to_string();
            let columns: Vec<String> = columns_str
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();

            IndexDefinition {
                name,
                columns,
                is_unique: row.is_unique,
                is_primary: false,
                index_type: None,
                comment: String::new(),
            }
        }).collect()
    }
}

impl EventEmitter<IndexesEditorEvent> for IndexesEditor {}

impl Focusable for IndexesEditor {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl Render for IndexesEditor {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        v_flex()
            .size_full()
            .child(
                h_flex()
                    .gap_1()
                    .items_center()
                    .px_2()
                    .py_1()
                    .border_b_1()
                    .border_color(cx.theme().border)
                    .child(
                        Button::new("add-idx")
                            .with_size(Size::Small)
                            .icon(IconName::Plus)
                            .tooltip("添加索引")
                            .on_click(cx.listener(|this, _, window, cx| this.add_index(window, cx)))
                    )
                    .child(
                        Button::new("remove-idx")
                            .with_size(Size::Small)
                            .icon(IconName::Minus)
                            .tooltip("删除索引")
                            .on_click(cx.listener(|this, _, _window, cx| this.remove_index(cx)))
                    )
            )
            .child(
                h_flex()
                    .gap_2()
                    .px_2()
                    .py_1()
                    .bg(cx.theme().muted)
                    .border_b_1()
                    .border_color(cx.theme().border)
                    .child(div().w(px(150.)).text_sm().font_semibold().child("索引名"))
                    .child(div().flex_1().text_sm().font_semibold().child("列"))
                    .child(div().w(px(80.)).text_sm().font_semibold().child("唯一"))
            )
            .child(
                v_flex()
                    .id("indexes-scroll")
                    .flex_1()
                    .overflow_y_scroll()
                    .children(
                        self.indexes.iter().enumerate().map(|(idx, row)| {
                            let is_selected = self.selected_index == Some(idx);
                            h_flex()
                                .id(("idx-row", idx))
                                .gap_2()
                                .px_2()
                                .py_1()
                                .when(is_selected, |this| this.bg(cx.theme().accent))
                                .border_b_1()
                                .border_color(cx.theme().border)
                                .on_mouse_down(MouseButton::Left, cx.listener(move |this, _, _window, cx| {
                                    this.select_row(idx, cx);
                                }))
                                .child(
                                    div().w(px(150.)).child(
                                        Input::new(&row.name_input)
                                            .w_full()
                                            .with_size(Size::XSmall)
                                    )
                                )
                                .child(
                                    div().flex_1().child(
                                        Input::new(&row.columns_input)
                                            .w_full()
                                            .with_size(Size::XSmall)
                                    )
                                )
                                .child(
                                    div().w(px(80.)).child(
                                        Button::new(("unique", idx))
                                            .with_size(Size::XSmall)
                                            .when(row.is_unique, |btn| btn.primary())
                                            .label(if row.is_unique { "✓" } else { "" })
                                            .on_click(cx.listener(move |this, _, _window, cx| this.toggle_unique(idx, cx)))
                                    )
                                )
                        })
                    )
            )
    }
}

// === Table Options Editor ===

pub enum TableOptionsEvent {
    Changed,
}

#[derive(Clone, Debug)]
pub struct EngineSelectItem {
    pub name: String,
}

impl SelectItem for EngineSelectItem {
    type Value = String;

    fn title(&self) -> SharedString {
        self.name.clone().into()
    }

    fn value(&self) -> &Self::Value {
        &self.name
    }
}

#[derive(Clone, Debug)]
pub struct CharsetSelectItem {
    pub info: CharsetInfo,
}

impl SelectItem for CharsetSelectItem {
    type Value = String;

    fn title(&self) -> SharedString {
        format!("{} - {}", self.info.name, self.info.description).into()
    }

    fn value(&self) -> &Self::Value {
        &self.info.name
    }
}

#[derive(Clone, Debug)]
pub struct CollationSelectItem {
    pub info: CollationInfo,
}

impl SelectItem for CollationSelectItem {
    type Value = String;

    fn title(&self) -> SharedString {
        if self.info.is_default {
            format!("{} (default)", self.info.name).into()
        } else {
            self.info.name.clone().into()
        }
    }

    fn value(&self) -> &Self::Value {
        &self.info.name
    }
}

pub struct TableOptionsEditor {
    focus_handle: FocusHandle,
    _database_type: DatabaseType,
    engine_select: Entity<SelectState<Vec<EngineSelectItem>>>,
    charset_select: Entity<SelectState<Vec<CharsetSelectItem>>>,
    collation_select: Entity<SelectState<Vec<CollationSelectItem>>>,
    comment_input: Entity<InputState>,
    _subscriptions: Vec<Subscription>,
}

impl TableOptionsEditor {
    pub fn new(database_type: DatabaseType, window: &mut Window, cx: &mut Context<Self>) -> Self {
        let focus_handle = cx.focus_handle();

        let engines = vec![
            EngineSelectItem { name: "InnoDB".to_string() },
            EngineSelectItem { name: "MyISAM".to_string() },
            EngineSelectItem { name: "MEMORY".to_string() },
        ];
        let engine_select = cx.new(|cx| {
            SelectState::new(engines, Some(IndexPath::new(0)), window, cx)
        });

        let charsets = Self::get_charsets(&database_type, cx);
        let charset_items: Vec<CharsetSelectItem> = charsets
            .iter()
            .cloned()
            .map(|info| CharsetSelectItem { info })
            .collect();
        let charset_select = cx.new(|cx| {
            SelectState::new(charset_items, Some(IndexPath::new(0)), window, cx)
        });

        let default_charset = charsets.first()
            .map(|c| c.name.clone())
            .unwrap_or_else(|| "utf8mb4".to_string());
        let collations = Self::get_collations(&database_type, &default_charset, cx);
        let collation_items: Vec<CollationSelectItem> = collations
            .iter()
            .cloned()
            .map(|info| CollationSelectItem { info })
            .collect();
        let default_coll_idx = collation_items.iter()
            .position(|c| c.info.is_default)
            .unwrap_or(0);
        let collation_select = cx.new(|cx| {
            SelectState::new(collation_items, Some(IndexPath::new(default_coll_idx)), window, cx)
        });

        let comment_input = cx.new(|cx| InputState::new(window, cx).placeholder("表注释"));

        let engine_sub = cx.observe(&engine_select, |_this, _, cx| {
            cx.emit(TableOptionsEvent::Changed);
        });
        let charset_sub = cx.observe(&charset_select, |_this, _, cx| {
            cx.emit(TableOptionsEvent::Changed);
        });
        let collation_sub = cx.observe(&collation_select, |_this, _, cx| {
            cx.emit(TableOptionsEvent::Changed);
        });
        let comment_sub = cx.observe(&comment_input, |_this, _, cx| {
            cx.emit(TableOptionsEvent::Changed);
        });

        Self {
            focus_handle,
            _database_type: database_type,
            engine_select,
            charset_select,
            collation_select,
            comment_input,
            _subscriptions: vec![engine_sub, charset_sub, collation_sub, comment_sub],
        }
    }

    fn get_charsets(database_type: &DatabaseType, cx: &App) -> Vec<CharsetInfo> {
        let global_state = cx.global::<GlobalDbState>();
        if let Ok(plugin) = global_state.db_manager.get_plugin(database_type) {
            plugin.get_charsets()
        } else {
            vec![CharsetInfo {
                name: "utf8mb4".to_string(),
                description: "UTF-8 Unicode".to_string(),
                default_collation: "utf8mb4_general_ci".to_string(),
            }]
        }
    }

    fn get_collations(database_type: &DatabaseType, charset: &str, cx: &App) -> Vec<CollationInfo> {
        let global_state = cx.global::<GlobalDbState>();
        if let Ok(plugin) = global_state.db_manager.get_plugin(database_type) {
            plugin.get_collations(charset)
        } else {
            vec![CollationInfo {
                name: "utf8mb4_general_ci".to_string(),
                charset: "utf8mb4".to_string(),
                is_default: true,
            }]
        }
    }

    pub fn get_options(&self, cx: &App) -> TableOptions {
        let engine = self.engine_select.read(cx).selected_value().cloned();
        let charset = self.charset_select.read(cx).selected_value().cloned();
        let collation = self.collation_select.read(cx).selected_value().cloned();
        let comment = self.comment_input.read(cx).text().to_string();

        TableOptions {
            engine,
            charset,
            collation,
            comment,
            auto_increment: None,
        }
    }
}

impl EventEmitter<TableOptionsEvent> for TableOptionsEditor {}

impl Focusable for TableOptionsEditor {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl Render for TableOptionsEditor {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        v_flex()
            .size_full()
            .p_4()
            .gap_4()
            .child(
                h_form()
                    .with_size(Size::Small)
                    .columns(1)
                    .label_width(px(80.))
                    .child(
                        field()
                            .label("引擎")
                            .items_center()
                            .label_justify_end()
                            .child(Select::new(&self.engine_select).w(px(200.)))
                    )
                    .child(
                        field()
                            .label("字符集")
                            .items_center()
                            .label_justify_end()
                            .child(Select::new(&self.charset_select).w(px(200.)))
                    )
                    .child(
                        field()
                            .label("排序规则")
                            .items_center()
                            .label_justify_end()
                            .child(Select::new(&self.collation_select).w(px(200.)))
                    )
                    .child(
                        field()
                            .label("注释")
                            .items_center()
                            .label_justify_end()
                            .child(Input::new(&self.comment_input).w(px(300.)))
                    )
            )
    }
}

// === TableDesignerTabContent - TabContent wrapper ===

#[derive(Clone)]
pub struct TableDesignerTabContent {
    pub title: SharedString,
    pub designer: Entity<TableDesigner>,
}

impl TableDesignerTabContent {
    pub fn new(
        title: impl Into<SharedString>,
        config: TableDesignerConfig,
        window: &mut Window,
        cx: &mut App,
    ) -> Self {
        let designer = cx.new(|cx| TableDesigner::new(config, window, cx));
        Self {
            title: title.into(),
            designer,
        }
    }
}

impl TabContent for TableDesignerTabContent {
    fn title(&self) -> SharedString {
        self.title.clone()
    }

    fn icon(&self) -> Option<IconName> {
        Some(IconName::Table)
    }

    fn closeable(&self) -> bool {
        true
    }

    fn render_content(&self, _window: &mut Window, _cx: &mut App) -> AnyElement {
        self.designer.clone().into_any_element()
    }

    fn content_type(&self) -> TabContentType {
        TabContentType::Custom("TableDesigner".to_string())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
