use std::collections::HashMap;

use gpui::{px, prelude::*, App, Context, Entity, EventEmitter, FocusHandle, Focusable, IntoElement, ParentElement, Render, Styled, Window};
use gpui_component::{
    button::{Button, ButtonVariants},
    form::{field, v_form},
    h_flex,
    input::{Input, InputState},
    select::{Select, SelectItem, SelectState},
    v_flex, IndexPath, Sizable, Size,
};
use db::plugin::DatabaseOperationRequest;

/// MySQL 字符集选项
#[derive(Clone, Debug)]
pub struct CharsetSelectItem {
    pub value: String,
}

impl CharsetSelectItem {
    pub fn new(value: impl Into<String>) -> Self {
        Self {
            value: value.into(),
        }
    }
}

impl SelectItem for CharsetSelectItem {
    type Value = String;

    fn title(&self) -> gpui::SharedString {
        self.value.clone().into()
    }

    fn value(&self) -> &Self::Value {
        &self.value
    }
}

/// MySQL 排序规则选项
#[derive(Clone, Debug)]
pub struct CollationSelectItem {
    pub value: String,
}

impl CollationSelectItem {
    pub fn new(value: impl Into<String>) -> Self {
        Self {
            value: value.into(),
        }
    }
}

impl SelectItem for CollationSelectItem {
    type Value = String;

    fn title(&self) -> gpui::SharedString {
        self.value.clone().into()
    }

    fn value(&self) -> &Self::Value {
        &self.value
    }
}

pub enum DatabaseFormEvent {
    Save(DatabaseOperationRequest),
    Cancel,
}

/// MySQL 数据库创建表单
pub struct DatabaseForm {
    focus_handle: FocusHandle,
    // MySQL 专用字段
    name_input: Entity<InputState>,
    charset_select: Entity<SelectState<Vec<CharsetSelectItem>>>,
    collation_select: Entity<SelectState<Vec<CollationSelectItem>>>,
}

impl DatabaseForm {
    /// 创建 MySQL 数据库表单
    pub fn new_mysql(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let focus_handle = cx.focus_handle();

        // 数据库名称输入框
        let name_input = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder("输入数据库名称")
        });

        // 字符集选择
        let charset_items = vec![
            CharsetSelectItem::new("utf8mb4"),
            CharsetSelectItem::new("utf8"),
            CharsetSelectItem::new("latin1"),
        ];
        let charset_select = cx.new(|cx| {
            SelectState::new(charset_items, Some(IndexPath::new(0)), window, cx)
        });

        // 排序规则选择
        let collation_items = vec![
            CollationSelectItem::new("utf8mb4_general_ci"),
            CollationSelectItem::new("utf8mb4_unicode_ci"),
            CollationSelectItem::new("utf8_general_ci"),
        ];
        let collation_select = cx.new(|cx| {
            SelectState::new(collation_items, Some(IndexPath::new(0)), window, cx)
        });

        Self {
            focus_handle,
            name_input,
            charset_select,
            collation_select,
        }
    }

    fn build_mysql_request(&self, cx: &App) -> DatabaseOperationRequest {
        let mut field_values = HashMap::new();

        // 获取数据库名称
        let db_name = self.name_input.read(cx).text().to_string();
        
        // 获取字符集
        let charset = self.charset_select.read(cx)
            .selected_value()
            .cloned()
            .unwrap_or_else(|| "utf8mb4".to_string());
            
        // 获取排序规则
        let collation = self.collation_select.read(cx)
            .selected_value()
            .cloned()
            .unwrap_or_else(|| "utf8mb4_general_ci".to_string());

        field_values.insert("name".to_string(), db_name.clone());
        field_values.insert("charset".to_string(), charset);
        field_values.insert("collation".to_string(), collation);

        DatabaseOperationRequest {
            database_name: db_name,
            field_values,
        }
    }

    fn validate_mysql(&self, cx: &App) -> Result<(), String> {
        let db_name = self.name_input.read(cx).text().to_string();
        if db_name.trim().is_empty() {
            return Err("数据库名称不能为空".to_string());
        }
        
        // 简单的 MySQL 数据库名称验证
        if !db_name.chars().all(|c| c.is_alphanumeric() || c == '_') {
            return Err("数据库名称只能包含字母、数字和下划线".to_string());
        }
        
        Ok(())
    }

    pub fn trigger_save(&mut self, cx: &mut Context<Self>) {
        if let Err(e) = self.validate_mysql(cx) {
            // TODO: Show error message in UI
            eprintln!("Validation error: {}", e);
            return;
        }

        let request = self.build_mysql_request(cx);
        cx.emit(DatabaseFormEvent::Save(request));
    }

    pub fn trigger_cancel(&mut self, cx: &mut Context<Self>) {
        cx.emit(DatabaseFormEvent::Cancel);
    }
}

impl EventEmitter<DatabaseFormEvent> for DatabaseForm {}

impl Focusable for DatabaseForm {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl Render for DatabaseForm {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        v_flex()
            .gap_4()
            .p_4()
            .size_full()
            .child(
                // MySQL 数据库表单
                v_form()
                    .with_size(Size::Small)
                    .columns(1)
                    .label_width(px(100.))
                    .child(
                        field()
                            .label("数据库名称")
                            .required(true)
                            .items_center()
                            .label_justify_end()
                            .child(Input::new(&self.name_input).w_full())
                    )
                    .child(
                        field()
                            .label("字符集")
                            .items_center()
                            .label_justify_end()
                            .child(Select::new(&self.charset_select).w_full())
                    )
                    .child(
                        field()
                            .label("排序规则")
                            .items_center()
                            .label_justify_end()
                            .child(Select::new(&self.collation_select).w_full())
                    )
            )
            .child(
                // 按钮区域
                h_flex()
                    .gap_2()
                    .justify_end()
                    .child(
                        Button::new("cancel")
                            .ghost()
                            .label("取消")
                            .on_click(cx.listener(|form, _, _, cx| {
                                form.trigger_cancel(cx);
                            }))
                    )
                    .child(
                        Button::new("save")
                            .primary()
                            .label("创建")
                            .on_click(cx.listener(|form, _, _, cx| {
                                form.trigger_save(cx);
                            }))
                    )
            )
    }
}