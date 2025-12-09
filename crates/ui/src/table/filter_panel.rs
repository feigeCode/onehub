// 1. 标准库导入
use std::collections::HashSet;

// 2. 外部 crate 导入
use gpui::{div, prelude::FluentBuilder, px, AnyElement, App, IntoElement, ParentElement, SharedString, Styled};

// 3. 当前 crate 导入
use crate::{
    button::{Button, ButtonVariants},
    checkbox::Checkbox,
    h_flex,
    label::Label,
    v_flex, ActiveTheme, Sizable, Size,
};

/// 筛选值项
#[derive(Clone, Debug)]
pub struct FilterValue {
    pub value: String,
    pub count: usize,
    pub selected: bool,
}

/// 筛选面板组件 - 用于在 Popover 中显示筛选选项
pub struct FilterPanel {
    values: Vec<FilterValue>,
}

impl FilterPanel {
    /// 创建新的筛选面板
    pub fn new(values: Vec<FilterValue>) -> Self {
        Self { values }
    }

    /// 获取当前选中的值
    pub fn get_selected_values(&self) -> HashSet<String> {
        self.values
            .iter()
            .filter(|v| v.selected)
            .map(|v| v.value.clone())
            .collect()
    }

    /// 切换值的选中状态
    pub fn toggle_value(&mut self, value: &str) {
        if let Some(v) = self.values.iter_mut().find(|v| v.value == value) {
            v.selected = !v.selected;
        }
    }

    /// 全选
    pub fn select_all(&mut self) {
        for v in &mut self.values {
            v.selected = true;
        }
    }

    /// 清空选择
    pub fn deselect_all(&mut self) {
        for v in &mut self.values {
            v.selected = false;
        }
    }

    /// 渲染筛选面板
    pub fn render(&self, cx: &mut App) -> impl IntoElement {
        let selected_count = self.values.iter().filter(|v| v.selected).count();
        let total_count = self.values.len();

        v_flex()
            .w(px(280.))
            .max_h(px(400.))
            .gap_2()
            .p_2()
            // 操作按钮
            .child(
                h_flex()
                    .justify_between()
                    .items_center()
                    .child(
                        div()
                            .text_xs()
                            .text_color(cx.theme().muted_foreground)
                            .child(format!("已选 {} / {}", selected_count, total_count)),
                    )
                    .child(
                        h_flex()
                            .gap_1()
                            .child(
                                Button::new("select-all")
                                    .label("全选")
                                    .ghost()
                                    .with_size(Size::XSmall),
                            )
                            .child(
                                Button::new("deselect-all")
                                    .label("清空")
                                    .ghost()
                                    .with_size(Size::XSmall),
                            ),
                    ),
            )
            // 分隔线
            .child(div().h(px(1.)).w_full().bg(cx.theme().border))
            // 唯一值列表
            .child(
                v_flex()
                    .flex_1()
                    .gap_1()
                    .children(self.values.iter().map(|v| {
                        self.render_value_item(v, cx)
                    }).collect::<Vec<AnyElement>>())
                    .when(self.values.is_empty(), |this| {
                        this.child(
                            div()
                                .p_4()
                                .text_center()
                                .text_sm()
                                .text_color(cx.theme().muted_foreground)
                                .child("无匹配结果"),
                        )
                    }),
            )
    }

    /// 渲染单个值项
    fn render_value_item(
        &self,
        v: &FilterValue,
        cx: &mut App,
    ) -> AnyElement {
        let theme = cx.theme();
        h_flex()
            .w_full()
            .px_2()
            .py_1()
            .rounded(px(4.))
            .items_center()
            .justify_between()
            .gap_2()
            .cursor_pointer()
            .child(
                h_flex()
                    .flex_1()
                    .gap_2()
                    .items_center()
                    .overflow_x_hidden()
                    .child(
                        Checkbox::new(SharedString::from(format!("filter-{}", v.value)))
                            .checked(v.selected),
                    )
                    .child(
                        Label::new(v.value.clone())
                            .whitespace_nowrap()
                            .overflow_hidden()
                            .text_ellipsis(),
                    ),
            )
            .child(
                div()
                    .text_xs()
                    .text_color(theme.muted_foreground)
                    .child(format!("({})", v.count)),
            ).into_any_element()
    }
}
