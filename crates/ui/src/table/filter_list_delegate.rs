// 1. 标准库导入
use std::rc::Rc;

// 2. 外部 crate 导入
use gpui::{actions, div, App, Context, ElementId, IntoElement, ParentElement, RenderOnce, SharedString, Styled, Task, Window};

// 3. 当前 crate 导入
use crate::{
    checkbox::Checkbox,
    h_flex,
    label::Label,
    list::{ListDelegate, ListItem, ListState},
    ActiveTheme, IndexPath,
};

actions!(filter_list, [ApplyFilter, ClearFilter, SelectedCompany]);

/// 筛选行数据 - 表示列中的一个唯一值及其出现次数
#[derive(Clone)]
pub struct FilterRow {
    pub name: SharedString,
    pub count: usize,
}

#[derive(IntoElement)]
pub struct FilterListItem {
    base: ListItem,
    value: Rc<FilterRow>,
    checked: bool,
}

impl FilterListItem {
    pub fn new(id: impl Into<ElementId>, value: Rc<FilterRow>, checked: bool) -> Self {
        FilterListItem {
            value,
            base: ListItem::new(id),
            checked,
        }
    }
}

impl crate::Selectable for FilterListItem {
    fn selected(mut self, selected: bool) -> Self {
        self.base = self.base.selected(selected);
        self
    }
    
    fn is_selected(&self) -> bool {
        self.checked
    }
}

impl RenderOnce for FilterListItem {
    fn render(self, _: &mut Window, cx: &mut App) -> impl IntoElement {
        self.base
            .px_2()
            .py_1()
            .overflow_x_hidden()
            .child(
                h_flex()
                    .items_center()
                    .justify_between()
                    .gap_2()
                    .child(
                        Checkbox::new(SharedString::from(format!("filter-value-{}", self.value.name)))
                            .checked(self.checked)
                    )
                    .child(
                        h_flex()
                            .flex_1()
                            .gap_2()
                            .overflow_x_hidden()
                            .child(Label::new(self.value.name.clone()).whitespace_nowrap()),
                    )
                    .child(
                        div()
                            .text_xs()
                            .text_color(cx.theme().muted_foreground)
                            .child(format!("({})", self.value.count)),
                    ),
            )
    }
}

/// 筛选列表的Delegate - 简化版本，只负责显示列的唯一值
pub struct FilterListDelegate {
    filter_rows: Vec<Rc<FilterRow>>,
    selected_index: Option<IndexPath>,
}

impl FilterListDelegate {
    pub fn new(filter_rows: Vec<Rc<FilterRow>>) -> Self {
        FilterListDelegate {
            filter_rows,
            selected_index: None,
        }
    }

    pub fn update_data(&mut self, filter_rows: Vec<Rc<FilterRow>>) {
        self.filter_rows = filter_rows;
    }
}

impl ListDelegate for FilterListDelegate {
    type Item = FilterListItem;

    fn perform_search(
        &mut self,
        _query: &str,
        _: &mut Window,
        _: &mut Context<ListState<Self>>,
    ) -> Task<()> {
        // 不需要搜索功能 - 列筛选直接显示所有唯一值
        Task::ready(())
    }

    fn items_count(&self, _section: usize, _: &App) -> usize {
        self.filter_rows.len()
    }

    fn render_item(
        &mut self,
        ix: IndexPath,
        _: &mut Window,
        _: &mut Context<ListState<Self>>,
    ) -> Option<Self::Item> {
        let selected = Some(ix) == self.selected_index;
        self.filter_rows.get(ix.row).map(|row| {
            FilterListItem::new(ix, row.clone(), selected)
        })
    }

    fn loading(&self, _: &App) -> bool {
        false
    }

    fn set_selected_index(
        &mut self,
        ix: Option<IndexPath>,
        _: &mut Window,
        cx: &mut Context<ListState<Self>>,
    ) {
        self.selected_index = ix;
        cx.notify();
    }

    fn confirm(&mut self, _secondary: bool, _window: &mut Window, _cx: &mut Context<ListState<Self>>) {
        // 列筛选不需要确认操作 - 通过复选框直接选择
    }
    

    fn load_more_threshold(&self) -> usize {
        0
    }

    fn load_more(&mut self, _window: &mut Window, _cx: &mut Context<ListState<Self>>) {
        // 不需要懒加载 - 列的唯一值数量通常不大
    }
}