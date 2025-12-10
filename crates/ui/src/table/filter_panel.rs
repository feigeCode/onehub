use gpui::{div, App, Context, ElementId, InteractiveElement, IntoElement, ParentElement, RenderOnce, SharedString, StatefulInteractiveElement, Styled, Window};
use std::collections::HashSet;
use std::rc::Rc;

use crate::list::{ListDelegate, ListItem, ListState};
use crate::{checkbox::Checkbox, h_flex, label::Label, ActiveTheme, IndexPath, Selectable};

/// 筛选值项
#[derive(Clone, Debug)]
pub struct FilterValue {
    pub value: String,
    pub count: usize,
    pub checked: bool,
    pub selected: bool,
}


#[derive(IntoElement)]
pub struct FilterListItem {
    pub base: ListItem,
    pub value: Rc<FilterValue>,
    pub selected: bool,
    /// 回调：当复选框状态改变时
    pub on_toggle: Option<Rc<dyn Fn(&mut Window, &mut App)>>,
}


impl FilterListItem {

    pub fn new(id: impl Into<ElementId> , value: Rc<FilterValue>, selected: bool) -> Self {
        Self {
            base: ListItem::new(id).selected(selected),
            value,
            selected,
            on_toggle: None,
        }
    }

    /// 设置切换回调
    pub fn on_toggle(mut self, handler: impl Fn(&mut Window, &mut App) + 'static) -> Self {
        self.on_toggle = Some(Rc::new(handler));
        self
    }
}


impl Selectable for FilterListItem {
    fn selected(mut self, selected: bool) -> Self {
        self.selected = selected;
        self
    }

    fn is_selected(&self) -> bool {
        self.selected
    }
}


impl RenderOnce for FilterListItem {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let on_toggle = self.on_toggle.clone();
        let value_str = self.value.value.clone();

        h_flex()
            .id(SharedString::from(format!("filter-item-{}", value_str)))
            .w_full()
            .px_2()
            .py_1()
            .items_center()
            .justify_between()
            .gap_2()
            .cursor_pointer()
            .on_click(move |_, window, cx| {
                if let Some(handler) = on_toggle.as_ref() {
                    handler(window, cx);
                }
            })
            .child(
                h_flex()
                    .flex_1()
                    .gap_2()
                    .items_center()
                    .overflow_x_hidden()
                    .child(
                        Checkbox::new(
                            SharedString::from(format!("filter-{}", value_str)),
                        )
                            .checked(self.value.checked),
                    )
                    .child(
                        Label::new(self.value.value.clone())
                            .whitespace_nowrap()
                            .overflow_hidden()
                            .text_ellipsis(),
                    ),
            )
            .child(
                div()
                    .text_xs()
                    .text_color(cx.theme().muted_foreground)
                    .child(format!("({})", self.value.count)),
            )
    }
}




/// 筛选面板组件 - 用于在 Popover 中显示筛选选项
pub struct FilterPanel {
    pub(crate) values: Vec<FilterValue>,
    selected_index: Option<IndexPath>,
    confirmed_index: Option<IndexPath>,
    /// 回调：当筛选值被切换时
    on_toggle: Option<Rc<dyn Fn(&str, &mut Window, &mut App)>>,
    /// 搜索查询
    search_query: String,
    /// 过滤后的值列表
    filtered_values: Vec<FilterValue>,
}

impl FilterPanel {
    /// 创建新的筛选面板
    pub fn new(values: Vec<FilterValue>) -> Self {
        let filtered_values = values.clone();
        Self { 
            values,
            selected_index: None,
            confirmed_index: None,
            on_toggle: None,
            search_query: String::new(),
            filtered_values,
        }
    }

    /// 设置切换回调
    pub fn on_toggle(mut self, handler: impl Fn(&str, &mut Window, &mut App) + 'static) -> Self {
        self.on_toggle = Some(Rc::new(handler));
        self
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
        // 更新主列表中的值
        if let Some(v) = self.values.iter_mut().find(|v| v.value == value) {
            v.selected = !v.selected;
            v.checked = v.selected;
        }
        
        // 同步更新过滤后的列表中的值
        if let Some(v) = self.filtered_values.iter_mut().find(|v| v.value == value) {
            v.selected = !v.selected;
            v.checked = v.selected;
        }
    }

    /// 全选（只选中当前可见的筛选项）
    pub fn select_all(&mut self) {
        // 获取当前可见项的值
        let visible_values: std::collections::HashSet<String> = self.filtered_values
            .iter()
            .map(|v| v.value.clone())
            .collect();
        
        // 只选中可见的项
        for v in &mut self.values {
            if visible_values.contains(&v.value) {
                v.selected = true;
                v.checked = true;
            }
        }
        
        // 同步更新filtered_values
        for v in &mut self.filtered_values {
            v.selected = true;
            v.checked = true;
        }
    }

    /// 清空选择（只清空当前可见的筛选项）
    pub fn deselect_all(&mut self) {
        // 获取当前可见项的值
        let visible_values: std::collections::HashSet<String> = self.filtered_values
            .iter()
            .map(|v| v.value.clone())
            .collect();
        
        // 只清空可见的项
        for v in &mut self.values {
            if visible_values.contains(&v.value) {
                v.selected = false;
                v.checked = false;
            }
        }
        
        // 同步更新filtered_values
        for v in &mut self.filtered_values {
            v.selected = false;
            v.checked = false;
        }
    }

    /// 设置搜索查询并更新过滤后的值列表
    pub fn set_search_query(&mut self, query: String) {
        self.search_query = query.clone();
        self.update_filtered_values();
    }

    /// 获取当前搜索查询
    pub fn search_query(&self) -> &str {
        &self.search_query
    }

    /// 更新过滤后的值列表（不区分大小写）
    fn update_filtered_values(&mut self) {
        if self.search_query.is_empty() {
            self.filtered_values = self.values.clone();
        } else {
            let query_lower = self.search_query.to_lowercase();
            self.filtered_values = self.values
                .iter()
                .filter(|v| v.value.to_lowercase().contains(&query_lower))
                .cloned()
                .collect();
        }
    }

    /// 获取过滤后的值列表
    pub fn filtered_values(&self) -> &[FilterValue] {
        &self.filtered_values
    }

}


impl ListDelegate for FilterPanel {
    type Item = FilterListItem;

    fn items_count(&self, _section: usize, _cx: &App) -> usize {
        self.filtered_values.len()
    }

    fn render_item(
        &mut self,
        ix: IndexPath,
        _: &mut Window,
        _: &mut Context<ListState<Self>>,
    ) -> Option<Self::Item> {
        let selected = Some(ix) == self.selected_index || Some(ix) == self.confirmed_index;
        if let Some(value) = self.filtered_values.get(ix.row) {
            let value_rc = Rc::from(value.clone());
            let mut item = FilterListItem::new(ix, value_rc.clone(), selected);
            
            // 如果有回调，设置到item上
            if let Some(on_toggle) = self.on_toggle.as_ref() {
                let on_toggle = on_toggle.clone();
                let value_str = value.value.clone();
                item = item.on_toggle(move |window, cx| {
                    on_toggle(&value_str, window, cx);
                });
            }
            
            return Some(item);
        }
        None
    }

    fn set_selected_index(&mut self, ix: Option<IndexPath>, _window: &mut Window, cx: &mut Context<ListState<Self>>) {
        self.selected_index = ix;
        cx.notify();
    }
}
