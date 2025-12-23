use std::rc::Rc;
use std::time::{Duration, Instant};

use gpui::{
    div, prelude::FluentBuilder as _, uniform_list, AnyElement, App, ElementId, Entity,
    FocusHandle, InteractiveElement as _, IntoElement, ListSizingBehavior,
    MouseButton, ParentElement, RenderOnce, StyleRefinement, Styled,
    UniformListScrollHandle, Window, Context, Render, px,
};
use crate::{h_flex, IconName, StyledExt};
use crate::scroll::{ScrollableElement};
use crate::tree::TreeItem;

/// 创建一个支持右键菜单的树形视图
///
/// # 参数
///
/// * `state` - ContextMenuTreeState 实体
/// * `render_item` - 渲染每个树节点的闭包，返回 AnyElement
///
/// # 示例
///
/// ```ignore
/// context_menu_tree(&tree_state, |ix, item, depth, selected, window, cx| {
///     let list_item = ListItem::new(ix)
///         .pl(px(12.) * depth + px(8.))
///         .child(item.label.clone());
///
///     div()
///         .child(list_item)
///         .child(ContextMenu::new(("menu", ix)).menu(|menu, _, _| {
///             menu.item(PopupMenuItem::new("Delete"))
///         }))
///         .into_any_element()
/// })
/// ```
pub fn context_menu_tree<R>(state: &Entity<ContextMenuTreeState>, render_item: R) -> ContextMenuTree
where
    R: Fn(usize, &TreeItem, usize, bool, &mut Window, &mut App) -> AnyElement + 'static,
{
    ContextMenuTree::new(state, render_item)
        .on_click(|_, _, _| {})
        .on_double_click(|_, _, _| {})
}

/// 扁平化的树条目
#[derive(Clone)]
pub struct FlatTreeEntry {
    pub item: TreeItem,
    pub depth: usize,
}

/// 支持右键菜单的树形视图状态
pub struct ContextMenuTreeState {
    pub focus_handle: FocusHandle,
    pub entries: Vec<FlatTreeEntry>,
    pub scroll_handle: UniformListScrollHandle,
    pub selected_ix: Option<usize>,
    pub last_click_time: Option<Instant>,
    pub last_click_index: Option<usize>,
}

impl ContextMenuTreeState {
    /// 创建新的树状态
    pub fn new(cx: &mut App) -> Self {
        Self {
            selected_ix: None,
            focus_handle: cx.focus_handle(),
            scroll_handle: UniformListScrollHandle::default(),
            entries: Vec::new(),
            last_click_time: None,
            last_click_index: None,
        }
    }

    /// 设置树项目
    pub fn items(mut self, items: impl Into<Vec<TreeItem>>) -> Self {
        let items = items.into();
        self.entries.clear();
        for item in items.into_iter() {
            self.add_entry(item, 0);
        }
        self
    }

    /// 更新树项目
    pub fn set_items(&mut self, items: impl Into<Vec<TreeItem>>, cx: &mut Context<Self>) {
        let items = items.into();
        self.entries.clear();
        for item in items.into_iter() {
            self.add_entry(item, 0);
        }
        self.selected_ix = None;
        cx.notify();
    }

    /// 获取当前选中的索引
    pub fn selected_index(&self) -> Option<usize> {
        self.selected_ix
    }

    /// 设置选中的索引
    pub fn set_selected_index(&mut self, ix: Option<usize>, cx: &mut Context<Self>) {
        self.selected_ix = ix;
        cx.notify();
    }

    /// 滚动到指定项
    pub fn scroll_to_item(&mut self, ix: usize, strategy: gpui::ScrollStrategy) {
        self.scroll_handle.scroll_to_item(ix, strategy);
    }

    /// 获取当前选中的条目
    pub fn selected_entry(&self) -> Option<&FlatTreeEntry> {
        self.selected_ix.and_then(|ix| self.entries.get(ix))
    }

    /// 递归添加条目
    fn add_entry(&mut self, item: TreeItem, depth: usize) {
        self.entries.push(FlatTreeEntry {
            item: item.clone(),
            depth,
        });

        if item.is_expanded() {
            for child in &item.children {
                self.add_entry(child.clone(), depth + 1);
            }
        }
    }

    /// 重建条目列表（在展开/折叠后调用）
    pub fn rebuild_entries(&mut self, cx: &mut Context<Self>) {
        let root_items: Vec<TreeItem> = self
            .entries
            .iter()
            .filter(|e| e.depth == 0)
            .map(|e| e.item.clone())
            .collect();

        self.entries.clear();
        for item in root_items.into_iter() {
            self.add_entry(item, 0);
        }

        cx.notify();
    }
}

impl Render for ContextMenuTreeState {
    fn render(&mut self, _: &mut Window, _: &mut Context<Self>) -> impl IntoElement {
        div()
    }
}

/// 支持右键菜单的树形视图组件
#[derive(IntoElement)]
pub struct ContextMenuTree {
    id: ElementId,
    state: Entity<ContextMenuTreeState>,
    style: StyleRefinement,
    render_item: Rc<dyn Fn(usize, &TreeItem, usize, bool, &mut Window, &mut App) -> AnyElement>,
    on_click: Option<Rc<dyn Fn(usize, &TreeItem, &mut App)>>,
    on_double_click: Option<Rc<dyn Fn(usize, &TreeItem, &mut App)>>,
}

impl ContextMenuTree {
    pub fn new<R>(state: &Entity<ContextMenuTreeState>, render_item: R) -> Self
    where
        R: Fn(usize, &TreeItem, usize, bool, &mut Window, &mut App) -> AnyElement + 'static,
    {
        Self {
            id: ElementId::Name(format!("context-menu-tree-{}", state.entity_id()).into()),
            state: state.clone(),
            style: StyleRefinement::default(),
            render_item: Rc::new(move |ix, item, depth, selected, window, app| {
                render_item(ix, item, depth, selected, window, app)
            }),
            on_click: None,
            on_double_click: None,
        }
    }

    /// 设置单击回调
    pub fn on_click<F>(mut self, callback: F) -> Self
    where
        F: Fn(usize, &TreeItem, &mut App) + 'static,
    {
        self.on_click = Some(Rc::new(callback));
        self
    }

    /// 设置双击回调
    pub fn on_double_click<F>(mut self, callback: F) -> Self
    where
        F: Fn(usize, &TreeItem, &mut App) + 'static,
    {
        self.on_double_click = Some(Rc::new(callback));
        self
    }

    fn on_toggle_expand(state: &Entity<ContextMenuTreeState>, ix: usize, _: &mut Window, cx: &mut App) {
        state.update(cx, |state, cx| {
            if let Some(entry) = state.entries.get(ix) {
                if entry.item.is_folder() {
                    let item_id = entry.item.id.clone();
                    let current_expanded = entry.item.is_expanded();

                    // 重建所有树项，切换目标项的展开状态
                    let new_items = state.entries
                        .iter()
                        .filter(|e| e.depth == 0)
                        .map(|e| {
                            Self::toggle_item_expanded(&e.item, &item_id, current_expanded)
                        })
                        .collect::<Vec<_>>();

                    state.entries.clear();
                    for item in new_items.into_iter() {
                        state.add_entry(item, 0);
                    }

                    cx.notify();
                }
            }
        })
    }

    fn on_item_click(
        state: &Entity<ContextMenuTreeState>,
        ix: usize,
        on_click: Option<Rc<dyn Fn(usize, &TreeItem, &mut App)>>,
        on_double_click: Option<Rc<dyn Fn(usize, &TreeItem, &mut App)>>,
        _: &mut Window,
        cx: &mut App,
    ) {
        let (is_double_click, item) = state.update(cx, |state, cx| {
            // 设置选中索引
            state.set_selected_index(Some(ix), cx);

            let now = Instant::now();
            let is_double = if let (Some(last_time), Some(last_ix)) =
                (state.last_click_time, state.last_click_index)
            {
                last_ix == ix && now.duration_since(last_time) < Duration::from_millis(500)
            } else {
                false
            };

            state.last_click_time = Some(now);
            state.last_click_index = Some(ix);

            let item = state.entries.get(ix).map(|e| e.item.clone());
            (is_double, item)
        });

        if let Some(item) = item {
            if is_double_click {
                if let Some(cb) = on_double_click {
                    cb(ix, &item, cx);
                }
            } else {
                if let Some(cb) = on_click {
                    cb(ix, &item, cx);
                }
            }
        }
    }

    // 递归切换树项的展开状态
    fn toggle_item_expanded(item: &TreeItem, target_id: &str, current_expanded: bool) -> TreeItem {
        let mut new_item = TreeItem::new(item.id.clone(), item.label.clone())
            .expanded(if item.id.as_ref() == target_id {
                !current_expanded
            } else {
                item.is_expanded()
            })
            .disabled(item.is_disabled());

        for child in &item.children {
            new_item = new_item.child(Self::toggle_item_expanded(child, target_id, current_expanded));
        }

        new_item
    }
}

impl Styled for ContextMenuTree {
    fn style(&mut self) -> &mut StyleRefinement {
        &mut self.style
    }
}

impl RenderOnce for ContextMenuTree {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let tree_state = self.state.read(cx);
        let render_item = self.render_item.clone();
        let on_click = self.on_click.clone();
        let on_double_click = self.on_double_click.clone();
        let scroll_handle = self.state.read(cx).scroll_handle.clone();

        div()
            .id(self.id)
            .track_focus(&tree_state.focus_handle)
            .size_full()
            .child(
                uniform_list("entries", tree_state.entries.len(), {
                    let selected_ix = tree_state.selected_index();
                    let entries = tree_state.entries.clone();
                    let state = self.state.clone();
                    move |visible_range, window, cx| {
                        let mut items = Vec::with_capacity(visible_range.len());
                        for ix in visible_range {
                            let entry = &entries[ix];
                            let selected = Some(ix) == selected_ix;
                            let has_children = entry.item.is_folder();
                            let is_expanded = entry.item.is_expanded();

                            // 创建展开/收起箭头
                            let arrow = if has_children {
                                div()
                                    .w(px(16.))
                                    .h(px(16.))
                                    .flex()
                                    .items_center()
                                    .justify_center()
                                    .cursor_pointer()
                                    .child(if is_expanded {
                                        IconName::ChevronDown
                                    } else {
                                        IconName::ChevronRight
                                    })
                                    .on_mouse_down(MouseButton::Left, {
                                        let state = state.clone();
                                        move |_, window, cx| {
                                            cx.stop_propagation();
                                            Self::on_toggle_expand(&state, ix, window, cx);
                                        }
                                    })
                            } else {
                                // 占位符，保持对齐
                                div().w(px(16.)).h(px(16.))
                            };

                            // 计算缩进 (每层 12px)
                            let indent = px(12.) * entry.depth as f32;

                            // 调用用户提供的 render_item 函数，返回 AnyElement
                            let item_content = (render_item)(ix, &entry.item, entry.depth, selected, window, cx);

                            // 组合缩进、箭头和内容
                            let el = div()
                                .id(ix)
                                .w_full()
                                .overflow_hidden()
                                .child(
                                    h_flex()
                                        .w_full()
                                        .min_w(px(0.))
                                        .overflow_hidden()
                                        .gap_1()
                                        .items_center()
                                        .pl(indent)  // 添加层级缩进
                                        .child(arrow)
                                        .child(
                                            div()
                                                .flex_1()
                                                .min_w(px(0.))
                                                .overflow_hidden()
                                                .child(item_content)
                                        )
                                )
                                .when(!entry.item.is_disabled(), |this| {
                                    this.on_mouse_down(MouseButton::Left, {
                                        let state = state.clone();
                                        let on_click = on_click.clone();
                                        let on_double_click = on_double_click.clone();
                                        move |_, window, cx| {
                                            Self::on_item_click(&state, ix, on_click.clone(), on_double_click.clone(), window, cx);
                                        }
                                    })
                                });

                            items.push(el)
                        }

                        items
                    }
                })
                .flex_grow()
                .size_full()
                .track_scroll(&tree_state.scroll_handle.clone())
                .with_sizing_behavior(ListSizingBehavior::Auto)
                .into_any_element(),
            )
            .refine_style(&self.style)
            .relative()
            .child(
                div()
                    .absolute()
                    .top_0()
                    .right_0()
                    .bottom_0()
                    .w(px(12.))  // Scrollbar 宽度
                    .vertical_scrollbar(&scroll_handle)
            )
    }
}
