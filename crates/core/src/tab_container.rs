use gpui::prelude::FluentBuilder;
use gpui::{div, px, AnyElement, App, AppContext as _, Context, Corner, Entity, Focusable, InteractiveElement, IntoElement, MouseButton, ParentElement, Render, RenderOnce, SharedString, Styled, Task, Window};
use gpui::{ScrollHandle, StatefulInteractiveElement as _};
use gpui_component::button::{Button, ButtonVariants as _};
use gpui_component::list::{List, ListDelegate, ListState};
use gpui_component::menu::{ContextMenuExt, PopupMenuItem};
use gpui_component::popover::Popover;
use gpui_component::{h_flex, v_flex, ActiveTheme, Icon, IconName, IndexPath, Selectable, Size};
use std::{any::Any, sync::Arc};
// ============================================================================
// TabContent Trait - Strategy Pattern Interface
// ============================================================================

/// Trait that defines how tab content should be rendered.
/// Different tab types implement this trait to provide their own rendering logic.
pub trait TabContent: Send + Sync {
    /// Get the tab title
    fn title(&self) -> SharedString;

    /// Get optional icon for the tab
    fn icon(&self) -> Option<Icon> {
        None
    }

    /// Check if tab can be closed
    fn closeable(&self) -> bool {
        true
    }

    /// Render the content of this tab
    fn render_content(&self, window: &mut Window, cx: &mut App) -> AnyElement;

    /// Called when tab becomes active
    fn on_activate(&self, _window: &mut Window, _cx: &mut App) {}

    /// Called when tab becomes inactive
    fn on_deactivate(&self, _window: &mut Window, _cx: &mut App) {}

    /// Get tab content type for identification
    fn content_type(&self) -> TabContentType;

    /// Get tab's preferred width size
    /// Returns None to use container's default size
    fn width_size(&self) -> Option<Size> {
        None  // Default: use container's default size
    }

    /// Enable downcasting to concrete types
    fn as_any(&self) -> &dyn Any;
}

/// Type-safe enum for different tab content types
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TabContentType {
    SqlEditor,
    TableData(String),    // Table name
    TableForm(String),    // Table name
    QueryResult(String),  // Query ID
    Custom(String),       // Custom type identifier
}

// ============================================================================
// TabItem - Represents a single tab with its content
// ============================================================================

pub struct TabItem {
    id: String,
    content: Arc<dyn TabContent>,
}

impl TabItem {
    pub fn new(id: impl Into<String>, content: impl TabContent + 'static) -> Self {
        Self {
            id: id.into(),
            content: Arc::new(content),
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn content(&self) -> &Arc<dyn TabContent> {
        &self.content
    }
}

// ============================================================================
// DragTab - Visual representation during drag
// ============================================================================

/// Represents a tab being dragged, used for visual feedback
#[derive(Clone)]
pub struct DragTab {
    pub tab_index: usize,
    pub title: SharedString,
}

impl DragTab {
    pub fn new(tab_index: usize, title: SharedString) -> Self {
        Self {
            tab_index,
            title,
        }
    }
}

impl Render for DragTab {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .id("drag-tab")
            .cursor_grabbing()
            .py_1()
            .px_3()
            .min_w(px(80.0))
            .overflow_hidden()
            .whitespace_nowrap()
            .text_ellipsis()
            .border_1()
            .border_color(cx.theme().border)
            .rounded(px(6.0))
            .text_color(cx.theme().tab_foreground)
            .bg(cx.theme().tab_active)
            .opacity(0.85)
            .shadow_md()
            .text_sm()
            .child(self.title.clone())
    }
}

// ============================================================================
// TabListItem - Custom list item for tab dropdown
// ============================================================================

#[derive(IntoElement)]
pub struct TabListItem {
    tab_index: usize,
    title: SharedString,
    icon: Option<Icon>,
    closeable: bool,
    selected: bool,
    container: Entity<TabContainer>,
}

impl TabListItem {
    pub fn new(
        tab_index: usize,
        title: SharedString,
        icon: Option<Icon>,
        closeable: bool,
        selected: bool,
        container: Entity<TabContainer>,
    ) -> Self {
        Self {
            tab_index,
            title,
            icon,
            closeable,
            selected,
            container,
        }
    }
}

impl Selectable for TabListItem {
    fn selected(mut self, selected: bool) -> Self {
        self.selected = selected;
        self
    }

    fn is_selected(&self) -> bool {
        self.selected
    }
}

impl RenderOnce for TabListItem {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let container = self.container.clone();
        let tab_index = self.tab_index;
        let selected = self.selected;

        h_flex()
            .id(SharedString::from(format!("tab-item-{}", tab_index)))
            .w_full()
            .px_2()
            .py_1()
            .rounded(px(4.0))
            .items_center()
            .gap_2()
            .cursor_pointer()
            .when(selected, |el| {
                el.bg(cx.theme().list_active)
            })
            .when(!selected, |el| {
                el.hover(|style| style.bg(cx.theme().list_hover))
            })
            .when_some(self.icon, |el, icon| {
                el.child(
                    Icon::new(icon)
                        .size_4()
                        .text_color(cx.theme().muted_foreground)
                )
            })
            .child(
                div()
                    .flex_1()
                    .overflow_hidden()
                    .whitespace_nowrap()
                    .text_ellipsis()
                    .child(self.title)
            )
            .when(self.closeable, |el| {
                let container = container.clone();
                el.child(
                    div()
                        .id(SharedString::from(format!("close-btn-{}", tab_index)))
                        .flex()
                        .items_center()
                        .justify_center()
                        .w(px(16.0))
                        .h(px(16.0))
                        .rounded(px(2.0))
                        .cursor_pointer()
                        .text_color(cx.theme().muted_foreground)
                        .hover(|style| {
                            style
                                .bg(cx.theme().muted)
                                .text_color(cx.theme().foreground)
                        })
                        .on_mouse_down(MouseButton::Left, move |_event, _window, cx| {
                            container.update(cx, |this, cx| {
                                this.close_tab(tab_index, cx);
                            });
                        })
                        .child("×")
                )
            })
    }
}

// ============================================================================
// TabListDelegate - List delegate for tab dropdown
// ============================================================================

pub struct TabListDelegate {
    container: Entity<TabContainer>,
    tabs: Vec<(usize, SharedString, Option<Icon>, bool)>,
    filtered_tabs: Vec<(usize, SharedString, Option<Icon>, bool)>,
    selected_index: Option<IndexPath>,
}

impl ListDelegate for TabListDelegate {
    type Item = TabListItem;

    fn perform_search(&mut self, query: &str, _window: &mut Window, cx: &mut Context<ListState<Self>>) -> Task<()> {
        if query.is_empty() {
            self.filtered_tabs = self.tabs.clone();
        } else {
            let query_lower = query.to_lowercase();
            self.filtered_tabs = self.tabs
                .iter()
                .filter(|(_, title, _, _)| title.to_lowercase().contains(&query_lower))
                .cloned()
                .collect();
        }
        cx.notify();
        Task::ready(())
    }

    fn items_count(&self, _section: usize, _cx: &App) -> usize {
        self.filtered_tabs.len()
    }

    fn render_item(
        &mut self,
        ix: IndexPath,
        _window: &mut Window,
        cx: &mut Context<ListState<Self>>,
    ) -> Option<Self::Item> {
        let (tab_index, title, icon, closeable) = self.filtered_tabs.get(ix.row)?.clone();
        let active_index = self.container.read(cx).active_index();
        let is_active = tab_index == active_index;

        Some(TabListItem::new(
            tab_index,
            title,
            icon,
            closeable,
            is_active,
            self.container.clone(),
        ))
    }

    fn set_selected_index(
        &mut self,
        ix: Option<IndexPath>,
        _window: &mut Window,
        _cx: &mut Context<ListState<Self>>,
    ) {
        self.selected_index = ix;
    }

    fn confirm(&mut self, _secondary: bool, window: &mut Window, cx: &mut Context<ListState<Self>>) {
        if let Some(ix) = self.selected_index {
            if let Some((tab_index, _, _, _)) = self.filtered_tabs.get(ix.row) {
                let tab_index = *tab_index;
                self.container.update(cx, |this, cx| {
                    this.list_popover_open = false;
                    this.set_active_index(tab_index, window, cx);
                });
            }
        }
    }

    fn cancel(&mut self, _window: &mut Window, cx: &mut Context<ListState<Self>>) {
        self.container.update(cx, |this, cx| {
            this.list_popover_open = false;
            cx.notify();
        });
    }
}

// ============================================================================
// TabContainer - Main container component
// ============================================================================

pub struct TabContainer {
    tabs: Vec<TabItem>,
    active_index: usize,
    size: Size,
    show_menu: bool,
    /// Optional background color for the tab bar (defaults to dark theme)
    tab_bar_bg_color: Option<gpui::Hsla>,
    /// Optional border color for the tab bar (defaults to dark theme)
    tab_bar_border_color: Option<gpui::Hsla>,
    /// Optional background color for active tab (defaults to dark theme)
    active_tab_bg_color: Option<gpui::Hsla>,
    /// Optional background color for inactive tab hover state (defaults to dark theme)
    inactive_tab_hover_color: Option<gpui::Hsla>,
    /// Optional background color for inactive tabs (defaults to dark theme)
    inactive_tab_bg_color: Option<gpui::Hsla>,
    /// Optional text color for tabs (defaults to white)
    tab_text_color: Option<gpui::Hsla>,
    /// Optional close button color (defaults to gray)
    tab_close_button_color: Option<gpui::Hsla>,
    /// Optional icon color for tabs (defaults to white)
    tab_icon_color: Option<gpui::Hsla>,
    /// Optional left padding for macOS traffic lights (defaults to 0)
    left_padding: Option<gpui::Pixels>,
    /// Optional top padding for vertical centering (defaults to 0)
    top_padding: Option<gpui::Pixels>,
    tab_bar_scroll_handle: ScrollHandle,
    /// Whether the tab list popover is open
    list_popover_open: bool,
    /// List state for the tab dropdown
    tab_list: Option<Entity<ListState<TabListDelegate>>>,
}

impl TabContainer {
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let _ = (window, cx);
        Self {
            tabs: Vec::new(),
            active_index: 0,
            size: Size::Large,
            show_menu: false,
            tab_bar_bg_color: None,
            tab_bar_border_color: None,
            active_tab_bg_color: None,
            inactive_tab_hover_color: None,
            inactive_tab_bg_color: None,
            tab_text_color: None,
            tab_close_button_color: None,
            tab_icon_color: None,
            left_padding: None,
            top_padding: None,
            tab_bar_scroll_handle: ScrollHandle::new(),
            list_popover_open: false,
            tab_list: None,
        }
    }

    pub fn with_tab_icon_color(mut self, color: impl Into<Option<gpui::Hsla>>) -> Self {
        self.tab_icon_color = color.into();
        self
    }
    pub fn with_inactive_tab_bg_color(mut self, color: impl Into<Option<gpui::Hsla>>) -> Self {
        self.inactive_tab_bg_color = color.into();
         self
    }

    /// Set custom tab bar colors (background and border)
    pub fn with_tab_bar_colors(
        mut self,
        bg_color: impl Into<Option<gpui::Hsla>>,
        border_color: impl Into<Option<gpui::Hsla>>,
    ) -> Self {
        self.tab_bar_bg_color = bg_color.into();
        self.tab_bar_border_color = border_color.into();
        self
    }

    /// Set custom tab item colors (active and hover)
    pub fn with_tab_item_colors(
        mut self,
        active_color: impl Into<Option<gpui::Hsla>>,
        hover_color: impl Into<Option<gpui::Hsla>>,
    ) -> Self {
        self.active_tab_bg_color = active_color.into();
        self.inactive_tab_hover_color = hover_color.into();
        self
    }

    /// Set custom tab text and close button colors
    pub fn with_tab_content_colors(
        mut self,
        text_color: impl Into<Option<gpui::Hsla>>,
        close_button_color: impl Into<Option<gpui::Hsla>>,
    ) -> Self {
        self.tab_text_color = text_color.into();
        self.tab_close_button_color = close_button_color.into();
        self
    }

    /// Set left padding for macOS traffic lights
    ///
    /// Use this to reserve space for the red/yellow/green buttons on macOS.
    /// Common values: px(80.0) for standard macOS window controls.
    ///
    /// # Example
    /// ```
    /// TabContainer::new(window, cx)
    ///     .with_left_padding(px(80.0))
    /// ```
    pub fn with_left_padding(mut self, padding: gpui::Pixels) -> Self {
        self.left_padding = Some(padding);
        self
    }

    /// Set top padding for vertical centering
    ///
    /// Use this to vertically center content when using custom window controls.
    /// Common values: px(4.0) for macOS traffic lights.
    ///
    /// # Example
    /// ```
    /// TabContainer::new(window, cx)
    ///     .with_top_padding(px(4.0))
    /// ```
    pub fn with_top_padding(mut self, padding: gpui::Pixels) -> Self {
        self.top_padding = Some(padding);
        self
    }

    /// Set tab bar background color
    pub fn set_tab_bar_bg_color(&mut self, color: impl Into<Option<gpui::Hsla>>, cx: &mut Context<Self>) {
        self.tab_bar_bg_color = color.into();
        cx.notify();
    }

    /// Set tab bar border color
    pub fn set_tab_bar_border_color(&mut self, color: impl Into<Option<gpui::Hsla>>, cx: &mut Context<Self>) {
        self.tab_bar_border_color = color.into();
        cx.notify();
    }

    /// Set active tab background color
    pub fn set_active_tab_bg_color(&mut self, color: impl Into<Option<gpui::Hsla>>, cx: &mut Context<Self>) {
        self.active_tab_bg_color = color.into();
        cx.notify();
    }

    /// Set inactive tab hover color
    pub fn set_inactive_tab_hover_color(&mut self, color: impl Into<Option<gpui::Hsla>>, cx: &mut Context<Self>) {
        self.inactive_tab_hover_color = color.into();
        cx.notify();
    }

    /// Add a new tab
    pub fn add_tab(&mut self, tab: TabItem, cx: &mut Context<Self>) {
        self.tabs.push(tab);
        cx.notify();
    }

    /// Add a new tab and activate it
    pub fn add_and_activate_tab(&mut self, tab: TabItem, cx: &mut Context<Self>) {
        self.tabs.push(tab);
        self.active_index = self.tabs.len() - 1;
        self.tab_bar_scroll_handle.scroll_to_item(self.tabs.len() - 1);
        cx.notify();
    }

    /// Activate existing tab by ID, or create and activate if not exists (lazy loading)
    /// The create_fn closure is only called if the tab doesn't exist
    /// The closure receives window and cx to avoid borrowing issues
    pub fn activate_or_add_tab_lazy<F>(&mut self, tab_id: impl Into<String>, create_fn: F, window: &mut Window, cx: &mut Context<Self>)
    where
        F: FnOnce(&mut Window, &mut Context<Self>) -> TabItem,
    {
        let tab_id = tab_id.into();

        // Check if tab already exists
        if let Some(index) = self.tabs.iter().position(|t| t.id() == tab_id) {
            // Tab exists, activate it without triggering callbacks
            if index < self.tabs.len() {
                self.tab_bar_scroll_handle.scroll_to_item(index);
                self.active_index = index;
                cx.notify();
            }
        } else {
            // Tab doesn't exist, create and activate it
            let tab = create_fn(window, cx);
            self.add_and_activate_tab(tab, cx);
        }
    }

    /// Close a tab by index
    pub fn close_tab(&mut self, index: usize, cx: &mut Context<Self>) {
        if index < self.tabs.len() && self.tabs[index].content().closeable() {
            self.tabs.remove(index);

            // Adjust active index if needed
            if self.active_index >= self.tabs.len() && !self.tabs.is_empty() {
                self.active_index = self.tabs.len() - 1;
            }

            cx.notify();
        }
    }

    /// Close all tabs except the one at the given index
    pub fn close_other_tabs(&mut self, keep_index: usize, cx: &mut Context<Self>) {
        if keep_index >= self.tabs.len() {
            return;
        }

        // Keep the tab at keep_index, remove all others
        let kept_tab = self.tabs.remove(keep_index);
        self.tabs.retain(|tab| !tab.content().closeable());
        self.tabs.insert(0, kept_tab);
        self.active_index = 0;

        cx.notify();
    }

    /// Close all tabs
    pub fn close_all_tabs(&mut self, cx: &mut Context<Self>) {
        self.tabs.retain(|tab| !tab.content().closeable());

        // Reset active index
        if self.active_index >= self.tabs.len() && !self.tabs.is_empty() {
            self.active_index = self.tabs.len() - 1;
        } else if self.tabs.is_empty() {
            self.active_index = 0;
        }

        cx.notify();
    }

    /// Close all tabs to the left of the given index
    pub fn close_tabs_to_left(&mut self, index: usize, cx: &mut Context<Self>) {
        if index == 0 || index >= self.tabs.len() {
            return;
        }

        // Remove closeable tabs from index-1 down to 0
        let mut i = 0;
        let mut removed_count = 0;
        while i < index {
            if self.tabs[i].content().closeable() {
                self.tabs.remove(i);
                removed_count += 1;
            }
            i += 1;
        }

        // Adjust active index
        if self.active_index >= removed_count {
            self.active_index -= removed_count;
        }

        cx.notify();
    }

    /// Close all tabs to the right of the given index
    pub fn close_tabs_to_right(&mut self, index: usize, cx: &mut Context<Self>) {
        if index >= self.tabs.len() - 1 {
            return;
        }

        // Remove closeable tabs from index+1 to end
        let mut i = index + 1;
        while i < self.tabs.len() {
            if self.tabs[i].content().closeable() {
                self.tabs.remove(i);
            }
            i += 1;
        }

        // Adjust active index if it was beyond the removed tabs
        if self.active_index > index && self.active_index >= self.tabs.len() {
            self.active_index = self.tabs.len() - 1;
        }

        cx.notify();
    }

    /// Close a tab by ID
    pub fn close_tab_by_id(&mut self, id: &str, cx: &mut Context<Self>) {
        if let Some(index) = self.tabs.iter().position(|t| t.id() == id) {
            self.close_tab(index, cx);
        }
    }

    /// Set the active tab by index
    pub fn set_active_index(&mut self, index: usize, window: &mut Window, cx: &mut Context<Self>) {
        if index < self.tabs.len() {
            // Deactivate old tab
            if let Some(old_tab) = self.tabs.get(self.active_index) {
                old_tab.content().on_deactivate(window, cx);
            }

            self.tab_bar_scroll_handle.scroll_to_item(index);

            self.active_index = index;

            // Activate new tab
            if let Some(new_tab) = self.tabs.get(self.active_index) {
                new_tab.content().on_activate(window, cx);
            }

            cx.notify();
        }
    }

    /// Set the active tab by ID
    pub fn set_active_by_id(&mut self, id: &str, window: &mut Window, cx: &mut Context<Self>) {
        if let Some(index) = self.tabs.iter().position(|t| t.id() == id) {
            self.set_active_index(index, window, cx);
        }
    }

    /// Get the active tab
    pub fn active_tab(&self) -> Option<&TabItem> {
        self.tabs.get(self.active_index)
    }

    /// Find tab by content type
    pub fn find_tab_by_type(&self, content_type: &TabContentType) -> Option<&TabItem> {
        self.tabs
            .iter()
            .find(|t| &t.content().content_type() == content_type)
    }

    /// Check if a tab with the given type exists
    pub fn has_tab_type(&self, content_type: &TabContentType) -> bool {
        self.find_tab_by_type(content_type).is_some()
    }

    /// Set tab bar size
    pub fn set_size(&mut self, size: Size, cx: &mut Context<Self>) {
        self.size = size;
        cx.notify();
    }

    /// Set whether to show more menu
    pub fn set_show_menu(&mut self, show: bool, cx: &mut Context<Self>) {
        self.show_menu = show;
        cx.notify();
    }

    /// Get all tabs
    pub fn tabs(&self) -> &[TabItem] {
        &self.tabs
    }

    /// Get active index
    pub fn active_index(&self) -> usize {
        self.active_index
    }

    /// Move tab from one position to another
    pub fn move_tab(&mut self, from_index: usize, to_index: usize, cx: &mut Context<Self>) {
        if from_index < self.tabs.len() && to_index < self.tabs.len() && from_index != to_index {
            let tab = self.tabs.remove(from_index);
            self.tabs.insert(to_index, tab);

            // Adjust active index if needed
            if self.active_index == from_index {
                self.active_index = to_index;
            } else if from_index < self.active_index && to_index >= self.active_index {
                self.active_index -= 1;
            } else if from_index > self.active_index && to_index <= self.active_index {
                self.active_index += 1;
            }

            cx.notify();
        }
    }

    /// Get the width for a specific tab
    /// Priority: tab's own width_size() > container's default size
    fn get_tab_width(&self, tab: &TabItem) -> gpui::Pixels {
        let size = tab.content().width_size().unwrap_or(self.size);
        self.size_to_pixels(size)
    }

    /// Convert Size enum to pixel width
    fn size_to_pixels(&self, size: Size) -> gpui::Pixels {
        match size {
            Size::Size(pixels) => pixels,  // 自定义像素值
            Size::XSmall => px(60.0),
            Size::Small => px(100.0),
            Size::Medium => px(140.0),
            Size::Large => px(180.0),
        }
    }

    pub fn render_tab_content(&self, window: &mut Window, cx: &mut App) -> impl IntoElement {
        // Active tab content
        div()
            .flex_1()
            .w_full()
            .overflow_hidden()
            .when_some(self.active_tab(), |el, tab| {
                el.child(tab.content().render_content(window, cx))
            })
    }

    pub fn render_tab_bar(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let view = cx.entity();

        // 使用自定义颜色或从主题动态读取
        let theme = cx.theme();
        let bg_color = self.tab_bar_bg_color.unwrap_or(theme.tab);
        let border_color = self.tab_bar_border_color.unwrap_or(theme.border);
        let active_tab_color = self.active_tab_bg_color.unwrap_or(theme.tab_active);
        let hover_tab_color = self.inactive_tab_hover_color.unwrap_or(theme.tab.opacity(0.8));
        let inactive_tab_color = self.inactive_tab_bg_color.unwrap_or(theme.tab.opacity(0.5));
        let text_color = self.tab_text_color.unwrap_or(theme.tab_foreground);
        let close_btn_color = self.tab_close_button_color.unwrap_or(theme.muted_foreground);
        let drag_border_color = theme.drag_border;
        let icon_color = self.tab_icon_color.unwrap_or(theme.tab_foreground);
        let active_index = self.active_index;

        let tab_list = self.tab_list.clone();

        h_flex()
            .w_full()
            .h(px(40.0))
            .bg(bg_color)
            .items_center()
            .border_b_1()
            .border_color(border_color)
            .child(
                // 标签滚动容器 - 使用 scrollable 实现水平滚动
                h_flex()
                    .id("tabs")
                    .flex_1()
                    .overflow_x_scroll()
                    .pl(self.left_padding.unwrap_or(px(8.0)))
                    .when_some(self.top_padding, |div, padding| div.pt(padding))
                    .pr_2()
                    .gap_1()
                    .track_scroll(&self.tab_bar_scroll_handle)
                    .children(self.tabs.iter().enumerate().map(|(idx, tab)| {
                        let title = tab.content().title();
                        let icon = tab.content().icon();
                        let closeable = tab.content().closeable();
                        let is_active = idx == active_index;
                        let view_clone = view.clone();
                        let title_clone = title.clone();
                        let tab_width = self.get_tab_width(tab);

                        div()
                            .id(idx)
                            .flex()
                            .flex_shrink_0()
                            .overflow_hidden()
                            .items_center()
                            .gap_2()
                            .h(px(32.0))
                            .w(tab_width)
                            .px_3()
                            .rounded(px(6.0))
                            .cursor_grab()
                            .when(is_active, |el| el.bg(active_tab_color))
                            .when(!is_active, |el| el.hover(move |style| style.bg(hover_tab_color)).bg(inactive_tab_color))
                            .on_drag(
                                DragTab::new(idx, title.clone()),
                                |drag, _, _, cx| {
                                    cx.stop_propagation();
                                    cx.new(|_| drag.clone())
                                },
                            )
                            .drag_over::<DragTab>(move |el, _, _, _cx| {
                                el.border_l_2()
                                    .border_color(drag_border_color)
                            })
                            .on_drop(cx.listener(move |this, drag: &DragTab, window, cx| {
                                let from_idx = drag.tab_index;
                                let to_idx = idx;
                                if from_idx != to_idx {
                                    this.move_tab(from_idx, to_idx, cx);
                                }
                                this.set_active_index(to_idx, window, cx);
                            }))
                            .on_click(cx.listener(move |this, _event, window, cx| {
                                this.set_active_index(idx, window, cx);
                            }))
                            .when_some(icon, |el, icon| {
                                el.child(
                                    div()
                                        .flex_shrink_0()
                                        .flex()
                                        .items_center()
                                        .text_color(icon_color)
                                        .child(icon)
                                )
                            })
                            .child(
                                div()
                                    .flex_1()
                                    .overflow_hidden()
                                    .text_sm()
                                    .text_color(text_color)
                                    .text_ellipsis()
                                    .child(title_clone.to_string())
                            )
                            .when(closeable, |el| {
                                let view_clone = view_clone.clone();
                                el.child(
                                    div()
                                        .flex_shrink_0()
                                        .w(px(16.0))
                                        .h(px(16.0))
                                        .flex()
                                        .items_center()
                                        .justify_center()
                                        .rounded(px(2.0))
                                        .cursor_pointer()
                                        .text_color(close_btn_color)
                                        .hover(|style| {
                                            style
                                                .bg(gpui::rgb(0x5a5a5a))
                                                .text_color(text_color)
                                        })
                                        .on_mouse_down(MouseButton::Left, move |_event, _window, cx| {
                                            view_clone.update(cx, |this, cx| {
                                                this.close_tab(idx, cx);
                                            });
                                        })
                                        .child("×")
                                )
                            }).context_menu(move |menu, window, cx| {
                            let view_for_menu = view_clone.clone();
                            let tab_count = view_for_menu.read(cx).tabs.len();
                            let has_tabs_left = idx > 0;
                            let has_tabs_right = idx < tab_count - 1;
                            let closeable = view_for_menu.read(cx).tabs.get(idx)
                                .map(|tab| tab.content().closeable())
                                .unwrap_or(false);

                            menu
                                .item(PopupMenuItem::new("Close")
                                    .disabled(!closeable)
                                    .on_click(window.listener_for(&view_for_menu, move |this, _, _, cx| {
                                        this.close_tab(idx, cx);
                                    })))
                                .item(PopupMenuItem::new("Close All")
                                    .on_click(window.listener_for(&view_for_menu, move |this, _, _, cx| {
                                        this.close_all_tabs(cx);
                                    })))
                                .item(PopupMenuItem::new("Close Others")
                                    .disabled(tab_count <= 1)
                                    .on_click(window.listener_for(&view_for_menu, move |this, _, _, cx| {
                                        this.close_other_tabs(idx, cx);
                                    })))
                                .item(PopupMenuItem::new("Close Tabs To The Left")
                                    .disabled(!has_tabs_left)
                                    .on_click(window.listener_for(&view_for_menu, move |this, _, _, cx| {
                                        this.close_tabs_to_left(idx, cx);
                                    })))
                                .item(PopupMenuItem::new("Close Tabs To The Right")
                                    .disabled(!has_tabs_right)
                                    .on_click(window.listener_for(&view_for_menu, move |this, _, _, cx| {
                                        this.close_tabs_to_right(idx, cx);
                                    })))

                        })
                    }))
            )
            .child(
                Popover::new("tab-list-popover")
                    .anchor(Corner::TopRight)
                    .p_0()
                    .open(self.list_popover_open)
                    .on_open_change(cx.listener(move |this, open, window, cx| {
                        this.list_popover_open = *open;
                        if *open {
                            let tabs_data: Vec<(usize, SharedString, Option<Icon>, bool)> = this.tabs
                                .iter()
                                .enumerate()
                                .map(|(idx, tab)| (idx, tab.content().title(), tab.content().icon(), tab.content().closeable()))
                                .collect();
                            let container = cx.entity();

                            if let Some(tab_list) = &this.tab_list {
                                tab_list.update(cx, |state, _| {
                                    let delegate = state.delegate_mut();
                                    delegate.tabs = tabs_data.clone();
                                    delegate.filtered_tabs = tabs_data;
                                });
                            } else {
                                this.tab_list = Some(cx.new(|cx| {
                                    ListState::new(
                                        TabListDelegate {
                                            container,
                                            tabs: tabs_data.clone(),
                                            filtered_tabs: tabs_data,
                                            selected_index: None,
                                        },
                                        window,
                                        cx,
                                    ).searchable(true)
                                }));
                            }
                        }
                        cx.notify();
                    }))
                    .when_some(tab_list.as_ref(), |popover, list| {
                        popover.track_focus(&list.focus_handle(cx))
                    })
                    .trigger(
                        Button::new("tab-dropdown-btn")
                            .icon(IconName::ChevronDown)
                            .ghost()
                            .compact()
                    )
                    .when_some(tab_list, |popover, list| {
                        popover.child(
                            List::new(&list)
                                .w(px(280.0))
                                .max_h(px(300.0))
                        )
                    })
            )
    }
}

impl Render for TabContainer {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        // 渲染标签栏和内容
        div()
            .relative()
            .size_full()
            .child(
                v_flex()
                    .size_full()
                    .child(
                        // Tab bar
                        self.render_tab_bar(window, cx)
                    )
                    .child(
                        // Tab content
                        self.render_tab_content(window, cx)
                    )
            )
    }
}
