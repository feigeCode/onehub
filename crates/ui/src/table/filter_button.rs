// 1. 外部 crate 导入
use gpui::{div, AnyElement, App, ClickEvent, IntoElement, ParentElement, Window};

// 2. 当前 crate 导入
use crate::{
    button::{Button, ButtonVariants},
    IconName, Sizable, Size,
};

// ============================================================================
// Filter Button - Column header filter button component
// ============================================================================

/// 筛选按钮组件
/// 
/// 显示在表头的筛选图标按钮，用于打开筛选面板
#[derive(Clone, Debug)]
pub struct FilterButton {
    /// 列索引
    col_ix: usize,
    /// 是否激活（有筛选条件）
    is_active: bool,
    /// 是否禁用（无数据时）
    is_disabled: bool,
    /// 是否正在加载
    is_loading: bool,
}

impl FilterButton {
    /// 创建新的筛选按钮
    pub fn new(col_ix: usize, is_active: bool) -> Self {
        Self {
            col_ix,
            is_active,
            is_disabled: false,
            is_loading: false,
        }
    }

    /// 设置禁用状态
    pub fn disabled(mut self, disabled: bool) -> Self {
        self.is_disabled = disabled;
        self
    }

    /// 设置加载状态
    pub fn loading(mut self, loading: bool) -> Self {
        self.is_loading = loading;
        self
    }

    /// 渲染筛选按钮
    /// 
    /// 根据状态显示不同的视觉效果：
    /// - 加载状态：显示加载图标
    /// - 激活状态：高亮显示
    /// - 禁用状态：灰色显示
    /// - 正常状态：默认样式
    pub fn render<F>(
        &self,
        on_click: F,
        _window: &mut Window,
        _cx: &mut App,
    ) -> AnyElement
    where
        F: Fn(&ClickEvent, &mut Window, &mut App) + 'static,
    {
        // 使用元组创建唯一的 button ID
        let icon = if self.is_loading {
            IconName::LoaderCircle
        } else {
            IconName::Settings
        };

        let mut button = Button::new(("filter-button", self.col_ix))
            .icon(icon)
            .with_size(Size::XSmall)
            .ghost();

        // 激活状态：使用 primary 样式高亮显示
        if self.is_active {
            button = button.primary();
        }

        // 只有在非禁用且非加载状态下才添加点击事件
        if !self.is_disabled && !self.is_loading {
            button = button.on_click(on_click);
        }

        div()
            .child(button)
            .into_any_element()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filter_button_new() {
        let button = FilterButton::new(0, false);
        assert_eq!(button.col_ix, 0);
        assert!(!button.is_active);
        assert!(!button.is_disabled);
    }

    #[test]
    fn test_filter_button_active() {
        let button = FilterButton::new(0, true);
        assert!(button.is_active);
    }

    #[test]
    fn test_filter_button_disabled() {
        let button = FilterButton::new(0, false).disabled(true);
        assert!(button.is_disabled);
    }

    #[test]
    fn test_filter_button_col_ix() {
        let button = FilterButton::new(5, false);
        assert_eq!(button.col_ix, 5);
    }

    #[test]
    fn test_filter_button_loading() {
        let button = FilterButton::new(0, false).loading(true);
        assert!(button.is_loading);
    }

    #[test]
    fn test_filter_button_loading_and_active() {
        let button = FilterButton::new(0, true).loading(true);
        assert!(button.is_active);
        assert!(button.is_loading);
    }
}
