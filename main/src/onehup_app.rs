use gpui::{div, px, App, AppContext, Context, Entity, IntoElement, KeyBinding, ParentElement, Render, Styled, Window};
use gpui_component::dock::{ClosePanel, ToggleZoom};
use gpui_component::{ActiveTheme, Root};
use one_core::tab_container::{TabContainer, TabItem};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use reqwest_client::ReqwestClient;
use crate::home::HomeTabContent;

/// Initialize all LLM provider factories
fn init_providers() {
    provider_deepseek::init();
    provider_openai::init();
    provider_claude::init();
    provider_qwen::init();
    provider_ollama::init();
}

pub fn init(cx: &mut App) {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("gpui_component=trace".parse().expect("固定的日志指令解析不应失败")),
        )
        .init();
    let http_client = std::sync::Arc::new(
        ReqwestClient::user_agent("one-hub").expect("HTTP 客户端初始化失败"),
    );
    cx.set_http_client(http_client);
    gpui_component::init(cx);
    one_core::init(cx);
    cx.bind_keys(vec![
        KeyBinding::new("shift-escape", ToggleZoom, None),
        KeyBinding::new("ctrl-w", ClosePanel, None),
    ]);
    init_providers();
    cx.activate(true);
}

pub struct OneHupApp {
    tab_container: Entity<TabContainer>,
}

impl OneHupApp {
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        // 创建标签容器，根据平台设置 padding
        // 使用深色标签栏配色方案
        let tab_container = cx.new(|cx| {
            let mut container = TabContainer::new(window, cx)
                .with_tab_bar_colors(
                    Some(gpui::rgb(0x2b2b2b).into()),
                    Some(gpui::rgb(0x1e1e1e).into()),
                )
                .with_tab_item_colors(
                    Some(gpui::rgb(0x555555).into()),
                    Some(gpui::rgb(0x3a3a3a).into()),
                )
                .with_inactive_tab_bg_color(Some(gpui::rgb(0x3a3a3a).into()))
                .with_tab_content_colors(
                    Some(gpui::white()),
                    Some(gpui::rgb(0xaaaaaa).into()),
                )
                .with_tab_icon_color(Some(gpui::rgb(0xaaaaaa).into()));
            
            // macOS: 为红黄绿按钮留出空间并垂直居中
            #[cfg(target_os = "macos")]
            {
                container = container
                    .with_left_padding(px(80.0))
                    .with_top_padding(px(4.0))
            }
            
            container
        });

        // 添加主页标签
        tab_container.update(cx, |tc, cx| {
            let home_tab = TabItem::new("home", HomeTabContent::new(tab_container.clone(), window, cx));
            tc.add_and_activate_tab(home_tab, cx);
        });

        Self {
            tab_container,
        }
    }
}

impl Render for OneHupApp {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let sheet_layer = Root::render_sheet_layer(window, cx);
        let dialog_layer = Root::render_dialog_layer(window, cx);
        let notification_layer = Root::render_notification_layer(window, cx);
        div()
            .size_full()
            .bg(cx.theme().background)
            .child(self.tab_container.clone())
            .children(sheet_layer)
            .children(dialog_layer)
            .children(notification_layer)
    }
}
