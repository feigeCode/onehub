use gpui::{div, px, App, AppContext, Context, Entity, IntoElement, InteractiveElement, KeyBinding, MouseDownEvent, MouseMoveEvent, MouseUpEvent, ParentElement, Point, Render, Styled, Window, Focusable};
use gpui_component::dock::{ClosePanel, ToggleZoom};
use gpui_component::{ActiveTheme, Root, button::Button, IconName, Sizable, WindowExt, h_flex};
use gpui_component::button::ButtonVariants;
use one_core::tab_container::{TabContainer, TabItem};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use reqwest_client::ReqwestClient;
use crate::home::HomeTabContent;
use db_view::ai_chat_panel::AiChatPanel;

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
    ai_button_y: gpui::Pixels,
    ai_button_dragging: bool,
    drag_start_pos: Option<Point<gpui::Pixels>>,
    drag_start_button_y: Option<gpui::Pixels>,
    ai_panel: Entity<AiChatPanel>,
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

        let ai_panel = cx.new(|cx| AiChatPanel::new(window, cx));

        Self {
            tab_container,
            ai_button_y: px(500.0),  // 默认位置
            ai_button_dragging: false,
            drag_start_pos: None,
            drag_start_button_y: None,
            ai_panel,
        }
    }

    fn toggle_ai_panel(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let ai_panel = self.ai_panel.clone();
        window.open_sheet(cx, move |sheet, _window, cx| {
            let ai_panel_for_new = ai_panel.clone();
            let ai_panel_for_history = ai_panel.clone();
            let ai_panel_for_content = ai_panel.clone();
            ai_panel.focus_handle(cx);

            sheet
                .overlay(false)
                .title(
                    h_flex()
                        .w_full()
                        .items_center()
                        .justify_between()
                        .child(
                            div()
                                .text_base()
                                .font_weight(gpui::FontWeight::SEMIBOLD)
                                .child("AI 助手")
                        )
                        .child(
                            h_flex()
                                .gap_1()
                                .child(
                                    Button::new("new-chat")
                                        .icon(IconName::Plus)
                                        .small()
                                        .ghost()
                                        .tooltip("新建对话")
                                        .on_click(move |_, _, cx| {
                                            ai_panel_for_new.update(cx, |panel, cx| {
                                                panel.start_new_session(cx);
                                            });
                                        })
                                )
                                .child(
                                    Button::new("history")
                                        .icon(IconName::Menu)
                                        .small()
                                        .ghost()
                                        .tooltip("聊天历史")
                                        .on_click(move |_, window, cx| {
                                            ai_panel_for_history.update(cx, |panel, cx| {
                                                panel.toggle_history_popover(window, cx);
                                            });
                                        })
                                )
                        )
                )
                .child(ai_panel_for_content)
        });
    }
}

impl Render for OneHupApp {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let sheet_layer = Root::render_sheet_layer(window, cx);
        let dialog_layer = Root::render_dialog_layer(window, cx);
        let notification_layer = Root::render_notification_layer(window, cx);

        div()
            .size_full()
            .relative()
            .bg(cx.theme().background)
            .child(
                div()
                    .size_full()
                    .child(self.tab_container.clone())
            )
            // 可拖动的全局 AI 按钮
            .child(
                div()
                    .absolute()
                    .right_4()
                    .top(self.ai_button_y)
                    .child(
                        Button::new("global-ai-button")
                            .icon(IconName::Bot)
                            .tooltip("AI 助手 (拖动调整位置)")
                            .large()
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.toggle_ai_panel(window, cx);
                            }))
                            .on_mouse_down(gpui::MouseButton::Left, cx.listener(|this, event: &MouseDownEvent, _, cx| {
                                this.ai_button_dragging = true;
                                this.drag_start_pos = Some(event.position);
                                this.drag_start_button_y = Some(this.ai_button_y);
                                cx.notify();
                            }))
                            .on_mouse_up(gpui::MouseButton::Left, cx.listener(|this, _event: &MouseUpEvent, _, cx| {
                                this.ai_button_dragging = false;
                                this.drag_start_pos = None;
                                this.drag_start_button_y = None;
                                cx.notify();
                            }))
                    )
                    .on_mouse_move(cx.listener(|this, event: &MouseMoveEvent, _window, cx| {
                        if this.ai_button_dragging {
                            if let (Some(start_pos), Some(start_y)) = (this.drag_start_pos, this.drag_start_button_y) {
                                let delta_y = event.position.y - start_pos.y;
                                let new_y = start_y + delta_y;

                                // 边界检查：确保按钮在窗口范围内
                                let min_y = px(0.0);
                                let max_y = px(800.0); // 简单边界

                                this.ai_button_y = new_y.max(min_y).min(max_y);
                                cx.notify();
                            }
                        }
                    }))
            )
            .children(sheet_layer)
            .children(dialog_layer)
            .children(notification_layer)
    }
}
