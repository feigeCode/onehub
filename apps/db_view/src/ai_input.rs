//! AI Input - 支持模型选择和智能输入的组件

use gpui::prelude::FluentBuilder;
use gpui::{
    div, App, AppContext, Context, Entity, EventEmitter, FocusHandle, Focusable, IntoElement,
    ParentElement, Render, SharedString, Styled, Subscription, Window,
};
use gpui_component::{
    button::{Button, ButtonVariants},
    h_flex,
    input::{Input, InputEvent, InputState},
    select::{Select, SelectItem, SelectState},
    v_flex, ActiveTheme, IconName, IndexPath, Sizable, Size,
};
use one_core::llm::types::ProviderConfig;

/// Provider 配置项用于选择
#[derive(Clone, Debug)]
pub struct ProviderItem {
    pub id: String,
    pub name: String,
    pub model: String,
    pub provider_type: String,
}

impl ProviderItem {
    pub fn new(
        id: impl Into<String>,
        name: impl Into<String>,
        model: impl Into<String>,
        provider_type: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
            model: model.into(),
            provider_type: provider_type.into(),
        }
    }

    pub fn from_config(config: &ProviderConfig) -> Self {
        Self {
            id: config.id.to_string(),
            name: config.name.clone(),
            model: config.model.clone(),
            provider_type: config.provider_type.display_name().to_string(),
        }
    }

    pub fn display_name(&self) -> String {
        format!("{} - {} ({})", self.provider_type, self.model, self.name)
    }
}

impl SelectItem for ProviderItem {
    type Value = String;

    fn title(&self) -> SharedString {
        self.display_name().into()
    }

    fn value(&self) -> &Self::Value {
        &self.id
    }
}

/// AI 输入框事件
#[derive(Clone, Debug)]
pub enum AIInputEvent {
    Submit { content: String },
    ProviderChanged { provider_id: String },
}

/// AI 输入框组件
pub struct AIInput {
    focus_handle: FocusHandle,
    input_state: Entity<InputState>,
    provider_select: Entity<SelectState<Vec<ProviderItem>>>,
    _provider_subscription: Subscription,
    _input_subscription: Subscription,
    providers: Vec<ProviderItem>,
    selected_provider: Option<String>,
    is_loading: bool,
    connection_name: Option<String>,
    database: Option<String>,
}

impl AIInput {
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let focus_handle = cx.focus_handle();
        let input_state = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder("向数据库提问... (Enter 发送 · Shift+Enter 换行)")
                .auto_grow(2, 6)
                .default_value("")
        });

        let mut providers = Vec::new();
        providers.push(ProviderItem::new("default", "请选择模型", "-", ""));

        let provider_select = cx.new(|cx| {
            SelectState::new(providers.clone(), Some(IndexPath::new(0)), window, cx)
        });

        let provider_subscription = cx
            .subscribe_in(&provider_select, window, |this, _select, event, _window, cx| {
                if let gpui_component::select::SelectEvent::Confirm(Some(provider_id)) = event {
                    this.selected_provider = Some(provider_id.clone());
                    cx.emit(AIInputEvent::ProviderChanged {
                        provider_id: provider_id.clone(),
                    });
                }
            });

        let input_subscription = cx.subscribe_in(&input_state, window, |this, state, event, window, cx| {
            match event {
                InputEvent::PressEnter { secondary } => {
                    if !secondary {
                        this.submit(window, cx);
                    }
                }
                InputEvent::Change => {
                    let empty = state.read(cx).value().trim().is_empty();
                    if empty && this.is_loading {
                        this.set_loading(false, window, cx);
                    }
                }
                _ => {}
            }
        });

        Self {
            focus_handle,
            input_state,
            provider_select,
            _provider_subscription: provider_subscription,
            _input_subscription:  input_subscription,
            providers,
            selected_provider: None,
            is_loading: false,
            connection_name: None,
            database: None,
        }
    }

    pub fn set_context(
        &mut self,
        connection_name: Option<String>,
        database: Option<String>,
        cx: &mut Context<Self>,
    ) {
        self.connection_name = connection_name;
        self.database = database;
        cx.notify();
    }

    pub fn update_providers(
        &mut self,
        providers: Vec<ProviderItem>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if providers.is_empty() {
            self.providers.clear();
        } else {
            self.providers = providers;
        }
        self.rebuild_provider_select(window, cx);
    }

    fn submit(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let content = self.input_state.read(cx).value().to_string();
        if content.trim().is_empty() {
            return;
        }

        cx.emit(AIInputEvent::Submit { content });

        self.input_state.update(cx, |state, cx| {
            state.set_value("", window, cx);
        });
    }

    pub fn set_selected_provider(
        &mut self,
        provider_id: Option<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.selected_provider = provider_id;
        self.apply_selected_provider(window, cx);
    }

    pub fn set_loading(&mut self, loading: bool, _window: &mut Window, cx: &mut Context<Self>) {
        if self.is_loading == loading {
            return;
        }
        self.is_loading = loading;
        cx.notify();
    }

    fn rebuild_provider_select(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let mut providers = self.providers.clone();
        if providers.is_empty() {
            providers.push(ProviderItem::new("default", "暂无可用模型", "-", ""));
        }

        let selected = self.selected_provider.clone();
        self.provider_select.update(cx, |state, cx| {
            state.set_items(providers.clone(), window, cx);
            if let Some(provider_id) = selected {
                state.set_selected_value(&provider_id, window, cx);
            } else {
                state.set_selected_index(Some(IndexPath::new(0)), window, cx);
            }
        });
    }

    fn apply_selected_provider(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let selected = self.selected_provider.clone();
        self.provider_select.update(cx, |state, cx| {
            if let Some(provider_id) = selected {
                state.set_selected_value(&provider_id, window, cx);
            } else if !self.providers.is_empty() {
                state.set_selected_index(Some(IndexPath::new(0)), window, cx);
            }
        });
    }
}

impl EventEmitter<AIInputEvent> for AIInput {}

impl Focusable for AIInput {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl Render for AIInput {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        v_flex()
            .w_full()
            .bg(cx.theme().background)
            .rounded_lg()
            .border_1()
            .border_color(cx.theme().border)
            .shadow_sm()
            // 数据库连接和表信息
            .child(
                h_flex()
                    .w_full()
                    .px_3()
                    .py_2()
                    .items_center()
                    .gap_2()
                    .border_b_1()
                    .border_color(cx.theme().border)
                    .bg(cx.theme().muted.opacity(0.3))
                    .child(
                        h_flex()
                            .items_center()
                            .gap_1()
                            .child(
                                div()
                                    .text_xs()
                                    .text_color(cx.theme().muted_foreground)
                                    .child("连接:")
                            )
                            .child(
                                div()
                                    .text_sm()
                                    .font_weight(gpui::FontWeight::MEDIUM)
                                    .child(self.connection_name.clone().unwrap_or_else(|| "未选择".to_string()))
                            )
                    )
                    .when_some(self.database.clone(), |this, db| {
                        this
                            .child(
                                div()
                                    .text_xs()
                                    .text_color(cx.theme().muted_foreground)
                                    .child("/")
                            )
                            .child(
                                h_flex()
                                    .items_center()
                                    .gap_1()
                                    .child(
                                        div()
                                            .text_xs()
                                            .text_color(cx.theme().muted_foreground)
                                            .child("库:")
                                    )
                                    .child(
                                        div()
                                            .text_sm()
                                            .font_weight(gpui::FontWeight::MEDIUM)
                                            .child(db)
                                    )
                            )
                    })
            )
            // 输入框
            .child(
                div()
                    .w_full()
                    .px_3()
                    .pt_3()
                    .pb_2()
                    .child(
                        Input::new(&self.input_state)
                            .w_full()
                            .with_size(Size::Large)
                            .bordered(false)
                            .appearance(false)
                            .bg(cx.theme().muted)
                            .rounded(cx.theme().radius)
                            // .px_3()
                            // .py_3()
                    )
            )
            // 底部：模型选择和发送按钮
            .child(
                h_flex()
                    .w_full()
                    .items_center()
                    .justify_between()
                    .px_3()
                    .pb_3()
                    .gap_2()
                    .child(
                        div()
                            .flex_1()
                            .min_w_0()
                            .overflow_hidden()
                            .child(
                                Select::new(&self.provider_select)
                                    .with_size(Size::Small)
                                    .placeholder("选择模型")
                            )
                    )
                    .child(
                        Button::new("send")
                            .with_size(Size::Small)
                            .primary()
                            .icon(IconName::ArrowRight)
                            .label("发送")
                            .loading(self.is_loading)
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.submit(window, cx);
                            })),
                    ),
            )
    }
}
