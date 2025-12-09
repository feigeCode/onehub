//! AI Input - 支持模型选择和智能输入的组件

use gpui::{
    div, App, AppContext, Context, Entity, EventEmitter, FocusHandle, Focusable, IntoElement,
    ParentElement, Render, SharedString, Styled, Window,
};
use gpui_component::{
    button::Button, h_flex, input::{Input, InputState}, select::{Select, SelectItem, SelectState},
    v_flex, ActiveTheme, IconName, IndexPath,
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
    providers: Vec<ProviderItem>,
    needs_rebuild: bool,
}

impl AIInput {
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let focus_handle = cx.focus_handle();
        let input_state = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder("Ask anything about your database...")
                .auto_grow(1, 5)
                .default_value("")
        });

        let providers = vec![ProviderItem::new(
            "default",
            "No Provider Selected",
            "N/A",
            "None",
        )];
        let provider_select = cx.new(|cx| {
            SelectState::new(providers.clone(), Some(IndexPath::new(0)), window, cx)
        });

        Self {
            focus_handle,
            input_state,
            provider_select,
            providers,
            needs_rebuild: false,
        }
    }

    pub fn update_providers(&mut self, providers: Vec<ProviderItem>, cx: &mut Context<Self>) {
        if !providers.is_empty() {
            self.providers = providers;
            self.needs_rebuild = true;
            cx.notify();
        }
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
}

impl EventEmitter<AIInputEvent> for AIInput {}

impl Focusable for AIInput {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl Render for AIInput {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if self.needs_rebuild {
            self.provider_select = cx.new(|cx| {
                SelectState::new(
                    self.providers.clone(),
                    Some(IndexPath::new(0)),
                    window,
                    cx,
                )
            });

            cx.subscribe_in(
                &self.provider_select,
                window,
                |_this, _select, event: &gpui_component::select::SelectEvent<Vec<ProviderItem>>, _window, cx| {
                    if let gpui_component::select::SelectEvent::Confirm(Some(provider_id)) = event {
                        cx.emit(AIInputEvent::ProviderChanged {
                            provider_id: provider_id.clone(),
                        });
                    }
                },
            )
            .detach();

            self.needs_rebuild = false;
        }

        v_flex()
            .w_full()
            .border_1()
            .border_color(cx.theme().border)
            .rounded_lg()
            .bg(cx.theme().background)
            .child(h_flex().w_full().px_2().pt_2().child(Select::new(&self.provider_select)))
            .child(div().w_full().px_2().child(Input::new(&self.input_state)))
            .child(
                h_flex()
                    .w_full()
                    .justify_between()
                    .items_center()
                    .px_2()
                    .pb_2()
                    .child(div())
                    .child(
                        Button::new("send")
                            .icon(IconName::ArrowRight)
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.submit(window, cx);
                            })),
                    ),
            )
    }
}
