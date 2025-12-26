//! Provider Form Dialog - 添加/编辑 LLM Provider 的表单对话框

use gpui::{div, App, AppContext, Context, Entity, FocusHandle, Focusable, IntoElement, ParentElement, Render, SharedString, Styled, Window};
use gpui_component::{
    v_flex,
    input::{Input, InputState},
    select::{Select, SelectItem, SelectState},
    IndexPath,
};
use one_core::llm::types::{ProviderConfig, ProviderType};

/// Provider 类型选择项
#[derive(Clone, Debug)]
pub struct ProviderTypeItem {
    pub provider_type: ProviderType,
}

impl ProviderTypeItem {
    pub fn new(provider_type: ProviderType) -> Self {
        Self { provider_type }
    }
}

impl SelectItem for ProviderTypeItem {
    type Value = ProviderType;

    fn title(&self) -> SharedString {
        self.provider_type.display_name().into()
    }

    fn value(&self) -> &Self::Value {
        &self.provider_type
    }
}


/// Provider 表单对话框
pub struct ProviderForm {
    focus_handle: FocusHandle,
    provider_id: Option<i64>,
    name_input: Entity<InputState>,
    provider_type_select: Entity<SelectState<Vec<ProviderTypeItem>>>,
    api_key_input: Entity<InputState>,
    api_base_input: Entity<InputState>,
    model_input: Entity<InputState>,
}

impl ProviderForm {

    pub fn new_with_config(
        config: Option<ProviderConfig>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let focus_handle = cx.focus_handle();

        // 创建 provider 类型选择器
        let provider_types = vec![
            ProviderTypeItem::new(ProviderType::OpenAI),
            ProviderTypeItem::new(ProviderType::Claude),
            ProviderTypeItem::new(ProviderType::DeepSeek),
            ProviderTypeItem::new(ProviderType::Ollama),
            ProviderTypeItem::new(ProviderType::Qwen),
        ];

        let selected_index = if let Some(ref cfg) = config {
            provider_types
                .iter()
                .position(|item| item.provider_type == cfg.provider_type)
                .map(|i| IndexPath::new(i))
        } else {
            Some(IndexPath::new(0))
        };

        let provider_type_select = cx.new(|cx| {
            SelectState::new(provider_types, selected_index, window, cx)
        });

        // 创建输入框
        let name_input = cx.new(|cx| {
            let mut state = InputState::new(window, cx).placeholder("Provider Name");
            if let Some(ref cfg) = config {
                state = state.default_value(&cfg.name);
            }
            state
        });

        let api_key_input = cx.new(|cx| {
            let mut state = InputState::new(window, cx)
                .placeholder("API Key");
            if let Some(ref cfg) = config {
                if let Some(ref key) = cfg.api_key {
                    state = state.default_value(key);
                }
            }
            state
        });

        let api_base_input = cx.new(|cx| {
            let mut state = InputState::new(window, cx).placeholder("API Base URL (optional)");
            if let Some(ref cfg) = config {
                if let Some(ref base) = cfg.api_base {
                    state = state.default_value(base);
                }
            }
            state
        });

        let model_input = cx.new(|cx| {
            let mut state = InputState::new(window, cx).placeholder("Model Name");
            if let Some(ref cfg) = config {
                state = state.default_value(&cfg.model);
            }
            state
        });

        Self {
            focus_handle,
            provider_id: config.map(|c| c.id),
            name_input,
            provider_type_select,
            api_key_input,
            api_base_input,
            model_input,
        }
    }

    pub fn get_config(&mut self, cx: &mut Context<Self>) -> Option<ProviderConfig>{
        let name = self.name_input.read(cx).value().to_string();
        let provider_type = self
            .provider_type_select
            .read(cx)
            .selected_value()
            .cloned()
            .unwrap_or(ProviderType::OpenAI);
        let api_key = self.api_key_input.read(cx).value().to_string();
        let api_base = self.api_base_input.read(cx).value().to_string();
        let model = self.model_input.read(cx).value().to_string();

        if name.trim().is_empty() {
            tracing::warn!("Provider name is required");
            return None;
        }

        if model.trim().is_empty() {
            tracing::warn!("Model name is required");
            return None;
        }

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("系统时间不应早于 UNIX 纪元")
            .as_secs() as i64;

        Some(ProviderConfig {
            id: self.provider_id.unwrap_or(now),
            name,
            provider_type,
            api_key: if api_key.is_empty() { None } else { Some(api_key) },
            api_base: if api_base.is_empty() { None } else { Some(api_base) },
            model,
            max_tokens: Some(4096),
            temperature: Some(0.7),
            enabled: true,
            created_at: now,
            updated_at: now,
        })
    }
}
impl Focusable for ProviderForm {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl Render for ProviderForm {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        v_flex()
            .gap_3()
            .child(
                v_flex()
                    .gap_1()
                    .child(
                        div()
                            .text_sm()
                            .font_weight(gpui::FontWeight::MEDIUM)
                            .child("Name"),
                    )
                    .child(Input::new(&self.name_input)),
            )
            .child(
                v_flex()
                    .gap_1()
                    .child(
                        div()
                            .text_sm()
                            .font_weight(gpui::FontWeight::MEDIUM)
                            .child("Provider Type"),
                    )
                    .child(Select::new(&self.provider_type_select)),
            )
            .child(
                v_flex()
                    .gap_1()
                    .child(
                        div()
                            .text_sm()
                            .font_weight(gpui::FontWeight::MEDIUM)
                            .child("API Key"),
                    )
                    .child(Input::new(&self.api_key_input)),
            )
            .child(
                v_flex()
                    .gap_1()
                    .child(
                        div()
                            .text_sm()
                            .font_weight(gpui::FontWeight::MEDIUM)
                            .child("API Base URL (Optional)"),
                    )
                    .child(Input::new(&self.api_base_input)),
            )
            .child(
                v_flex()
                    .gap_1()
                    .child(
                        div()
                            .text_sm()
                            .font_weight(gpui::FontWeight::MEDIUM)
                            .child("Model"),
                    )
                    .child(Input::new(&self.model_input)),
            )
    }
}
