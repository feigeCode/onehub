use gpui::{div, px, App, AppContext, AsyncApp, Context, EventEmitter, FocusHandle, Focusable, IntoElement, ParentElement, Render, SharedString, Styled, Window};
use gpui::prelude::FluentBuilder;
use gpui_component::{
    button::{Button, ButtonVariants},
    h_flex, v_flex, ActiveTheme, WindowExt,
};
use gpui_component::button::ButtonVariant;
use gpui_component::dialog::DialogButtonProps;
use one_core::gpui_tokio::Tokio;
use one_core::llm::{storage::ProviderRepository, types::ProviderConfig};
use one_core::storage::{traits::Repository, GlobalStorageState, StorageManager};
use super::provider_form_dialog::{ProviderForm};

pub struct LlmProvidersView {
    focus_handle: FocusHandle,
    storage_manager: StorageManager,
    providers: Vec<ProviderConfig>,
    loading: bool,
    loaded: bool,
}

impl LlmProvidersView {
    pub fn new(cx: &mut Context<Self>) -> Self {
        let focus_handle = cx.focus_handle();
        let storage_state = cx.global::<GlobalStorageState>();
        let storage_manager = storage_state.storage.clone();

        Self {
            focus_handle,
            storage_manager,
            providers: vec![],
            loading: false,
            loaded: false,
        }
    }

    fn load_providers(&mut self, cx: &mut Context<Self>) {
        self.loading = true;
        self.loaded = true;
        cx.notify();

        let storage_manager = self.storage_manager.clone();

        cx.spawn(async move |this, cx: &mut AsyncApp| {
            let task_result = match Tokio::spawn(cx, async move {
                let pool = storage_manager.get_pool().await?;
                let repo = storage_manager.get::<ProviderRepository>().await
                    .ok_or_else(|| anyhow::anyhow!("ProviderRepository not found"))?;
                repo.list(&pool).await
            }) {
                Ok(task) => task.await.ok(),
                Err(_) => None,
            };

            _ = this.update(cx, |view, cx| match task_result {
                Some(Ok(providers)) => {
                    view.providers = providers;
                    view.loading = false;
                    cx.notify();
                }
                Some(Err(e)) => {
                    tracing::error!("Failed to load providers: {}", e);
                    view.loading = false;
                    cx.notify();
                }
                None => {
                    view.loading = false;
                    cx.notify();
                }
            });
        })
        .detach();
    }

    fn add_provider(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.open_provider_form(None, cx, window);
    }

    fn edit_provider(&mut self, provider_id: i64, window: &mut Window, cx: &mut Context<Self>) {
        // 查找要编辑的 provider
        let provider = self
            .providers
            .iter()
            .find(|p| p.id == provider_id)
            .cloned();
        self.open_provider_form(provider, cx, window);
    }
    
    fn open_provider_form(&mut self, provider: Option<ProviderConfig>, cx: &mut Context<Self>, window: &mut Window) {
        let is_update = provider.is_some();
        let storage_manager = self.storage_manager.clone();
        let form = cx.new(|cx| ProviderForm::new_with_config(provider, window, cx));
        let form_for_ok = form.clone();
        let storage_manager_for_ok = storage_manager.clone();
        let view = cx.entity().clone();
        
        window.open_dialog(cx, move |dialog, _, _| {
            let form_clone = form_for_ok.clone();
            let storage_clone = storage_manager_for_ok.clone();
            let view_clone = view.clone();
            
            dialog
                .title(if is_update {"Edit Provider"} else {"Add Provider"})
                .child(form.clone())
                .confirm()
                .button_props(
                    DialogButtonProps::default()
                        .ok_text(if is_update {"Update"} else {"Add"})
                )
                .on_ok(move |_, window, cx| {
                    let config_opt = form_clone.update(cx, |form, cx| {
                        form.get_config(cx)
                    });
                    
                    if config_opt.is_none() {
                        window.push_notification("Please fill in all required fields", cx);
                        return false;
                    }
                    
                    let mut config = config_opt.unwrap();
                    let storage_manager_clone = storage_clone.clone();
                    let view_for_spawn = view_clone.clone();
                    
                    // 在 tokio 线程池中执行持久层操作
                    cx.spawn(async move |cx: &mut AsyncApp| {
                        let task_result = match Tokio::spawn(cx, async move {
                            let pool = storage_manager_clone.get_pool().await?;
                            let repo = storage_manager_clone.get::<ProviderRepository>().await
                                .ok_or_else(|| anyhow::anyhow!("ProviderRepository not found"))?;
                            // 如果是更新走更新逻辑
                            if is_update { 
                                repo.update(&pool, &config).await
                            } else { 
                                repo.insert(&pool, &mut config).await?;
                                Ok(())
                            }
                        }) {
                            Ok(task) => task.await.ok(),
                            Err(_) => None,
                        };
                        
                        // 保存成功后重新加载列表
                        _ = view_for_spawn.update(cx, |view, cx| {
                            match task_result {
                                Some(Ok(_)) => {
                                    view.load_providers(cx);
                                }
                                Some(Err(e)) => {
                                    tracing::error!("Failed to save provider: {}", e);
                                }
                                None => {
                                    tracing::error!("Failed to save provider: task cancelled");
                                }
                            }
                        });
                    }).detach();
                    true
                })
        });
    }

    fn delete_provider(&mut self, provider_id: i64, cx: &mut Context<Self>) {
        let storage_manager = self.storage_manager.clone();

        cx.spawn(async move |this, cx: &mut AsyncApp| {
            let task_result = match Tokio::spawn(cx, async move {
                let pool = storage_manager.get_pool().await?;
                let repo = storage_manager.get::<ProviderRepository>().await
                    .ok_or_else(|| anyhow::anyhow!("ProviderRepository not found"))?;
                repo.delete(&pool, provider_id).await
            }) {
                Ok(task) => task.await.ok(),
                Err(_) => None,
            };

            _ = this.update(cx, |view, cx| {
                if let Some(Ok(_)) = task_result {
                    view.load_providers(cx);
                } else if let Some(Err(e)) = task_result {
                    tracing::error!("Failed to delete provider: {}", e);
                }
            });
        })
        .detach();
    }

    fn toggle_provider(&mut self, mut provider: ProviderConfig, cx: &mut Context<Self>) {
        provider.enabled = !provider.enabled;

        let storage_manager = self.storage_manager.clone();

        cx.spawn(async move |this, cx: &mut AsyncApp| {
            let task_result = match Tokio::spawn(cx, async move {
                let pool = storage_manager.get_pool().await?;
                let repo = storage_manager.get::<ProviderRepository>().await
                    .ok_or_else(|| anyhow::anyhow!("ProviderRepository not found"))?;
                repo.update(&pool, &provider).await
            }) {
                Ok(task) => task.await.ok(),
                Err(_) => None,
            };

            _ = this.update(cx, |view, cx| {
                if let Some(Ok(_)) = task_result {
                    view.load_providers(cx);
                } else if let Some(Err(e)) = task_result {
                    tracing::error!("Failed to toggle provider: {}", e);
                }
            });
        })
        .detach();
    }
}

impl Render for LlmProvidersView {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        // 第一次渲染时开始加载
        if !self.loaded && !self.loading {
            self.load_providers(cx);
        }

        v_flex()
            .size_full()
            .gap_4()
            .p_6()
            .child(
                h_flex()
                    .justify_between()
                    .items_center()
                    .child(
                        v_flex()
                            .gap_1()
                            .child(
                                div()
                                    .text_xl()
                                    .font_weight(gpui::FontWeight::BOLD)
                                    .child("LLM Providers"),
                            )
                            .child(
                                div()
                                    .text_sm()
                                    .text_color(cx.theme().muted_foreground)
                                    .child("Manage your AI model providers"),
                            ),
                    )
                    .child(
                        Button::new("add-provider")
                            .with_variant(ButtonVariant::Primary)
                            .label("Add Provider")
                            .on_click(cx.listener(|view,_, window, cx| {
                                view.add_provider(window, cx);
                            })),
                    ),
            )
            .child(if self.loading {
                div()
                    .flex_1()
                    .flex()
                    .items_center()
                    .justify_center()
                    .child("Loading providers...")
                    .into_any_element()
            } else if self.providers.is_empty() {
                div()
                    .flex_1()
                    .flex()
                    .items_center()
                    .justify_center()
                    .child(
                        v_flex()
                            .gap_2()
                            .items_center()
                            .child("No providers configured")
                            .child(
                                div()
                                    .text_sm()
                                    .text_color(cx.theme().muted_foreground)
                                    .child("Click 'Add Provider' to get started"),
                            ),
                    )
                    .into_any_element()
            } else {
                let mut cards = v_flex().gap_3();
                for provider in &self.providers {
                    cards = cards.child(self.render_provider_card(provider.clone(), cx));
                }
                cards.into_any_element()
            })
    }
}

impl LlmProvidersView {
    fn render_provider_card(
        &self,
        provider: ProviderConfig,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let provider_id = provider.id.clone();
        let provider_id_edit = provider.id.clone();
        let provider_clone = provider.clone();

        div()
            .flex()
            .p_4()
            .gap_4()
            .rounded_lg()
            .border_1()
            .border_color(cx.theme().border)
            .bg(cx.theme().primary_foreground)
            .child(
                v_flex()
                    .flex_1()
                    .gap_2()
                    .child(
                        h_flex()
                            .gap_2()
                            .items_center()
                            .child(
                                div()
                                    .text_lg()
                                    .font_weight(gpui::FontWeight::SEMIBOLD)
                                    .child(provider.name.clone()),
                            )
                            .child(
                                div()
                                    .px_2()
                                    .py(px(2.0))
                                    .rounded_md()
                                    .bg(cx.theme().secondary)
                                    .text_xs()
                                    .child(provider.provider_type.display_name()),
                            )
                            .when(!provider.enabled, |this| {
                                this.child(
                                    div()
                                        .px_2()
                                        .py(px(2.0))
                                        .rounded_md()
                                        .bg(cx.theme().muted)
                                        .text_xs()
                                        .child("Disabled"),
                                )
                            }),
                    )
                    .child(
                        v_flex()
                            .gap_1()
                            .child(
                                div()
                                    .text_sm()
                                    .text_color(cx.theme().muted_foreground)
                                    .child(format!("Model: {}", provider.model)),
                            )
                            .when(provider.api_base.is_some(), |this| {
                                this.child(
                                    div()
                                        .text_sm()
                                        .text_color(cx.theme().muted_foreground)
                                        .child(format!(
                                            "API Base: {}",
                                            provider.api_base.as_ref().unwrap()
                                        )),
                                )
                            }),
                    ),
            )
            .child(
                h_flex()
                    .gap_2()
                    .items_center()
                    .child(
                        Button::new(SharedString::from(format!("toggle-{}", provider_id)))
                            .with_variant(if provider.enabled {
                                ButtonVariant::Secondary
                            } else {
                                ButtonVariant::Primary
                            })
                            .label(if provider.enabled { "Disable" } else { "Enable" })
                            .on_click(cx.listener(move |view, _, _, cx| {
                                view.toggle_provider(provider_clone.clone(), cx);
                            })),
                    )
                    .child(
                        Button::new(SharedString::from(format!("edit-{}", provider_id_edit)))
                            .with_variant(ButtonVariant::Secondary)
                            .label("Edit")
                            .on_click(cx.listener(move |view,_, window, cx| {
                                view.edit_provider(provider_id_edit.clone(), window, cx);
                            })),
                    )
                    .child(
                        Button::new(SharedString::from(format!("delete-{}", provider_id)))
                            .with_variant(ButtonVariant::Secondary)
                            .label("Delete")
                            .on_click(cx.listener(move |view,_, _, cx| {
                                view.delete_provider(provider_id.clone(), cx);
                            })),
                    ),
            )
    }
}

impl Focusable for LlmProvidersView {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl EventEmitter<()> for LlmProvidersView {}
