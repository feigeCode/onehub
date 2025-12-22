//! AI Chat Panel - 数据库 AI 助手对话面板

use std::rc::Rc;
use futures::StreamExt;
use gpui::{div, px, prelude::FluentBuilder, AnyElement, App, AppContext, Context, Corner, Entity, EventEmitter, FocusHandle, Focusable, IntoElement, InteractiveElement, ParentElement, Render, RenderOnce, SharedString, StatefulInteractiveElement, Styled, Subscription, Task, Window, AsyncApp};
use gpui_component::{
    button::{Button, ButtonVariants},
    clipboard::Clipboard,
    h_flex, list::{List, ListDelegate, ListState}, popover::Popover, text::TextView, v_flex,
    v_virtual_list, VirtualListScrollHandle,
    ActiveTheme, Icon, IconName, IndexPath, Selectable, Sizable, Size,
};
use uuid::Uuid;

use crate::ai_input::{AIInput, AIInputEvent};
use one_core::llm::{
    chat_history::{ChatMessage, ChatSession, MessageRepository, SessionRepository},
    manager::GlobalProviderState,
    storage::ProviderRepository,
    types::{ChatMessage as LlmChatMessage, ChatRequest, ChatStreamEvent},
};
use one_core::storage::{traits::Repository, GlobalStorageState};

/// AI 聊天消息类型
#[derive(Clone, Debug, PartialEq)]
pub enum MessageVariant {
    Text,
    Status {
        title: String,
        is_done: bool,
    },
}

/// AI 聊天消息
#[derive(Clone, Debug)]
pub struct ChatMessageUI {
    pub id: String,
    pub role: ChatRole,
    pub content: String,
    pub variant: MessageVariant,
    pub is_streaming: bool,
    pub is_expanded: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub enum ChatRole {
    User,
    Assistant,
    System,
}

impl ChatMessageUI {
    pub fn user(content: impl Into<String>) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            role: ChatRole::User,
            content: content.into(),
            variant: MessageVariant::Text,
            is_streaming: false,
            is_expanded: true,
        }
    }

    pub fn assistant(content: impl Into<String>) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            role: ChatRole::Assistant,
            content: content.into(),
            variant: MessageVariant::Text,
            is_streaming: false,
            is_expanded: true,
        }
    }

    pub fn status(title: impl Into<String>, is_done: bool) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            role: ChatRole::Assistant,
            content: String::new(),
            variant: MessageVariant::Status {
                title: title.into(),
                is_done,
            },
            is_streaming: !is_done,
            is_expanded: !is_done,
        }
    }

    // Convert UI message to LLM message
    pub fn to_llm_message(&self) -> LlmChatMessage {
        LlmChatMessage {
            role: match self.role {
                ChatRole::User => "user".to_string(),
                ChatRole::Assistant => "assistant".to_string(),
                ChatRole::System => "system".to_string(),
            },
            content: self.content.clone(),
        }
    }

    // Create from LLM message
    pub fn from_llm_message(llm_msg: &LlmChatMessage) -> Self {
        let role = match llm_msg.role.as_str() {
            "user" => ChatRole::User,
            "assistant" => ChatRole::Assistant,
            "system" => ChatRole::System,
            _ => ChatRole::User, // Default to user
        };

        Self {
            id: Uuid::new_v4().to_string(),
            role,
            content: llm_msg.content.clone(),
            variant: MessageVariant::Text,
            is_streaming: false,
            is_expanded: true,
        }
    }
}

/// AI 聊天面板事件
#[derive(Clone, Debug)]
pub enum AiChatPanelEvent {
    Close,
    ExecuteSql { sql: String },
}

// ============================================================================
// SessionListItem - 历史会话列表项
// ============================================================================

#[derive(IntoElement)]
pub struct SessionListItem {
    session_id: i64,
    name: SharedString,
    updated_at: i64,
    selected: bool,
    panel: Entity<AiChatPanel>,
}

impl SessionListItem {
    pub fn new(
        session_id: i64,
        name: SharedString,
        updated_at: i64,
        panel: Entity<AiChatPanel>,
    ) -> Self {
        Self {
            session_id,
            name,
            updated_at,
            selected: false,
            panel,
        }
    }
}

impl Selectable for SessionListItem {
    fn selected(mut self, selected: bool) -> Self {
        self.selected = selected;
        self
    }

    fn is_selected(&self) -> bool {
        self.selected
    }
}

impl RenderOnce for SessionListItem {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let panel = self.panel.clone();
        let session_id = self.session_id;
        let is_current = panel.read(cx).session_id == Some(session_id);

        h_flex()
            .id(SharedString::from(format!("session-item-{}", session_id)))
            .w_full()
            .gap_2()
            .items_center()
            .px_2()
            .py_1()
            .rounded_md()
            .cursor_pointer()
            .when(self.selected, |this| this.bg(cx.theme().list_active))
            .when(is_current, |this| {
                this.bg(cx.theme().accent)
                    .text_color(cx.theme().accent_foreground)
            })
            .on_click({
                let panel = panel.clone();
                move |_, window, cx| {
                    panel.update(cx, |this, cx| {
                        this.history_popover_open = false;
                        this.load_session(session_id, cx);
                    });
                    window.refresh();
                }
            })
            .child(
                v_flex()
                    .flex_1()
                    .min_w_0()
                    .gap_0p5()
                    .child(
                        div()
                            .text_sm()
                            .font_weight(gpui::FontWeight::MEDIUM)
                            .overflow_hidden()
                            .text_ellipsis()
                            .child(self.name.clone())
                    )
                    .child(
                        div()
                            .text_xs()
                            .text_color(if is_current {
                                cx.theme().accent_foreground
                            } else {
                                cx.theme().muted_foreground
                            })
                            .child(format_timestamp(self.updated_at))
                    )
            )
            .child(
                Button::new(SharedString::from(format!("delete-{}", session_id)))
                    .icon(IconName::Delete)
                    .ghost()
                    .xsmall()
                    .on_click({
                        let panel = panel.clone();
                        move |_, window, cx| {
                            panel.update(cx, |this, cx| {
                                this.delete_session(session_id, cx);
                            });
                            window.refresh();
                        }
                    })
            )
    }
}

// ============================================================================
// SessionListDelegate - 历史会话列表代理
// ============================================================================

pub struct SessionListDelegate {
    panel: Entity<AiChatPanel>,
    sessions: Vec<(i64, SharedString, i64)>,
    filtered_sessions: Vec<(i64, SharedString, i64)>,
    selected_index: Option<IndexPath>,
}

impl ListDelegate for SessionListDelegate {
    type Item = SessionListItem;

    fn perform_search(&mut self, query: &str, _window: &mut Window, cx: &mut Context<ListState<Self>>) -> Task<()> {
        if query.is_empty() {
            self.filtered_sessions = self.sessions.clone();
        } else {
            let query_lower = query.to_lowercase();
            self.filtered_sessions = self.sessions
                .iter()
                .filter(|(_, name, _)| name.to_lowercase().contains(&query_lower))
                .cloned()
                .collect();
        }
        cx.notify();
        Task::ready(())
    }

    fn items_count(&self, _section: usize, _cx: &App) -> usize {
        self.filtered_sessions.len()
    }

    fn render_item(
        &mut self,
        ix: IndexPath,
        _window: &mut Window,
        _cx: &mut Context<ListState<Self>>,
    ) -> Option<Self::Item> {
        let (session_id, name, updated_at) = self.filtered_sessions.get(ix.row)?.clone();
        Some(SessionListItem::new(session_id, name, updated_at, self.panel.clone()))
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
            if let Some((session_id, _, _)) = self.filtered_sessions.get(ix.row) {
                let session_id = *session_id;
                self.panel.update(cx, |this, cx| {
                    this.history_popover_open = false;
                    this.load_session(session_id, cx);
                });
                window.refresh();
            }
        }
    }

    fn cancel(&mut self, _window: &mut Window, cx: &mut Context<ListState<Self>>) {
        self.panel.update(cx, |this, cx| {
            this.history_popover_open = false;
            cx.notify();
        });
    }
}

/// AI 聊天面板
pub struct AiChatPanel {
    focus_handle: FocusHandle,
    messages: Vec<ChatMessageUI>,
    ai_input: Entity<AIInput>,
    _input_subscription: Subscription,
    session_id: Option<i64>,
    provider_id: Option<String>,
    connection_name: Option<String>,
    database: Option<String>,
    is_loading: bool,
    storage_manager: one_core::storage::StorageManager,
    scroll_handle: VirtualListScrollHandle,
    history_sessions: Vec<ChatSession>,
    auto_scroll_enabled: bool,
    history_popover_open: bool,
    session_list: Option<Entity<ListState<SessionListDelegate>>>,
}


impl AiChatPanel {
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let focus_handle = cx.focus_handle();
        let ai_input = cx.new(|cx| AIInput::new(window, cx));

        let input_subscription = cx.subscribe_in(&ai_input, window, |this, _input, event, _window, cx| {
            match event {
                AIInputEvent::Submit { content } => {
                    this.send_message(content.clone(), cx);
                },
                AIInputEvent::ProviderChanged { provider_id } => {
                    this.provider_id = Some(provider_id.to_string());
                    cx.notify();
                }
            }
        });

        // Get the storage manager from the global state
        let global_state = cx.global::<GlobalStorageState>();
        let storage_manager = global_state.storage.clone();

        let mut panel = Self {
            focus_handle,
            messages: Vec::new(),
            ai_input,
            _input_subscription: input_subscription,
            session_id: None,
            provider_id: None,
            connection_name: None,
            database: None,
            is_loading: false,
            storage_manager,
            scroll_handle: VirtualListScrollHandle::new(),
            history_sessions: Vec::new(),
            auto_scroll_enabled: true,
            history_popover_open: false,
            session_list: None,
        };

        // 加载 providers
        panel.load_providers(cx);
        panel
    }

    fn load_providers(&mut self, cx: &mut Context<Self>) {
        let storage_manager = self.storage_manager.clone();
        let ai_input = self.ai_input.clone();

        cx.spawn(async move |this, cx: &mut AsyncApp| {
            use one_core::gpui_tokio::Tokio;
            use crate::ai_input::ProviderItem;

            // 在 tokio 运行时中执行持久层操作
            let result = Tokio::spawn(cx, async move {
                let repo = storage_manager.get::<ProviderRepository>().await
                    .ok_or_else(|| anyhow::anyhow!("ProviderRepository not found"))?;
                let all_providers = repo.list().await?;
                let enabled_providers: Vec<_> = all_providers.into_iter()
                    .filter(|p| p.enabled)
                    .collect();
                Ok::<_, anyhow::Error>(enabled_providers)
            });

            if let Ok(task) = result {
                if let Ok(Ok(providers)) = task.await {
                    if providers.is_empty() {
                        return;
                    }

                    let items: Vec<ProviderItem> = providers
                        .iter()
                        .map(ProviderItem::from_config)
                        .collect();
                    let first_provider_id = providers[0].id.to_string();

                    let _ = cx.update(|cx| {
                        if let Some(window_id) = cx.active_window() {
                            cx.update_window(window_id, |_entity, window, cx| {
                                ai_input.update(cx, |input, cx| {
                                    input.update_providers(items.clone(), window, cx);
                                });
                            })
                        } else {
                            Ok(())
                        }
                    });

                    if let Some(entity) = this.upgrade() {
                        let _ = cx.update(|cx| {
                            entity.update(cx, |panel, cx| {
                                panel.provider_id = Some(first_provider_id.clone());
                                cx.notify();
                            });
                        });
                    }
                }
            }
        })
        .detach();
    }

    pub fn set_connection_info(&mut self, connection_name: Option<String>, database: Option<String>) {
        self.connection_name = connection_name;
        self.database = database;
    }

    pub fn set_provider_id(&mut self, provider_id: String, cx: &mut Context<Self>) {
        self.provider_id = Some(provider_id);
        cx.notify();
    }

    // 创建新会话 - 同步返回，异步保存
    pub fn start_new_session(&mut self, cx: &mut Context<Self>) {
        self.session_id = None;
        self.messages.clear();
        cx.notify();
    }

    pub fn toggle_history_popover(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.history_popover_open = !self.history_popover_open;
        if self.history_popover_open {
            // 先用现有数据更新列表，然后异步加载最新数据
            self.update_session_list(window, cx);
            self.load_history_sessions(cx);
        }
        cx.notify();
    }

    fn update_session_list(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let sessions_data: Vec<(i64, SharedString, i64)> = self.history_sessions
            .iter()
            .map(|s| (s.id, SharedString::from(s.name.clone()), s.updated_at))
            .collect();
        let panel = cx.entity();

        if let Some(session_list) = &self.session_list {
            session_list.update(cx, |state, _| {
                let delegate = state.delegate_mut();
                delegate.sessions = sessions_data.clone();
                delegate.filtered_sessions = sessions_data;
            });
        } else {
            self.session_list = Some(cx.new(|cx| {
                ListState::new(
                    SessionListDelegate {
                        panel,
                        sessions: sessions_data.clone(),
                        filtered_sessions: sessions_data,
                        selected_index: None,
                    },
                    window,
                    cx,
                ).searchable(true)
            }));
        }
    }

    fn load_history_sessions(&mut self, cx: &mut Context<Self>) {
        let storage_manager = self.storage_manager.clone();

        cx.spawn(async move |this, cx: &mut AsyncApp| {
            use one_core::gpui_tokio::Tokio;

            let result = Tokio::spawn(cx, async move {
                let session_repo = storage_manager.get::<SessionRepository>().await
                    .ok_or_else(|| anyhow::anyhow!("SessionRepository not found"))?;
                session_repo.list().await
            });

            if let Ok(task) = result {
                if let Ok(Ok(sessions)) = task.await {
                    if let Some(entity) = this.upgrade() {
                        let _ = cx.update(|cx| {
                            if let Some(window_id) = cx.active_window() {
                                let _ = cx.update_window(window_id, |_, window, cx| {
                                    entity.update(cx, |this, cx| {
                                        this.history_sessions = sessions;
                                        this.update_session_list(window, cx);
                                        cx.notify();
                                    });
                                });
                            }
                        });
                    }
                }
            }
        }).detach();
    }

    fn delete_session(&mut self, session_id: i64, cx: &mut Context<Self>) {
        let storage_manager = self.storage_manager.clone();

        cx.spawn(async move |this, cx: &mut AsyncApp| {
            use one_core::gpui_tokio::Tokio;

            let result = Tokio::spawn(cx, async move {
                let session_repo = storage_manager.get::<SessionRepository>().await
                    .ok_or_else(|| anyhow::anyhow!("SessionRepository not found"))?;
                let message_repo = storage_manager.get::<MessageRepository>().await
                    .ok_or_else(|| anyhow::anyhow!("MessageRepository not found"))?;

                // 先删除消息，再删除会话
                message_repo.delete_by_session(session_id).await?;
                session_repo.delete(session_id).await
            });

            if let Ok(task) = result {
                if let Ok(Ok(_)) = task.await {
                    if let Some(entity) = this.upgrade() {
                        let _ = cx.update(|cx| {
                            entity.update(cx, |this, cx| {
                                // 如果删除的是当前会话，清空界面
                                if this.session_id == Some(session_id) {
                                    this.session_id = None;
                                    this.messages.clear();
                                }
                                // 从历史列表中移除
                                this.history_sessions.retain(|s| s.id != session_id);
                                cx.notify();
                            });
                        });
                    }
                }
            }
        }).detach();
    }

    #[allow(dead_code)]
    fn rename_session(&mut self, session_id: i64, new_name: String, cx: &mut Context<Self>) {
        let storage_manager = self.storage_manager.clone();

        cx.spawn(async move |this, cx: &mut AsyncApp| {
            use one_core::gpui_tokio::Tokio;

            let result = Tokio::spawn(cx, async move {
                let session_repo = storage_manager.get::<SessionRepository>().await
                    .ok_or_else(|| anyhow::anyhow!("SessionRepository not found"))?;

                if let Some(mut session) = session_repo.get(session_id).await? {
                    session.name = new_name;
                    session_repo.update(&session).await?;
                }
                Ok::<(), anyhow::Error>(())
            });

            if let Ok(task) = result {
                if let Ok(Ok(_)) = task.await {
                    if let Some(entity) = this.upgrade() {
                        let _ = cx.update(|cx| {
                            entity.update(cx, |this, cx| {
                                // 重新加载历史会话列表
                                this.load_history_sessions(cx);
                            });
                        });
                    }
                }
            }
        }).detach();
    }

    fn load_session(&mut self, session_id: i64, cx: &mut Context<Self>) {
        let storage_manager = self.storage_manager.clone();

        cx.spawn(async move |this, cx: &mut AsyncApp| {
            use one_core::gpui_tokio::Tokio;

            let result = Tokio::spawn(cx, async move {
                let message_repo = storage_manager.get::<MessageRepository>().await
                    .ok_or_else(|| anyhow::anyhow!("MessageRepository not found"))?;
                message_repo.list_by_session(session_id).await
            });

            if let Ok(task) = result {
                if let Ok(Ok(messages)) = task.await {
                    if let Some(entity) = this.upgrade() {
                        let _ = cx.update(|cx| {
                            entity.update(cx, |this, cx| {
                                this.session_id = Some(session_id);
                                this.messages = messages.iter()
                                    .map(|msg| ChatMessageUI {
                                        id: msg.id.to_string(),
                                        role: match msg.role.as_str() {
                                            "user" => ChatRole::User,
                                            "assistant" => ChatRole::Assistant,
                                            "system" => ChatRole::System,
                                            _ => ChatRole::User,
                                        },
                                        content: msg.content.clone(),
                                        variant: MessageVariant::Text,
                                        is_streaming: false,
                                        is_expanded: true,
                                    })
                                    .collect();
                                this.history_popover_open = false;
                                cx.notify();
                            });
                        });
                    }
                }
            }
        }).detach();
    }

    #[allow(dead_code)]
    fn save_message(&self, session_id: i64, role: &str, content: &str, cx: &mut Context<Self>) {
        let storage_manager = self.storage_manager.clone();
        let message_content = content.to_string();
        let message_role = role.to_string();

        cx.spawn(async move |_this, cx: &mut AsyncApp| {
            use one_core::gpui_tokio::Tokio;
            if let Ok(task) = Tokio::spawn(cx, async move {
                let message_repo = storage_manager.get::<MessageRepository>().await
                    .ok_or_else(|| anyhow::anyhow!("MessageRepository not found"))?;
                let mut message = ChatMessage::new(session_id, message_role, message_content);
                message_repo.insert(&mut message).await
            }) {
                let _ = task.await;
            }
        }).detach();
    }

    fn send_message(&mut self, content: String, cx: &mut Context<Self>) {
        if content.trim().is_empty() || self.is_loading {
            return;
        }

        let Some(provider_id_str) = self.provider_id.clone() else {
            self.messages.push(ChatMessageUI::assistant("No provider selected.".to_string()));
            cx.notify();
            return;
        };

        let provider_id: i64 = match provider_id_str.parse() {
            Ok(id) => id,
            Err(_) => {
                self.messages.push(ChatMessageUI::assistant("Invalid provider ID.".to_string()));
                cx.notify();
                return;
            }
        };

        let global_provider_state = cx.global::<GlobalProviderState>().clone();
        let storage_manager = self.storage_manager.clone();
        let connection_name = self.connection_name.clone();
        let session_id = self.session_id;

        // 添加用户消息到 UI
        self.messages.push(ChatMessageUI::user(content.clone()));

        // 创建助手消息占位符
        let assistant_msg_id = Uuid::new_v4().to_string();
        self.messages.push(ChatMessageUI {
            id: assistant_msg_id.clone(),
            role: ChatRole::Assistant,
            content: String::new(),
            variant: MessageVariant::Text,
            is_streaming: true,
            is_expanded: true,
        });

        self.auto_scroll_enabled = true;
        self.is_loading = true;
        cx.notify();

        cx.spawn(async move |this, cx: &mut AsyncApp| {
            use one_core::gpui_tokio::Tokio;

            // 获取或创建会话
            let session_db_id = match session_id {
                Some(id) => id,
                None => {
                    let storage_manager_clone = storage_manager.clone();
                    let result = Tokio::spawn(cx, async move {
                        let session_repo = storage_manager_clone.get::<SessionRepository>().await
                            .ok_or_else(|| anyhow::anyhow!("SessionRepository not found"))?;
                        let session_name = format!("Chat with {}", connection_name.as_deref().unwrap_or("Database"));
                        let mut session = ChatSession::new(session_name, provider_id.to_string());
                        session_repo.insert(&mut session).await
                    });

                    match result {
                        Ok(task) => match task.await {
                            Ok(Ok(id)) => {
                                // 更新 UI 中的 session_id
                                if let Some(entity) = this.upgrade() {
                                    let _ = cx.update(|cx| {
                                        entity.update(cx, |this, cx| {
                                            this.session_id = Some(id);
                                            cx.notify();
                                        });
                                    });
                                }
                                id
                            }
                            _ => {
                                if let Some(entity) = this.upgrade() {
                                    let _ = cx.update(|cx| {
                                        entity.update(cx, |this, cx| {
                                            if let Some(msg) = this.messages.iter_mut().find(|m| m.id == assistant_msg_id) {
                                                msg.is_streaming = false;
                                                msg.content = "Failed to create session.".to_string();
                                            }
                                            this.is_loading = false;
                                            cx.notify();
                                        });
                                    });
                                }
                                return;
                            }
                        }
                        Err(_) => return,
                    }
                }
            };

            // 保存用户消息
            let content_clone = content.clone();
            let storage_manager_for_save = storage_manager.clone();
            if let Ok(task) = Tokio::spawn_result(cx, async move {
                let message_repo = storage_manager_for_save.get::<MessageRepository>().await
                    .ok_or_else(|| anyhow::anyhow!("MessageRepository not found"))?;
                let mut message = ChatMessage::new(session_db_id, "user".to_string(), content_clone);
                message_repo.insert(&mut message).await?;
                Ok(())
            }) {
                if let Err(e) = task.await {
                    eprintln!("Failed to save user message: {}", e);
                }
            }

            // 获取聊天历史（不包含当前消息）
            let storage_manager_for_history = storage_manager.clone();
            let history_task = Tokio::spawn_result(cx, async move {
                let message_repo = storage_manager_for_history.get::<MessageRepository>().await
                    .ok_or_else(|| anyhow::anyhow!("MessageRepository not found"))?;
                let messages = message_repo.list_by_session(session_db_id).await?;
                Ok::<Vec<LlmChatMessage>, anyhow::Error>(
                    messages.iter().map(|msg| LlmChatMessage {
                        role: msg.role.clone(),
                        content: msg.content.clone(),
                    }).collect()
                )
            });

            let mut history = match history_task {
                Ok(task) => task.await.unwrap_or_else(|e| {
                    eprintln!("Failed to load chat history: {}", e);
                    vec![]
                }),
                Err(e) => {
                    eprintln!("Failed to start chat history task: {}", e);
                    vec![]
                }
            };

            // 确保当前用户消息在历史中
            if history.is_empty() || history.last().map(|m| &m.content) != Some(&content) {
                history.push(LlmChatMessage::user(content.clone()));
            }

            let request = ChatRequest {
                messages: history,
                max_tokens: Some(2000),
                temperature: Some(0.7),
                stream: true,
            };

            // 开始流式聊天
            let storage_manager_for_stream = storage_manager.clone();
            let stream_result = Tokio::spawn(cx, async move {
                let repo = storage_manager_for_stream.get::<ProviderRepository>().await
                    .ok_or_else(|| anyhow::anyhow!("ProviderRepository not found"))?;
                let config = repo.get(provider_id).await?
                    .ok_or_else(|| anyhow::anyhow!("Provider not found: {}", provider_id))?;
                let provider = global_provider_state.manager().get_provider(config).await?;
                provider.chat_stream(request).await
            });

            let mut stream = match stream_result {
                Ok(task) => match task.await {
                    Ok(Ok(s)) => s,
                    Ok(Err(e)) => {
                        let error_msg = format!("Failed to start chat: {}", e);
                        eprintln!("Stream error: {}", error_msg);
                        if let Some(entity) = this.upgrade() {
                            let _ = cx.update(|cx| {
                                entity.update(cx, |this, cx| {
                                    if let Some(msg) = this.messages.iter_mut().find(|m| m.id == assistant_msg_id) {
                                        msg.is_streaming = false;
                                        msg.content = error_msg;
                                    }
                                    this.is_loading = false;
                                    cx.notify();
                                });
                            });
                        }
                        return;
                    }
                    Err(e) => {
                        let error_msg = format!("Task execution error: {:?}", e);
                        eprintln!("Task error: {}", error_msg);
                        if let Some(entity) = this.upgrade() {
                            let _ = cx.update(|cx| {
                                entity.update(cx, |this, cx| {
                                    if let Some(msg) = this.messages.iter_mut().find(|m| m.id == assistant_msg_id) {
                                        msg.is_streaming = false;
                                        msg.content = error_msg;
                                    }
                                    this.is_loading = false;
                                    cx.notify();
                                });
                            });
                        }
                        return;
                    }
                }
                Err(e) => {
                    eprintln!("Tokio spawn error: {:?}", e);
                    return;
                }
            };

            // 处理流式响应
            let mut full_content = String::new();
            while let Some(event) = stream.next().await {
                match event {
                    ChatStreamEvent::Chunk(chunk) => {
                        full_content.push_str(&chunk.delta);
                        if let Some(entity) = this.upgrade() {
                            let content_clone = full_content.clone();
                            let msg_id = assistant_msg_id.clone();
                            let _ = cx.update(|cx| {
                                entity.update(cx, |this, cx| {
                                    if let Some(msg) = this.messages.iter_mut().find(|m| m.id == msg_id) {
                                        msg.content = content_clone;
                                    }
                                    this.auto_scroll_to_bottom();
                                    cx.notify();
                                });
                            });
                        }
                    }
                    ChatStreamEvent::Done(_) => break,
                    ChatStreamEvent::Error(err) => {
                        if let Some(entity) = this.upgrade() {
                            let error_msg = format!("Stream error: {}", err);
                            let msg_id = assistant_msg_id.clone();
                            let _ = cx.update(|cx| {
                                entity.update(cx, |this, cx| {
                                    if let Some(msg) = this.messages.iter_mut().find(|m| m.id == msg_id) {
                                        msg.is_streaming = false;
                                        msg.content = error_msg;
                                    }
                                    this.is_loading = false;
                                    this.auto_scroll_to_bottom();
                                    cx.notify();
                                });
                            });
                        }
                        return;
                    }
                }
            }

            // 流结束，保存助手消息
            if let Some(entity) = this.upgrade() {
                let final_content = full_content.clone();
                let msg_id = assistant_msg_id.clone();
                let storage_manager_final = storage_manager.clone();
                let _ = cx.update(|cx| {
                    entity.update(cx, |this, cx| {
                        if let Some(msg) = this.messages.iter_mut().find(|m| m.id == msg_id) {
                            msg.is_streaming = false;
                            msg.content = final_content.clone();
                        }

                        // 保存助手消息到数据库
                        let final_content_inner = final_content.clone();
                        cx.spawn(async move |_this, cx: &mut AsyncApp| {
                            use one_core::gpui_tokio::Tokio;
                            match Tokio::spawn_result(cx, async move {
                                let message_repo = storage_manager_final.get::<MessageRepository>().await
                                    .ok_or_else(|| anyhow::anyhow!("MessageRepository not found"))?;
                                let mut assistant_message = ChatMessage::new(session_db_id, "assistant".to_string(), final_content_inner);
                                message_repo.insert(&mut assistant_message).await?;
                                Ok(())
                            }) {
                                Ok(task) => {
                                    if let Err(e) = task.await {
                                        eprintln!("Error saving assistant message: {}", e);
                                    }
                                }
                                Err(e) => {
                                    eprintln!("Failed to schedule assistant message save: {}", e);
                                }
                            }
                        }).detach();

                        this.is_loading = false;
                        this.auto_scroll_to_bottom();
                        cx.notify();
                    });
                });
            }
        }).detach();
    }

    fn render_messages(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let entity = cx.entity();

        // 计算每条消息的估算高度
        let item_sizes: Rc<Vec<gpui::Size<gpui::Pixels>>> = Rc::new(
            self.messages
                .iter()
                .map(|msg| {
                    // 估算消息高度：基础高度 + 内容行数 * 行高
                    let line_height = px(24.0);
                    let base_height = px(48.0); // padding + margin
                    let content_lines = (msg.content.len() as f32 / 60.0).ceil().max(1.0);
                    let estimated_height = base_height + line_height * content_lines;
                    gpui::size(px(0.0), estimated_height) // width will be measured
                })
                .collect(),
        );

        div()
            .flex_1()
            .w_full()
            .overflow_hidden()
            .p_4()
            .child(
                v_virtual_list(
                    entity.clone(),
                    "chat-messages-list",
                    item_sizes,
                    |this, range, window, cx| {
                        range
                            .into_iter()
                            .filter_map(|ix| {
                                this.messages.get(ix).map(|msg| {
                                    this.render_message(msg, window, cx)
                                })
                            })
                            .collect()
                    },
                )
                .track_scroll(&self.scroll_handle)
                .gap_4()
            )
    }

    fn check_scroll_position(&mut self) {
        use gpui_component::scroll::ScrollbarHandle;
        let offset = self.scroll_handle.offset();
        let content_size = self.scroll_handle.content_size();

        // 检查是否在底部（允许 50px 的误差）
        let scroll_bottom = offset.y.abs() + px(500.0); // 假设可见区域约 500px
        let is_at_bottom = scroll_bottom >= content_size.height - px(50.0);
        self.auto_scroll_enabled = is_at_bottom;
    }

    fn auto_scroll_to_bottom(&self) {
        if self.auto_scroll_enabled {
            self.scroll_handle.scroll_to_bottom();
        }
    }

    fn render_message(&self, msg: &ChatMessageUI, window: &mut Window, cx: &mut Context<Self>) -> AnyElement {
        match msg.role {
            ChatRole::User => {
                h_flex()
                    .w_full()
                    .justify_end()
                    .child(
                        div()
                            .px_4()
                            .py_2()
                            .bg(cx.theme().muted)
                            .rounded_lg()
                            .max_w_4_5()
                            .child(msg.content.clone())
                    )
                    .into_any_element()
            }
            ChatRole::Assistant => {
                match &msg.variant {
                    MessageVariant::Status { title, is_done } => {
                        self.render_status_message(msg.id.clone(), title, *is_done, msg.is_expanded, cx)
                    }
                    MessageVariant::Text => {
                        self.render_assistant_message(msg, window, cx)
                    }
                }
            }
            ChatRole::System => {
                h_flex()
                    .w_full()
                    .justify_center()
                    .child(
                        div()
                            .text_sm()
                            .text_color(cx.theme().muted_foreground)
                            .child(msg.content.clone())
                    )
                    .into_any_element()
            }
        }
    }

    fn render_status_message(&self, id: String, title: &str, is_done: bool, _is_expanded: bool, cx: &mut Context<Self>) -> AnyElement {
        let icon = if is_done { IconName::Check } else { IconName::Loader };

        div()
            .id(SharedString::from(id))
            .w_full()
            .flex()
            .items_center()
            .gap_2()
            .py_1()
            .child(
                Icon::new(icon)
                    .with_size(Size::Small)
                    .text_color(if is_done { cx.theme().success } else { cx.theme().muted_foreground })
            )
            .child(
                div()
                    .text_sm()
                    .text_color(cx.theme().muted_foreground)
                    .child(title.to_string())
            )
            .into_any_element()
    }

    fn render_assistant_message(&self, msg: &ChatMessageUI, _window: &mut Window, cx: &mut Context<Self>) -> AnyElement {
        if msg.is_streaming && msg.content.is_empty() {
            return h_flex()
                .gap_2()
                .child(
                    div()
                        .text_sm()
                        .text_color(cx.theme().muted_foreground)
                        .child("Thinking...")
                )
                .into_any_element();
        }

        // 使用消息的唯一 ID 作为 TextView 的 ID
        let view_id = SharedString::from(format!("ai-msg-{}", msg.id));

        div()
            .w_full()
            .max_w_4_5()
            .child(
                TextView::markdown(view_id, msg.content.clone())
                    .code_block_actions(|code_block, _window, _cx| {
                        let code = code_block.code();
                        let lang = code_block.lang();

                        h_flex()
                            .gap_1()
                            .child(Clipboard::new("copy").value(code.clone()))
                            .when_some(lang, |this, lang| {
                                if lang.as_ref() == "rust" || lang.as_ref() == "python" {
                                    this.child(
                                        Button::new("run-terminal")
                                            .icon(IconName::SquareTerminal)
                                            .ghost()
                                            .xsmall()
                                            .on_click(move |_, _, _cx| {
                                                println!("Running {} code: {}", lang, code);
                                            }),
                                    )
                                } else {
                                    this
                                }
                            })
                    })
                    .p_3()
                    .selectable(true)
            )
            .into_any_element()
    }

    fn render_input(&self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .w_full()
            .px_2()
            .py_2()
            .border_t_1()
            .border_color(cx.theme().border)
            .bg(cx.theme().muted)
            .child(self.ai_input.clone())
    }
}

// 格式化时间戳为可读格式
fn format_timestamp(timestamp: i64) -> String {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::from_secs(0))
        .as_secs() as i64;
    
    let diff = now - timestamp;
    
    if diff < 60 {
        "Just now".to_string()
    } else if diff < 3600 {
        format!("{} minutes ago", diff / 60)
    } else if diff < 86400 {
        format!("{} hours ago", diff / 3600)
    } else if diff < 604800 {
        format!("{} days ago", diff / 86400)
    } else {
        format!("{} weeks ago", diff / 604800)
    }
}

impl EventEmitter<AiChatPanelEvent> for AiChatPanel {}

impl Focusable for AiChatPanel {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl Render for AiChatPanel {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let session_list = self.session_list.clone();

        div()
            .size_full()
            .relative()
            .child(
                v_flex()
                    .size_full()
                    .bg(cx.theme().background)
                    .child(self.render_messages(window, cx))
                    .child(self.render_input(window, cx))
            )
            // 历史记录 Popover（悬浮在右上角）
            .when(self.history_popover_open, |this| {
                this.child(
                    div()
                        .absolute()
                        .top_2()
                        .right_2()
                        .child(
                            v_flex()
                                .bg(cx.theme().background)
                                .border_1()
                                .border_color(cx.theme().border)
                                .rounded(cx.theme().radius)
                                .shadow_lg()
                                .when_some(session_list, |container, list| {
                                    container.child(
                                        List::new(&list)
                                            .w(px(280.0))
                                            .max_h(px(350.0))
                                    )
                                })
                        )
                )
            })
    }
}
