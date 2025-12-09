use serde::{Deserialize, Serialize};
use crate::storage::now;

/// LLM Provider type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ProviderType {
    DeepSeek,
    Qwen,
    Claude,
    OpenAI,
    Ollama,
    Custom,
}

impl ProviderType {
    pub fn as_str(&self) -> &'static str {
        match self {
            ProviderType::DeepSeek => "deepseek",
            ProviderType::Qwen => "qwen",
            ProviderType::Claude => "claude",
            ProviderType::OpenAI => "openai",
            ProviderType::Ollama => "ollama",
            ProviderType::Custom => "custom",
        }
    }

    pub fn display_name(&self) -> &'static str {
        match self {
            ProviderType::DeepSeek => "DeepSeek",
            ProviderType::Qwen => "Qwen",
            ProviderType::Claude => "Claude",
            ProviderType::OpenAI => "OpenAI",
            ProviderType::Ollama => "Ollama",
            ProviderType::Custom => "Custom",
        }
    }

    pub fn all() -> Vec<ProviderType> {
        vec![
            ProviderType::DeepSeek,
            ProviderType::Qwen,
            ProviderType::Claude,
            ProviderType::OpenAI,
            ProviderType::Ollama,
            ProviderType::Custom,
        ]
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "deepseek" => Some(ProviderType::DeepSeek),
            "qwen" => Some(ProviderType::Qwen),
            "claude" => Some(ProviderType::Claude),
            "openai" => Some(ProviderType::OpenAI),
            "ollama" => Some(ProviderType::Ollama),
            "custom" => Some(ProviderType::Custom),
            _ => None,
        }
    }
}

/// LLM Provider configuration stored in database
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderConfig {
    pub id: i64,
    pub name: String,
    pub provider_type: ProviderType,
    pub api_key: Option<String>,
    pub api_base: Option<String>,
    pub model: String,
    pub max_tokens: Option<i32>,
    pub temperature: Option<f32>,
    pub enabled: bool,
    pub created_at: i64,
    pub updated_at: i64
}

impl ProviderConfig {
    pub fn new(
        name: String,
        provider_type: ProviderType,
        api_key: Option<String>,
        api_base: Option<String>,
        model: String,
    ) -> Self {
        let now = now();
        Self {
            id: now,
            name,
            provider_type,
            api_key,
            api_base,
            model,
            max_tokens: None,
            temperature: None,
            enabled: true,
            created_at: now,
            updated_at: now,
        }
    }
}

/// Request message for LLM chat completion
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatMessage {
    pub role: String,
    pub content: String,
}

impl ChatMessage {
    pub fn user(content: impl Into<String>) -> Self {
        Self {
            role: "user".to_string(),
            content: content.into(),
        }
    }

    pub fn assistant(content: impl Into<String>) -> Self {
        Self {
            role: "assistant".to_string(),
            content: content.into(),
        }
    }

    pub fn system(content: impl Into<String>) -> Self {
        Self {
            role: "system".to_string(),
            content: content.into(),
        }
    }
}

/// Chat completion request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatRequest {
    pub messages: Vec<ChatMessage>,
    pub max_tokens: Option<i32>,
    pub temperature: Option<f32>,
    pub stream: bool,
}

impl ChatRequest {
    pub fn new(messages: Vec<ChatMessage>) -> Self {
        Self {
            messages,
            max_tokens: None,
            temperature: None,
            stream: false,
        }
    }
}

/// Chat completion response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatResponse {
    pub content: String,
    pub model: String,
    pub usage: Option<Usage>,
}

/// Token usage information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Usage {
    pub prompt_tokens: i32,
    pub completion_tokens: i32,
    pub total_tokens: i32,
}

/// Available models for a provider
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelInfo {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
}

/// Stream chunk for chat completion
#[derive(Debug, Clone)]
pub struct ChatStreamChunk {
    pub delta: String,
    pub finish_reason: Option<String>,
}

/// Stream event for chat completion
#[derive(Debug, Clone)]
pub enum ChatStreamEvent {
    /// Content chunk received
    Chunk(ChatStreamChunk),
    /// Stream completed with usage info
    Done(Option<Usage>),
    /// Error occurred
    Error(String),
}
