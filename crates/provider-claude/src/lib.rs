use std::pin::Pin;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use futures::Stream;
use gpui::http_client::HttpClient;

use one_core::llm::claude_client::ClaudeClient;
use one_core::llm::provider::LlmProvider;
use one_core::llm::types::{
    ChatRequest, ChatResponse, ChatStreamEvent, ModelInfo, ProviderConfig, ProviderType,
};

/// Initialize and register the Claude provider factory
pub fn init() {
    one_core::llm::register_provider(ProviderType::Claude, |config, client| {
        Box::new(ClaudeProvider::new(config, client))
    });
}

pub struct ClaudeProvider {
    config: ProviderConfig,
    client: ClaudeClient,
}

impl ClaudeProvider {
    pub fn new(config: ProviderConfig, client: Arc<dyn HttpClient>) -> Self {
        Self {
            config,
            client: ClaudeClient::new(client),
        }
    }

    fn api_base(&self) -> String {
        self.config
            .api_base
            .clone()
            .unwrap_or_else(|| "https://api.anthropic.com/v1".to_string())
    }

    fn api_key(&self) -> Result<String> {
        self.config
            .api_key
            .clone()
            .ok_or_else(|| anyhow::anyhow!("API key is required for Claude"))
    }
}

#[async_trait]
impl LlmProvider for ClaudeProvider {
    fn config(&self) -> &ProviderConfig {
        &self.config
    }

    async fn test_connection(&self) -> Result<bool> {
        let api_key = self.api_key()?;
        self.client
            .test_connection(&self.api_base(), &api_key, &self.config.model)
            .await
    }

    async fn list_models(&self) -> Result<Vec<ModelInfo>> {
        Ok(self.default_models())
    }

    async fn chat(&self, request: ChatRequest) -> Result<ChatResponse> {
        let api_key = self.api_key()?;
        self.client
            .chat_completion(&self.api_base(), &api_key, request, &self.config)
            .await
    }

    async fn chat_stream(
        &self,
        request: ChatRequest,
    ) -> Result<Pin<Box<dyn Stream<Item = ChatStreamEvent> + Send>>> {
        let api_key = self.api_key()?;
        self.client
            .chat_completion_stream(&self.api_base(), &api_key, request, &self.config)
            .await
    }

    fn validate_api_key(&self, api_key: &str) -> Result<()> {
        if api_key.is_empty() {
            anyhow::bail!("API key cannot be empty");
        }
        if !api_key.starts_with("sk-ant-") {
            anyhow::bail!("Claude API key should start with 'sk-ant-'");
        }
        Ok(())
    }

    fn default_api_base(&self) -> Option<String> {
        Some("https://api.anthropic.com/v1".to_string())
    }

    fn default_models(&self) -> Vec<ModelInfo> {
        vec![
            ModelInfo {
                id: "claude-opus-4-5-20251101".to_string(),
                name: "Claude Opus 4.5".to_string(),
                description: Some("Most capable Claude model".to_string()),
            },
            ModelInfo {
                id: "claude-sonnet-4-5-20250929".to_string(),
                name: "Claude Sonnet 4.5".to_string(),
                description: Some("Balanced performance and speed".to_string()),
            },
            ModelInfo {
                id: "claude-3-5-sonnet-20241022".to_string(),
                name: "Claude 3.5 Sonnet".to_string(),
                description: Some("Previous generation model".to_string()),
            },
            ModelInfo {
                id: "claude-3-5-haiku-20241022".to_string(),
                name: "Claude 3.5 Haiku".to_string(),
                description: Some("Fast and cost-effective".to_string()),
            },
        ]
    }
}
