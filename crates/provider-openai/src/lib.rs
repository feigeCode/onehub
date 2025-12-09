use std::pin::Pin;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use futures::Stream;
use gpui::http_client::HttpClient;

use one_core::llm::openai_client::OpenAIClient;
use one_core::llm::provider::LlmProvider;
use one_core::llm::types::{
    ChatRequest, ChatResponse, ChatStreamEvent, ModelInfo, ProviderConfig, ProviderType,
};

/// Initialize and register the OpenAI provider factory
pub fn init() {
    one_core::llm::register_provider(ProviderType::OpenAI, |config, client| {
        Box::new(OpenAIProvider::new(config, client))
    });
}

pub struct OpenAIProvider {
    config: ProviderConfig,
    client: OpenAIClient,
}

impl OpenAIProvider {
    pub fn new(config: ProviderConfig, client: Arc<dyn HttpClient>) -> Self {
        Self {
            config,
            client: OpenAIClient::new(client),
        }
    }

    fn api_base(&self) -> String {
        self.config
            .api_base
            .clone()
            .unwrap_or_else(|| "https://api.openai.com/v1".to_string())
    }

    fn api_key(&self) -> Result<String> {
        self.config
            .api_key
            .clone()
            .ok_or_else(|| anyhow::anyhow!("API key is required for OpenAI"))
    }
}

#[async_trait]
impl LlmProvider for OpenAIProvider {
    fn config(&self) -> &ProviderConfig {
        &self.config
    }

    async fn test_connection(&self) -> Result<bool> {
        let api_key = self.api_key()?;
        self.client.test_connection(&self.api_base(), &api_key).await
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
        if !api_key.starts_with("sk-") {
            anyhow::bail!("OpenAI API key should start with 'sk-'");
        }
        Ok(())
    }

    fn default_api_base(&self) -> Option<String> {
        Some("https://api.openai.com/v1".to_string())
    }

    fn default_models(&self) -> Vec<ModelInfo> {
        vec![
            ModelInfo {
                id: "gpt-4o".to_string(),
                name: "GPT-4o".to_string(),
                description: Some("Most advanced GPT-4 model".to_string()),
            },
            ModelInfo {
                id: "gpt-4o-mini".to_string(),
                name: "GPT-4o Mini".to_string(),
                description: Some("Faster and more affordable".to_string()),
            },
            ModelInfo {
                id: "gpt-4-turbo".to_string(),
                name: "GPT-4 Turbo".to_string(),
                description: Some("Previous generation model".to_string()),
            },
            ModelInfo {
                id: "gpt-3.5-turbo".to_string(),
                name: "GPT-3.5 Turbo".to_string(),
                description: Some("Fast and cost-effective".to_string()),
            },
        ]
    }
}
