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

/// Initialize and register the Qwen provider factory
pub fn init() {
    one_core::llm::register_provider(ProviderType::Qwen, |config, client| {
        Box::new(QwenProvider::new(config, client))
    });
}

pub struct QwenProvider {
    config: ProviderConfig,
    client: OpenAIClient,
}

impl QwenProvider {
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
            .unwrap_or_else(|| "https://dashscope.aliyuncs.com/compatible-mode/v1".to_string())
    }

    fn api_key(&self) -> Result<String> {
        self.config
            .api_key
            .clone()
            .ok_or_else(|| anyhow::anyhow!("API key is required for Qwen"))
    }
}

#[async_trait]
impl LlmProvider for QwenProvider {
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

    fn default_api_base(&self) -> Option<String> {
        Some("https://dashscope.aliyuncs.com/compatible-mode/v1".to_string())
    }

    fn default_models(&self) -> Vec<ModelInfo> {
        vec![
            ModelInfo {
                id: "qwen-max".to_string(),
                name: "Qwen Max".to_string(),
                description: Some("Most capable Qwen model".to_string()),
            },
            ModelInfo {
                id: "qwen-plus".to_string(),
                name: "Qwen Plus".to_string(),
                description: Some("Balanced performance".to_string()),
            },
            ModelInfo {
                id: "qwen-turbo".to_string(),
                name: "Qwen Turbo".to_string(),
                description: Some("Fast and cost-effective".to_string()),
            },
            ModelInfo {
                id: "qwen-long".to_string(),
                name: "Qwen Long".to_string(),
                description: Some("Long context support".to_string()),
            },
        ]
    }
}
