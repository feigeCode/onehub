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

/// Initialize and register the Ollama provider factory
pub fn init() {
    one_core::llm::register_provider(ProviderType::Ollama, |config, client| {
        Box::new(OllamaProvider::new(config, client))
    });
}

pub struct OllamaProvider {
    config: ProviderConfig,
    client: OpenAIClient,
}

impl OllamaProvider {
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
            .unwrap_or_else(|| "http://localhost:11434/v1".to_string())
    }
}

#[async_trait]
impl LlmProvider for OllamaProvider {
    fn config(&self) -> &ProviderConfig {
        &self.config
    }

    async fn test_connection(&self) -> Result<bool> {
        // Ollama doesn't require API key, just test the endpoint
        self.client.test_connection(&self.api_base(), "").await
    }

    async fn list_models(&self) -> Result<Vec<ModelInfo>> {
        // Ollama has a custom models API, keep this implementation
        // TODO: Could use OpenAI-compatible /v1/models endpoint in the future
        Ok(self.default_models())
    }

    async fn chat(&self, request: ChatRequest) -> Result<ChatResponse> {
        // Ollama doesn't require API key
        self.client
            .chat_completion(&self.api_base(), "", request, &self.config)
            .await
    }

    async fn chat_stream(
        &self,
        request: ChatRequest,
    ) -> Result<Pin<Box<dyn Stream<Item = ChatStreamEvent> + Send>>> {
        // Ollama doesn't require API key
        self.client
            .chat_completion_stream(&self.api_base(), "", request, &self.config)
            .await
    }

    fn validate_api_key(&self, _api_key: &str) -> Result<()> {
        // Ollama doesn't require API key
        Ok(())
    }

    fn default_api_base(&self) -> Option<String> {
        Some("http://localhost:11434/v1".to_string())
    }

    fn default_models(&self) -> Vec<ModelInfo> {
        vec![
            ModelInfo {
                id: "llama3.1:latest".to_string(),
                name: "Llama 3.1".to_string(),
                description: Some("Meta's latest Llama model".to_string()),
            },
            ModelInfo {
                id: "mistral:latest".to_string(),
                name: "Mistral".to_string(),
                description: Some("Mistral AI model".to_string()),
            },
            ModelInfo {
                id: "codellama:latest".to_string(),
                name: "Code Llama".to_string(),
                description: Some("Code generation model".to_string()),
            },
        ]
    }
}
