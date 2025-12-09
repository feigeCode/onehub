use std::pin::Pin;

use anyhow::Result;
use async_trait::async_trait;
use futures::Stream;

use super::types::{ChatRequest, ChatResponse, ChatStreamEvent, ModelInfo, ProviderConfig};

/// LLM Provider trait - all providers must implement this interface
#[async_trait]
pub trait LlmProvider: Send + Sync {
    /// Get provider configuration
    fn config(&self) -> &ProviderConfig;

    /// Test connection to the provider
    async fn test_connection(&self) -> Result<bool>;

    /// List available models from the provider
    async fn list_models(&self) -> Result<Vec<ModelInfo>>;

    /// Send chat completion request
    async fn chat(&self, request: ChatRequest) -> Result<ChatResponse>;

    /// Send streaming chat completion request
    async fn chat_stream(
        &self,
        request: ChatRequest,
    ) -> Result<Pin<Box<dyn Stream<Item = ChatStreamEvent> + Send>>>;

    /// Validate API key format (optional, provider-specific)
    fn validate_api_key(&self, api_key: &str) -> Result<()> {
        if api_key.is_empty() {
            anyhow::bail!("API key cannot be empty");
        }
        Ok(())
    }

    /// Get default API base URL for this provider
    fn default_api_base(&self) -> Option<String> {
        None
    }

    /// Get default models for this provider
    fn default_models(&self) -> Vec<ModelInfo> {
        vec![]
    }
}
