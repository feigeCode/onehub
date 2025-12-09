//! OpenAI-compatible HTTP client wrapper
//! 
//! This module provides a unified HTTP client for OpenAI-compatible APIs
//! that uses gpui's HttpClient for better integration with the application.

use std::pin::Pin;
use std::sync::Arc;

use anyhow::Result;
use futures::{Stream, StreamExt, TryStreamExt};
use gpui::http_client::{AsyncBody, HttpClient, Request};
use http_body_util::BodyExt;
use serde::Serialize;

use super::openai_compat::{OpenAICompatRequest, OpenAICompatResponse, parse_sse_events};
use super::types::{ChatRequest, ChatResponse, ChatStreamEvent, ProviderConfig};

/// OpenAI-compatible API client
pub struct OpenAIClient {
    client: Arc<dyn HttpClient>,
}

impl OpenAIClient {
    /// Create a new client with the given HttpClient
    pub fn new(client: Arc<dyn HttpClient>) -> Self {
        Self { client }
    }

    /// Send a chat completion request (non-streaming)
    pub async fn chat_completion(
        &self,
        api_base: &str,
        api_key: &str,
        request: ChatRequest,
        config: &ProviderConfig,
    ) -> Result<ChatResponse> {
        let url = format!("{}/chat/completions", api_base);
        let compat_request = OpenAICompatRequest::from_chat_request(request, config, false);

        let response = self
            .post_json(&url, api_key, &compat_request)
            .await?;

        let compat_response: OpenAICompatResponse = serde_json::from_slice(&response)?;
        compat_response.into_chat_response()
    }

    /// Send a chat completion request (streaming)
    pub async fn chat_completion_stream(
        &self,
        api_base: &str,
        api_key: &str,
        request: ChatRequest,
        config: &ProviderConfig,
    ) -> Result<Pin<Box<dyn Stream<Item = ChatStreamEvent> + Send>>> {
        let url = format!("{}/chat/completions", api_base);
        let compat_request = OpenAICompatRequest::from_chat_request(request, config, true);

        let body_json = serde_json::to_vec(&compat_request)?;
        
        let req = Request::builder()
            .method("POST")
            .uri(&url)
            .header("Authorization", format!("Bearer {}", api_key))
            .header("Content-Type", "application/json")
            .body(AsyncBody::from(body_json))?;

        let response = self.client.send(req).await?;

        if !response.status().is_success() {
            let status = response.status();
            anyhow::bail!("API error: {}", status);
        }

        // Convert response body to stream
        let body = response.into_body();
        let byte_stream = body.into_data_stream();
        
        // Parse SSE events from byte stream
        let event_stream = byte_stream
            .map(|chunk_result| {
                match chunk_result {
                    Ok(bytes) => {
                        let text = String::from_utf8_lossy(&bytes);
                        parse_sse_events(&text)
                    }
                    Err(e) => vec![ChatStreamEvent::Error(e.to_string())],
                }
            })
            .flat_map(futures::stream::iter);
        
        Ok(Box::pin(event_stream))
    }

    /// Test connection by listing models
    pub async fn test_connection(&self, api_base: &str, api_key: &str) -> Result<bool> {
        let url = format!("{}/models", api_base);
        
        let req = Request::builder()
            .method("GET")
            .uri(&url)
            .header("Authorization", format!("Bearer {}", api_key))
            .body(AsyncBody::default())?;

        let response = self.client.send(req).await?;
        Ok(response.status().is_success())
    }

    /// Helper method to send POST request with JSON body
    async fn post_json<T: Serialize>(&self, url: &str, api_key: &str, body: &T) -> Result<Vec<u8>> {
        let body_json = serde_json::to_vec(body)?;
        
        let req = Request::builder()
            .method("POST")
            .uri(url)
            .header("Authorization", format!("Bearer {}", api_key))
            .header("Content-Type", "application/json")
            .body(AsyncBody::from(body_json))?;

        let response = self.client.send(req).await?;

        if !response.status().is_success() {
            let status = response.status();
            anyhow::bail!("API error: {}", status);
        }

        let body = response.into_body();
        let bytes = body.into_data_stream()
            .try_collect::<Vec<_>>()
            .await?
            .concat();
        Ok(bytes)
    }
}
