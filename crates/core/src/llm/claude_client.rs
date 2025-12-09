//! Claude API client wrapper
//! 
//! This module provides a unified HTTP client for Claude API
//! that uses gpui's HttpClient for better integration with the application.

use std::pin::Pin;
use std::sync::Arc;

use anyhow::Result;
use futures::{Stream, StreamExt, TryStreamExt};
use gpui::http_client::{AsyncBody, HttpClient, Request};
use http_body_util::BodyExt;
use serde::{Deserialize, Serialize};

use super::types::{ChatRequest, ChatResponse, ChatStreamChunk, ChatStreamEvent, ProviderConfig, Usage};

/// Claude API client
pub struct ClaudeClient {
    client: Arc<dyn HttpClient>,
}

impl ClaudeClient {
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
        let url = format!("{}/messages", api_base);
        
        // Separate system message from other messages
        let mut system_message: Option<String> = None;
        let mut claude_messages = Vec::new();

        for msg in request.messages {
            if msg.role == "system" {
                system_message = Some(msg.content);
            } else {
                claude_messages.push(ClaudeMessage {
                    role: msg.role,
                    content: msg.content,
                });
            }
        }

        let claude_request = ClaudeChatRequest {
            model: config.model.clone(),
            messages: claude_messages,
            max_tokens: request.max_tokens.or(config.max_tokens).or(Some(4096)),
            temperature: request.temperature.or(config.temperature),
            system: system_message,
        };

        let body_json = serde_json::to_vec(&claude_request)?;
        
        let req = Request::builder()
            .method("POST")
            .uri(&url)
            .header("x-api-key", api_key)
            .header("anthropic-version", "2023-06-01")
            .header("Content-Type", "application/json")
            .body(AsyncBody::from(body_json))?;

        let response = self.client.send(req).await?;

        if !response.status().is_success() {
            let status = response.status();
            anyhow::bail!("Claude API error: {}", status);
        }

        let body = response.into_body();
        let bytes = body.into_data_stream()
            .try_collect::<Vec<_>>()
            .await?
            .concat();
        
        let claude_response: ClaudeChatResponse = serde_json::from_slice(&bytes)?;

        let content = claude_response
            .content
            .first()
            .map(|c| c.text.clone())
            .ok_or_else(|| anyhow::anyhow!("No response from Claude"))?;

        let usage = Usage {
            prompt_tokens: claude_response.usage.input_tokens,
            completion_tokens: claude_response.usage.output_tokens,
            total_tokens: claude_response.usage.input_tokens + claude_response.usage.output_tokens,
        };

        Ok(ChatResponse {
            content,
            model: claude_response.model,
            usage: Some(usage),
        })
    }

    /// Send a chat completion request (streaming)
    pub async fn chat_completion_stream(
        &self,
        api_base: &str,
        api_key: &str,
        request: ChatRequest,
        config: &ProviderConfig,
    ) -> Result<Pin<Box<dyn Stream<Item = ChatStreamEvent> + Send>>> {
        let url = format!("{}/messages", api_base);
        
        // Separate system message from other messages
        let mut system_message: Option<String> = None;
        let mut claude_messages = Vec::new();

        for msg in request.messages {
            if msg.role == "system" {
                system_message = Some(msg.content);
            } else {
                claude_messages.push(ClaudeMessage {
                    role: msg.role,
                    content: msg.content,
                });
            }
        }

        let claude_request = ClaudeStreamRequest {
            model: config.model.clone(),
            messages: claude_messages,
            max_tokens: request.max_tokens.or(config.max_tokens).or(Some(4096)),
            temperature: request.temperature.or(config.temperature),
            system: system_message,
            stream: true,
        };

        let body_json = serde_json::to_vec(&claude_request)?;
        
        let req = Request::builder()
            .method("POST")
            .uri(&url)
            .header("x-api-key", api_key)
            .header("anthropic-version", "2023-06-01")
            .header("Content-Type", "application/json")
            .body(AsyncBody::from(body_json))?;

        let response = self.client.send(req).await?;

        if !response.status().is_success() {
            let status = response.status();
            anyhow::bail!("Claude API error: {}", status);
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
                        parse_claude_sse_events(&text)
                    }
                    Err(e) => vec![ChatStreamEvent::Error(e.to_string())],
                }
            })
            .flat_map(futures::stream::iter);
        
        Ok(Box::pin(event_stream))
    }

    /// Test connection
    pub async fn test_connection(&self, api_base: &str, api_key: &str, model: &str) -> Result<bool> {
        let url = format!("{}/messages", api_base);

        let test_request = ClaudeChatRequest {
            model: model.to_string(),
            messages: vec![ClaudeMessage {
                role: "user".to_string(),
                content: "Hi".to_string(),
            }],
            max_tokens: Some(10),
            temperature: None,
            system: None,
        };

        let body_json = serde_json::to_vec(&test_request)?;
        
        let req = Request::builder()
            .method("POST")
            .uri(&url)
            .header("x-api-key", api_key)
            .header("anthropic-version", "2023-06-01")
            .header("Content-Type", "application/json")
            .body(AsyncBody::from(body_json))?;

        let response = self.client.send(req).await?;
        Ok(response.status().is_success())
    }
}

// Claude-specific types

#[derive(Debug, Serialize)]
struct ClaudeChatRequest {
    model: String,
    messages: Vec<ClaudeMessage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_tokens: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    temperature: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    system: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ClaudeMessage {
    role: String,
    content: String,
}

#[derive(Debug, Deserialize)]
struct ClaudeChatResponse {
    content: Vec<ContentBlock>,
    model: String,
    usage: ClaudeUsage,
}

#[derive(Debug, Deserialize)]
struct ContentBlock {
    text: String,
}

#[derive(Debug, Deserialize)]
struct ClaudeUsage {
    input_tokens: i32,
    output_tokens: i32,
}

#[derive(Debug, Serialize)]
struct ClaudeStreamRequest {
    model: String,
    messages: Vec<ClaudeMessage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_tokens: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    temperature: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    system: Option<String>,
    stream: bool,
}

#[derive(Debug, Deserialize)]
struct StreamEvent {
    #[serde(rename = "type")]
    event_type: String,
    #[serde(default)]
    delta: Option<StreamDelta>,
    #[serde(default)]
    usage: Option<ClaudeUsage>,
}

#[derive(Debug, Deserialize)]
struct StreamDelta {
    #[serde(default)]
    text: Option<String>,
}

fn parse_claude_sse_events(text: &str) -> Vec<ChatStreamEvent> {
    let mut events = Vec::new();

    for line in text.lines() {
        if let Some(data) = line.strip_prefix("data: ") {
            match serde_json::from_str::<StreamEvent>(data) {
                Ok(event) => {
                    match event.event_type.as_str() {
                        "content_block_delta" => {
                            if let Some(delta) = event.delta {
                                if let Some(text) = delta.text {
                                    if !text.is_empty() {
                                        events.push(ChatStreamEvent::Chunk(ChatStreamChunk {
                                            delta: text,
                                            finish_reason: None,
                                        }));
                                    }
                                }
                            }
                        }
                        "message_delta" => {
                            let usage = event.usage.map(|u| Usage {
                                prompt_tokens: u.input_tokens,
                                completion_tokens: u.output_tokens,
                                total_tokens: u.input_tokens + u.output_tokens,
                            });
                            events.push(ChatStreamEvent::Done(usage));
                        }
                        "message_stop" => {
                            events.push(ChatStreamEvent::Done(None));
                        }
                        "error" => {
                            events.push(ChatStreamEvent::Error("Claude API error".to_string()));
                        }
                        _ => {}
                    }
                }
                Err(e) => {
                    tracing::debug!("Failed to parse Claude SSE data: {}", e);
                }
            }
        }
    }

    events
}
