//! OpenAI-compatible API utilities
//! 
//! This module provides common types and functions for providers that use
//! OpenAI-compatible APIs (DeepSeek, Qwen, OpenAI, etc.)

use std::pin::Pin;

use anyhow::Result;
use futures::stream::{self, Stream, StreamExt};
use serde::{Deserialize, Serialize};

use super::types::{
    ChatMessage, ChatRequest, ChatResponse, ChatStreamChunk, ChatStreamEvent,
    ProviderConfig, Usage,
};

/// OpenAI-compatible chat request
#[derive(Debug, Serialize)]
pub struct OpenAICompatRequest {
    pub model: String,
    pub messages: Vec<ChatMessage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream: Option<bool>,
}

impl OpenAICompatRequest {
    pub fn from_chat_request(request: ChatRequest, config: &ProviderConfig, stream: bool) -> Self {
        Self {
            model: config.model.clone(),
            messages: request.messages,
            max_tokens: request.max_tokens.or(config.max_tokens),
            temperature: request.temperature.or(config.temperature),
            stream: Some(stream),
        }
    }
}

/// OpenAI-compatible chat response
#[derive(Debug, Deserialize)]
pub struct OpenAICompatResponse {
    pub id: String,
    pub object: String,
    pub created: i64,
    pub model: String,
    pub choices: Vec<Choice>,
    pub usage: Option<OpenAICompatUsage>,
}

#[derive(Debug, Deserialize)]
pub struct Choice {
    pub index: i32,
    pub message: ChatMessage,
    pub finish_reason: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct OpenAICompatUsage {
    pub prompt_tokens: i32,
    pub completion_tokens: i32,
    pub total_tokens: i32,
}

impl From<OpenAICompatUsage> for Usage {
    fn from(u: OpenAICompatUsage) -> Self {
        Usage {
            prompt_tokens: u.prompt_tokens,
            completion_tokens: u.completion_tokens,
            total_tokens: u.total_tokens,
        }
    }
}

impl OpenAICompatResponse {
    pub fn into_chat_response(self) -> Result<ChatResponse> {
        let content = self
            .choices
            .first()
            .map(|c| c.message.content.clone())
            .ok_or_else(|| anyhow::anyhow!("No response content"))?;

        Ok(ChatResponse {
            content,
            model: self.model,
            usage: self.usage.map(Into::into),
        })
    }
}

/// Stream response types
#[derive(Debug, Deserialize)]
pub struct StreamChoice {
    pub index: i32,
    pub delta: StreamDelta,
    pub finish_reason: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct StreamDelta {
    #[serde(default)]
    pub content: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct StreamResponse {
    pub choices: Vec<StreamChoice>,
    pub usage: Option<OpenAICompatUsage>,
}

/// Parse SSE events from OpenAI-compatible API
pub fn parse_sse_events(text: &str) -> Vec<ChatStreamEvent> {
    let mut events = Vec::new();

    for line in text.lines() {
        if let Some(data) = line.strip_prefix("data: ") {
            if data.trim() == "[DONE]" {
                events.push(ChatStreamEvent::Done(None));
                continue;
            }

            match serde_json::from_str::<StreamResponse>(data) {
                Ok(response) => {
                    if let Some(choice) = response.choices.first() {
                        if let Some(content) = &choice.delta.content {
                            if !content.is_empty() {
                                events.push(ChatStreamEvent::Chunk(ChatStreamChunk {
                                    delta: content.clone(),
                                    finish_reason: choice.finish_reason.clone(),
                                }));
                            }
                        }
                        if choice.finish_reason.is_some() {
                            let usage = response.usage.map(Into::into);
                            events.push(ChatStreamEvent::Done(usage));
                        }
                    }
                }
                Err(e) => {
                    tracing::debug!("Failed to parse SSE data: {}", e);
                }
            }
        }
    }

    events
}

/// Create a stream from byte stream using SSE parsing
pub fn create_sse_stream(
    byte_stream: impl Stream<Item = Result<bytes::Bytes, reqwest::Error>> + Send + 'static,
) -> Pin<Box<dyn Stream<Item = ChatStreamEvent> + Send>> {
    let event_stream = byte_stream
        .map(|chunk_result: Result<bytes::Bytes, reqwest::Error>| match chunk_result {
            Ok(bytes) => {
                let text = String::from_utf8_lossy(&bytes);
                parse_sse_events(&text)
            }
            Err(e) => vec![ChatStreamEvent::Error(e.to_string())],
        })
        .flat_map(stream::iter);

    Box::pin(event_stream)
}
