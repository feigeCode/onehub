use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock as StdRwLock};

use anyhow::Result;
use gpui::Global;
use gpui::http_client::HttpClient;
use tokio::sync::RwLock;
use super::provider::LlmProvider;
use super::types::{ProviderConfig, ProviderType};

/// Factory function type for creating provider instances
pub type ProviderFactoryFn = fn(ProviderConfig, Arc<dyn HttpClient>) -> Box<dyn LlmProvider>;

/// Global provider factory registry
static PROVIDER_REGISTRY: OnceLock<StdRwLock<HashMap<ProviderType, ProviderFactoryFn>>> = OnceLock::new();

fn get_registry() -> &'static StdRwLock<HashMap<ProviderType, ProviderFactoryFn>> {
    PROVIDER_REGISTRY.get_or_init(|| StdRwLock::new(HashMap::new()))
}

/// Register a provider factory
pub fn register_provider(provider_type: ProviderType, factory: ProviderFactoryFn) {
    let registry = get_registry();
    let mut map = registry.write().unwrap();
    map.insert(provider_type, factory);
}

/// Provider factory - creates provider instances based on configuration
pub struct ProviderFactory;

impl ProviderFactory {
    pub fn create_provider(config: ProviderConfig, client: Arc<dyn HttpClient>) -> Result<Box<dyn LlmProvider>> {
        let registry = get_registry();
        let map = registry.read().unwrap();

        if let Some(factory) = map.get(&config.provider_type) {
            Ok(factory(config, client))
        } else {
            anyhow::bail!("Provider not registered: {:?}", config.provider_type)
        }
    }

    /// Check if a provider type is registered
    pub fn is_registered(provider_type: ProviderType) -> bool {
        let registry = get_registry();
        let map = registry.read().unwrap();
        map.contains_key(&provider_type)
    }

    /// List all registered provider types
    pub fn registered_types() -> Vec<ProviderType> {
        let registry = get_registry();
        let map = registry.read().unwrap();
        map.keys().cloned().collect()
    }
}

/// Provider manager - manages all active provider instances
pub struct ProviderManager {
    providers: Arc<RwLock<HashMap<i64, Arc<Box<dyn LlmProvider>>>>>,
    client: Arc<dyn HttpClient>,
}

impl ProviderManager {
    pub fn new(client: Arc<dyn HttpClient>) -> Self {
        Self {
            providers: Arc::new(RwLock::new(HashMap::new())),
            client,
        }
    }

    /// Get or create a provider by ID
    pub async fn get_provider(&self, config: ProviderConfig) -> Result<Arc<Box<dyn LlmProvider>>> {
        let id = config.id;
        // Check if provider already exists in cache
        {
            let providers = self.providers.read().await;
            if let Some(provider) = providers.get(&id) {
                return Ok(Arc::clone(provider));
            }
        }
        

        if !config.enabled {
            anyhow::bail!("Provider is disabled: {}", id);
        }

        let provider = Arc::new(ProviderFactory::create_provider(config, Arc::clone(&self.client))?);

        // Cache the provider
        {
            let mut providers = self.providers.write().await;
            providers.insert(id, Arc::clone(&provider));
        }

        Ok(provider)
    }

    /// Remove a provider from cache (e.g., when deleted or disabled)
    pub async fn remove_provider(&self, id: i64) {
        let mut providers = self.providers.write().await;
        providers.remove(&id);
    }

    /// Clear all cached providers
    pub async fn clear_cache(&self) {
        let mut providers = self.providers.write().await;
        providers.clear();
    }
    
}

/// Global provider manager state
pub struct GlobalProviderState {
    manager: Arc<ProviderManager>,
}

impl Clone for GlobalProviderState {
    fn clone(&self) -> Self {
        Self {
            manager: Arc::clone(&self.manager),
        }
    }
}

impl GlobalProviderState {
    pub fn new(client: Arc<dyn HttpClient>) -> Self {
        Self {
            manager: Arc::new(ProviderManager::new(client)),
        }
    }

    pub fn manager(&self) -> Arc<ProviderManager> {
        Arc::clone(&self.manager)
    }
}

impl Global for GlobalProviderState {
    
}
