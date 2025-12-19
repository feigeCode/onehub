use anyhow::Result;
use gpui::{App, Global};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::SqlitePool;
use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tracing::log;
use crate::gpui_tokio::Tokio;

/// Storage manager - unified entry point for all repositories
pub struct StorageManager {
    pool: Arc<RwLock<SqlitePool>>,
    repositories: Arc<RwLock<HashMap<TypeId, Box<dyn Any + Send + Sync>>>>,
}

pub struct  GlobalStorageState {
    pub storage: StorageManager,
}

impl Global for GlobalStorageState {}

impl Clone for GlobalStorageState {
    fn clone(&self) -> Self {
        GlobalStorageState {
            storage: self.storage.clone(),
        }
    }
}

impl StorageManager {
    /// Create a new storage manager
    pub async fn new() -> Result<Self> {
        let database_url = get_db_path()?;
        let options = SqliteConnectOptions::from_str(&database_url)?
            .create_if_missing(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(options)
            .await?;
        let manager = Self {
            pool: Arc::new(RwLock::new(pool)),
            repositories: Arc::new(RwLock::new(HashMap::new())),
        };
        Ok(manager)
    }

    /// Register a repository
    pub async fn register<R>(&self, repo: R) -> Result<()>
    where
        R: 'static + Send + Sync,
    {

        let type_id = TypeId::of::<R>();
        let mut repos = self.repositories.write().await;
        repos.insert(type_id, Box::new(repo));
        Ok(())
    }

    /// Get a repository by type
    pub async fn get<R>(&self) -> Option<Arc<R>>
    where
        R: 'static + Clone + Send + Sync,
    {
        let type_id = TypeId::of::<R>();
        let repos = self.repositories.read().await;
        repos.get(&type_id).and_then(|r| {
            r.downcast_ref::<R>().map(|repo| Arc::new(repo.clone()))
        })
    }

    pub async fn get_pool(&self) -> Result<SqlitePool>  {
        let pool = self.pool.read().await;
        Ok(pool.clone())
    }
}

impl Clone for StorageManager {
    fn clone(&self) -> Self {
        Self {
            pool: Arc::clone(&self.pool),
            repositories: Arc::clone(&self.repositories),
        }
    }
}

pub fn get_db_path() -> Result<String> {
    let config_dir = get_config_dir()?;
    let db_path = config_dir.join("one-hub.db");
    Ok(format!("sqlite://{}",db_path.display()))
}

pub fn get_config_dir() -> Result<PathBuf> {
    // Use platform-specific config directory
    let config_dir = if cfg!(target_os = "macos") {
        dirs::home_dir()
            .ok_or_else(|| anyhow::anyhow!("Could not find home directory"))?
            .join(".config")
            .join("one-hub")
    } else if cfg!(target_os = "windows") {
        dirs::config_dir()
            .ok_or_else(|| anyhow::anyhow!("Could not find config directory"))?
            .join("one-hub")
    } else {
        dirs::home_dir()
            .ok_or_else(|| anyhow::anyhow!("Could not find home directory"))?
            .join(".config")
            .join("one-hub")
    };

    Ok(config_dir)
}

pub fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

/// Initialize storage manager and set as global
pub fn init(cx: &mut App) {
    let global_storage_state = Tokio::block_on(cx, async move {
        match StorageManager::new().await {
            Ok(manager) => {
                GlobalStorageState { storage: manager }
            }
            Err(err) => {
                log::error!("Failed to initialize storage manager: {}", err);
                panic!("Failed to initialize storage manager: {}", err);
            }
        }
    });
    cx.set_global(global_storage_state)
}



