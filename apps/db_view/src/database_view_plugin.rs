


#[async_trait::async_trait]
pub trait DatabaseViewPlugin: Send + Sync {
    
    fn get_name(&self) -> String;
    
    
    
}