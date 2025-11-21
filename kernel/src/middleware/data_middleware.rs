use crate::middleware::{DataMiddleware,};
use crate::model::data::Data;
use crate::model::ModuleConfig;
use async_trait::async_trait;
use error::Result;
use std::sync::Arc;

#[async_trait]
pub trait DataStoreMiddleware: DataMiddleware {
    async fn store_data(&self, data: Data, config: &Option<ModuleConfig>) -> Result<()>;
    fn default_arc() -> Arc<dyn DataStoreMiddleware>
    where
        Self: Sized;
}
