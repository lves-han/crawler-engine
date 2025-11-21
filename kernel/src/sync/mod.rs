
use error::{SyncError,Result};
use serde::Serialize;
use std::fmt::Debug;
use std::sync::Arc;

pub mod redis;
pub mod sync_service;

#[async_trait::async_trait]
pub trait SyncTrait: Send + Sync + Debug {
    fn name(&self) -> String;
    /// 需要内部默认实现一个对id+field分布式锁，在加锁的情况下执行同步操作
    async fn send(&self, id: &str, field: &str, data: &serde_json::Value) -> Result<()>;
    async fn send_with_ttl(
        &self,
        id: &str,
        field: &str,
        data: &serde_json::Value,
        ttl: u64,
    ) -> Result<()>;
    /// 原子：仅当 key 不存在时写入，并设置 TTL；返回是否写入成功
    async fn setnx(
        &self,
        id: &str,
        field: &str,
        data: &serde_json::Value,
        ttl: u64,
    ) -> Result<bool>;
    /// 需要内部默认实现一个对id+field分布式锁，在加锁的情况下执行同步操作
    /// 这样可以避免多个任务同时同步同一个id+field导致数据覆盖的问题
    async fn sync(&self, id: &str, field: &str) -> Result<Option<serde_json::Value>>;
    async fn clear(&self, id: &str, field: &str) -> Result<()>;
    async fn record_error(&self, id: &str) -> Result<usize>;
    async fn record_error_with_ttl(&self, id: &str, ttl: u64) -> Result<usize>;
    async fn decrement_error(&self, id: &str, amount: usize) -> Result<usize>;
}
#[async_trait::async_trait]
pub trait SyncAble: Send + Sync + Debug
where
    Self: Sized + Serialize + for<'de> serde::Deserialize<'de>,
{
    fn field() -> String;
    async fn sync(id: &str, synchronizer: Arc<dyn SyncTrait>) -> Result<Option<Self>> {
        let data = synchronizer.sync(id, &Self::field()).await;
        data.and_then(|data| {
            data.map(|x| {
                serde_json::from_value(x).map_err(|e| SyncError::DeserializeFailed(e.into()).into())
            })
            .transpose()
        })
    }
    async fn send(&self, id: &str, synchronizer: Arc<dyn SyncTrait>) -> Result<()> {
        let data = serde_json::to_value(self).map_err(|e| SyncError::SerdeFailed(e.into()))?;
        synchronizer.send(id, &Self::field(), &data).await
    }
}
