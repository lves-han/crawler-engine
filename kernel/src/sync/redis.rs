
use error::{SyncError,Result};
use crate::sync::SyncTrait;
use redis::AsyncCommands;
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;
use utils::redis_lock::DistributedLockManager;

#[derive(Debug)]
pub struct RedisSync {
    pool: Arc<deadpool_redis::Pool>,
    key_prefix: String,
    ttl: u64, // 配置过期时间（秒）
    locker: Arc<DistributedLockManager>,
}

impl RedisSync {
    pub fn new(
        pool: Arc<deadpool_redis::Pool>,
        locker: Arc<DistributedLockManager>,
        key_prefix: &str,
        ttl: u64,
    ) -> Self {
        Self {
            pool,
            key_prefix: key_prefix.to_string(),
            ttl,
            locker,
        }
    }

    fn get_key(&self, id: &str, field: &str) -> String {
        format!("{}:{}:{}", self.key_prefix, field, id)
    }
}
#[async_trait::async_trait]
impl SyncTrait for RedisSync {
    fn name(&self) -> String {
        "redis_sync".to_string()
    }

    async fn send(&self, id: &str, field: &str, data: &Value) -> Result<()> {
        self.send_with_ttl(id, field, data, self.ttl).await
    }

    async fn send_with_ttl(&self, id: &str, field: &str, data: &Value, ttl: u64) -> Result<()> {
        let lock_key = format!("lock:{id}:{field}");
        self.locker.acquire_lock_default(&lock_key).await.ok();
        let mut conn = self.pool.get().await.map_err(|e| {
            SyncError::ConnectFailed(format!("Redis connection failed: {e}").into())
        })?;

        let key = self.get_key(id, field);
        let serialized = serde_json::to_string(data)
            .map_err(|e| SyncError::SerdeFailed(format!("Serialization failed: {e}").into()))?;

        let _: () = timeout(Duration::from_secs(5), async {
            conn.set_ex(&key, &serialized, ttl).await
        })
        .await
        .map_err(|_| SyncError::Timeout("Redis operation timeout".into()))?
        .map_err(|e| SyncError::SyncSaveFailed(format!("Redis set failed: {e}").into()))?;
        self.locker.release_lock(&lock_key).await.ok();
        Ok(())
    }

    async fn setnx(&self, id: &str, field: &str, data: &Value, ttl: u64) -> Result<bool> {
        // 使用原子 SET NX EX 实现一次性写入
        let mut conn = self.pool.get().await.map_err(|e| {
            SyncError::ConnectFailed(format!("Redis connection failed: {e}").into())
        })?;
        let key = self.get_key(id, field);
        let serialized = serde_json::to_string(data)
            .map_err(|e| SyncError::SerdeFailed(format!("Serialization failed: {e}").into()))?;

        // SET key value NX EX ttl
        let result: Option<String> = timeout(Duration::from_secs(5), async {
            redis::cmd("SET")
                .arg(&key)
                .arg(&serialized)
                .arg("NX")
                .arg("EX")
                .arg(ttl as usize)
                .query_async(&mut *conn)
                .await
        })
        .await
        .map_err(|_| SyncError::Timeout("Redis operation timeout".into()))?
        .map_err(|e| SyncError::ExecuteFailed(format!("Redis SET NX failed: {e}").into()))?;

        Ok(matches!(result.as_deref(), Some("OK")))
    }

    async fn sync(&self, id: &str, field: &str) -> Result<Option<Value>> {
        let lock_key = format!("lock:{id}:{field}");
        self.locker.acquire_lock_default(&lock_key).await.ok();
        let mut conn = self.pool.get().await.map_err(|e| {
            SyncError::ConnectFailed(format!("Redis connection failed: {e}").into())
        })?;

        let key = self.get_key(id, field);
        let result: Option<String> =
            timeout(Duration::from_secs(5), async { conn.get(&key).await })
                .await
                .map_err(|_| SyncError::Timeout("Redis operation timeout".into()))?
                .map_err(|e| SyncError::SyncLoadFailed(format!("Redis get failed: {e}").into()))?;
        self.locker.release_lock(&lock_key).await.ok();
        match result {
            Some(serialized) => {
                let data = serde_json::from_str(&serialized).map_err(|e| {
                    SyncError::DeserializeFailed(format!("Deserialization failed: {e}").into())
                })?;
                Ok(Some(data))
            }
            None => Ok(None),
        }
    }

    async fn clear(&self, id: &str, field: &str) -> Result<()> {
        let mut conn = self.pool.get().await.map_err(|e| {
            SyncError::ConnectFailed(format!("Redis connection failed: {e}").into())
        })?;

        // 使用 Lua 脚本高效删除匹配的键
        let key_pattern = self.get_key(id, field);
        let lua_script = r#"
            local keys = redis.call('KEYS', ARGV[1])
            if #keys > 0 then
                return redis.call('DEL', unpack(keys))
            else
                return 0
            end
        "#;

        let _: i32 = timeout(Duration::from_secs(5), async {
            redis::Script::new(lua_script)
                .arg(&key_pattern)
                .invoke_async(&mut *conn)
                .await
        })
        .await
        .map_err(|_| SyncError::Timeout("Redis operation timeout".into()))?
        .map_err(|e| SyncError::ExecuteFailed(format!("Redis script failed: {e}").into()))?;

        Ok(())
    }

    async fn record_error(&self, id: &str) -> Result<usize> {
        self.record_error_with_ttl(id, self.ttl).await
    }

    async fn record_error_with_ttl(&self, id: &str, ttl: u64) -> Result<usize> {
        let mut conn = self.pool.get().await.map_err(|e| {
            SyncError::ConnectFailed(format!("Redis connection failed: {e}").into())
        })?;
        let key = self.get_key(id, "error_times");
        
        // 使用 Lua 脚本实现原子性的 INCR + EXPIRE
        // 如果 key 不存在，初始化为 1 并设置 TTL
        // 如果 key 已存在，递增并更新 TTL
        let lua_script = r#"
            local current = redis.call('INCR', KEYS[1])
            redis.call('EXPIRE', KEYS[1], ARGV[1])
            return current
        "#;

        let count: i64 = timeout(Duration::from_secs(5), async {
            redis::Script::new(lua_script)
                .key(&key)
                .arg(ttl as i64)
                .invoke_async(&mut *conn)
                .await
        })
        .await
        .map_err(|_| SyncError::Timeout("Redis operation timeout".into()))?
        .map_err(|e| SyncError::ExecuteFailed(format!("Redis script failed: {e}").into()))?;
        
        Ok(count as usize)
    }

    async fn decrement_error(&self, id: &str, amount: usize) -> Result<usize> {
        let mut conn = self.pool.get().await.map_err(|e| {
            SyncError::ConnectFailed(format!("Redis connection failed: {e}").into())
        })?;
        let key = self.get_key(id, "error_times");
        
        // 使用 Lua 脚本实现原子性的 DECRBY，确保不小于 0
        let lua_script = r#"
            local current = redis.call('GET', KEYS[1])
            if not current then
                return 0
            end
            local val = tonumber(current)
            local decr = tonumber(ARGV[1])
            if val <= 0 then
                return 0
            end
            if val < decr then
                redis.call('SET', KEYS[1], 0)
                return 0
            else
                return redis.call('DECRBY', KEYS[1], decr)
            end
        "#;

        let count: i64 = timeout(Duration::from_secs(5), async {
            redis::Script::new(lua_script)
                .key(&key)
                .arg(amount as i64)
                .invoke_async(&mut *conn)
                .await
        })
        .await
        .map_err(|_| SyncError::Timeout("Redis operation timeout".into()))?
        .map_err(|e| SyncError::ExecuteFailed(format!("Redis script failed: {e}").into()))?;
        
        Ok(count as usize)
    }
}
