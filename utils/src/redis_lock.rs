#![allow(unused)]
use deadpool_redis::{Config, Runtime};
use log::trace;
use redis::AsyncCommands;
use redis::Script;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tokio::time::sleep;
use tracing::error;
use uuid::Uuid;


#[derive(Debug)]
pub enum LockError {
    Redis(redis::RedisError),
    Pool(deadpool_redis::PoolError),
    Timeout,
    InvalidOperation(String),
}

impl std::fmt::Display for LockError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LockError::Redis(e) => write!(f, "Redis error: {e}"),
            LockError::Pool(e) => write!(f, "Pool error: {e}"),
            LockError::Timeout => write!(f, "Lock operation timed out"),
            LockError::InvalidOperation(msg) => write!(f, "Invalid operation: {msg}"),
        }
    }
}

impl std::error::Error for LockError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            LockError::Redis(e) => Some(e),
            LockError::Pool(e) => Some(e),
            _ => None,
        }
    }
}

impl From<redis::RedisError> for LockError {
    fn from(error: redis::RedisError) -> Self {
        LockError::Redis(error)
    }
}

impl From<deadpool_redis::PoolError> for LockError {
    fn from(error: deadpool_redis::PoolError) -> Self {
        LockError::Pool(error)
    }
}

// 简化的锁信息结构
#[derive(Debug, Clone)]
pub struct LockInfo {
    key: String,
    value: String,
    ttl: u64,
    created_at: Instant,
}

pub struct AdvancedDistributedLock {
    pool: Arc<deadpool_redis::Pool>,
    lock_info: LockInfo,
    renewal_handle: Option<tokio::task::JoinHandle<()>>,
}

impl AdvancedDistributedLock {
    /// 尝试获取锁，支持重试
    pub async fn acquire_with_retry(
        pool: Arc<deadpool_redis::Pool>,
        lock_key: String,
        ttl_seconds: u64,
        retry_interval: Duration,
        max_wait: Duration,
    ) -> Result<Option<Self>, LockError> {
        let unique_value = Uuid::now_v7().to_string();
        let start_time = Instant::now();

        loop {
            let acquired = Self::try_acquire(&pool, &lock_key, &unique_value, ttl_seconds).await?;

            if acquired {
                let lock_info = LockInfo {
                    key: lock_key,
                    value: unique_value,
                    ttl: ttl_seconds,
                    created_at: Instant::now(),
                };

                let mut lock = Self {
                    pool: pool.clone(),
                    lock_info,
                    renewal_handle: None,
                };

                // 启动自动续期
                lock.start_renewal().await;
                return Ok(Some(lock));
            }

            if start_time.elapsed() >= max_wait {
                return Ok(None); // 超时
            }

            sleep(retry_interval).await;
        }
    }

    async fn try_acquire(
        pool: &deadpool_redis::Pool,
        key: &str,
        value: &str,
        ttl: u64,
    ) -> Result<bool, LockError> {
        let mut conn = pool.get().await?;

        // 使用SET NX EX命令实现原子操作
        let script = r#"
            return redis.call("SET", KEYS[1], ARGV[1], "NX", "EX", ARGV[2])
        "#;

        let result: Option<String> = Script::new(script)
            .key(key)
            .arg(value)
            .arg(ttl)
            .invoke_async(&mut *conn)
            .await?;

        Ok(result.is_some())
    }

    /// 启动自动续期任务
    async fn start_renewal(&mut self) {
        let pool = self.pool.clone();
        let key = self.lock_info.key.clone();
        let value = self.lock_info.value.clone();
        let ttl = self.lock_info.ttl;

        let handle = tokio::spawn(async move {
            let renewal_interval = Duration::from_secs(ttl / 3); // 每 1/3 TTL 续期一次

            loop {
                sleep(renewal_interval).await;

                let script = r#"
                    if redis.call("GET", KEYS[1]) == ARGV[1] then
                        return redis.call("EXPIRE", KEYS[1], ARGV[2])
                    else
                        return 0
                    end
                "#;

                match pool.get().await {
                    Ok(mut conn) => {
                        let result: Result<i32, _> = Script::new(script)
                            .key(&key)
                            .arg(&value)
                            .arg(ttl)
                            .invoke_async(&mut *conn)
                            .await;

                        match result {
                            Ok(1) => {
                                trace!("锁续期成功: {key}");
                            }
                            Ok(0) => {
                                trace!("锁已失效，停止续期: {key}");
                                break;
                            }
                            Ok(_) => {
                                trace!("续期返回意外值: {key}");
                                break;
                            }
                            Err(e) => {
                                trace!("续期失败: {e}");
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        trace!("获取连接失败，停止续期: {e}");
                        break;
                    }
                }
            }
        });

        self.renewal_handle = Some(handle);
    }

    pub async fn release(mut self) -> Result<bool, LockError> {
        // 停止续期任务
        if let Some(handle) = self.renewal_handle.take() {
            handle.abort();
        }

        let script = r#"
            if redis.call("GET", KEYS[1]) == ARGV[1] then
                return redis.call("DEL", KEYS[1])
            else
                return 0
            end
        "#;

        let mut conn = self.pool.get().await?;
        let result: i32 = Script::new(script)
            .key(&self.lock_info.key)
            .arg(&self.lock_info.value)
            .invoke_async(&mut *conn)
            .await?;

        Ok(result == 1)
    }

    /// 检查锁是否仍然有效
    pub async fn is_valid(&self) -> Result<bool, LockError> {
        let mut conn = self.pool.get().await?;
        let current_value: Option<String> = conn.get(&self.lock_info.key).await?;
        Ok(current_value.as_ref() == Some(&self.lock_info.value))
    }
}

impl std::fmt::Debug for AdvancedDistributedLock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdvancedDistributedLock")
            .field("lock_info", &self.lock_info)
            .field("renewal_handle", &self.renewal_handle.is_some())
            .finish()
    }
}

impl Drop for AdvancedDistributedLock {
    fn drop(&mut self) {
        if let Some(handle) = self.renewal_handle.take() {
            handle.abort();
        }
    }
}
#[derive(Debug)]
pub struct DistributedLockManager {
    redis_pool: Arc<deadpool_redis::Pool>,
    locks: Arc<RwLock<HashMap<String, AdvancedDistributedLock>>>,
}

impl DistributedLockManager {
    pub fn new(pool: Arc<deadpool_redis::Pool>) -> Self {
        Self {
            redis_pool: pool,
            locks: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn acquire_lock(
        &self,
        lock_name: &str,
        ttl_seconds: u64,
        max_wait: Duration,
    ) -> Result<bool, LockError> {
        if let Some(lock) = AdvancedDistributedLock::acquire_with_retry(
            self.redis_pool.clone(),
            lock_name.to_string(),
            ttl_seconds,
            Duration::from_millis(100),
            max_wait,
        )
        .await?
        {
            let mut locks = self.locks.write().await;
            locks.insert(lock_name.to_string(), lock);
            Ok(true)
        } else {
            Ok(false)
        }
    }
    pub async fn acquire_lock_default(&self, lock_name: &str) -> Result<bool, LockError> {
        if let Some(lock) = AdvancedDistributedLock::acquire_with_retry(
            self.redis_pool.clone(),
            lock_name.to_string(),
            5, // 默认锁定时间为5秒
            Duration::from_millis(100),
            Duration::from_secs(10), // 最大等待时间为10秒
        )
        .await?
        {
            let mut locks = self.locks.write().await;
            locks.insert(lock_name.to_string(), lock);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub async fn release_lock(&self, lock_name: &str) -> Result<bool, LockError> {
        let mut locks = self.locks.write().await;
        if let Some(lock) = locks.remove(lock_name) {
            let released = lock.release().await?;
            Ok(released)
        } else {
            Ok(false) // 锁不存在
        }
    }

    pub async fn with_lock<F, R>(
        &self,
        lock_name: &str,
        ttl_seconds: u64,
        max_wait: Duration,
        f: F,
    ) -> Result<Option<R>, LockError>
    where
        F: std::future::Future<Output = R>,
    {
        if self.acquire_lock(lock_name, ttl_seconds, max_wait).await? {
            let result = f.await;
            self.release_lock(lock_name).await?;
            Ok(Some(result))
        } else {
            Ok(None)
        }
    }

    // 检查锁是否仍然有效
    pub async fn is_lock_valid(&self, lock_name: &str) -> Result<bool, LockError> {
        let locks = self.locks.read().await;
        if let Some(lock) = locks.get(lock_name) {
            lock.is_valid().await
        } else {
            Ok(false)
        }
    }

    // 获取连接池的引用（供其他地方使用）
    pub fn get_pool(&self) -> &deadpool_redis::Pool {
        &self.redis_pool
    }
}

// DistributedLockManager is Send + Sync via its fields (Arc<Pool>, Arc<RwLock<...>>)
// Rely on compiler to enforce/derive thread-safety without unsafe impls.
