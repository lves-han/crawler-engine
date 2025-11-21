use super::SystemEvent;
use super::event_bus::EventHandler;
use async_trait::async_trait;
use log::{error, warn};
use redis::AsyncCommands;
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;

/// Redis事件处理器，用于将事件保存到Redis供其他程序监控
pub struct RedisEventHandler {
    redis_pool: Arc<deadpool_redis::Pool>,
    key_prefix: String,
    ttl: u64, // 事件数据的TTL（秒）
    max_retries: u32,
}

impl RedisEventHandler {
    pub fn new(redis_pool: Arc<deadpool_redis::Pool>, key_prefix: String, ttl: u64) -> Self {
        Self {
            redis_pool,
            key_prefix,
            ttl,
            max_retries: 3,
        }
    }

    /// 生成Redis键名
    fn generate_key(&self, event_type: &str) -> String {
        format!("{}:events:{}", self.key_prefix, event_type)
    }

    /// 生成统计键名
    fn generate_stats_key(&self, stat_type: &str) -> String {
        format!("{}:stats:{}", self.key_prefix, stat_type)
    }

    /// 生成时间序列键名
    fn generate_timeseries_key(&self, event_type: &str) -> String {
        format!("{}:timeseries:{}", self.key_prefix, event_type)
    }

    /// 保存事件详情
    async fn save_event_details(
        &self,
        event: &SystemEvent,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut conn = self
            .redis_pool
            .get()
            .await
            .map_err(|e| format!("Redis connection failed: {e}"))?;

        let event_data = self.serialize_event(event);
        let event_type = event.event_type();

        // 根据事件类型确定标识符

        let key = self.generate_key(event_type);

        // 保存事件详情，带TTL
        let _: () = timeout(Duration::from_secs(5), async {
            conn.rpush(&key, &event_data).await
        })
        .await??;

        // debug!("Saved event {event_type} to Redis with key: {key}");
        Ok(())
    }

    /// 保存时间序列数据
    async fn save_timeseries_data(
        &self,
        event: &SystemEvent,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut conn = self
            .redis_pool
            .get()
            .await
            .map_err(|e| format!("Redis connection failed: {e}"))?;
        let event_type = event.event_type();
        let timestamp = event.timestamp();

        let ts_key = self.generate_timeseries_key(event_type);

        // 使用有序集合保存时间序列数据
        let score = timestamp;
        let member = json!({
            "timestamp": timestamp,
            "event_type": event_type,
            "data": event
        })
        .to_string();

        let _: () = timeout(Duration::from_secs(5), async {
            redis::cmd("ZADD")
                .arg(&ts_key)
                .arg(score)
                .arg(&member)
                .query_async(&mut *conn)
                .await
        })
        .await??;

        // 移除24小时前的数据
        let cutoff_time = timestamp.saturating_sub(24 * 3600);
        let _: () = redis::cmd("ZREMRANGEBYSCORE")
            .arg(&ts_key)
            .arg(0.0)
            .arg(cutoff_time as f64)
            .query_async(&mut *conn)
            .await?;

        // 设置TTL
        let _: () = conn.expire(&ts_key, 24 * 3600).await?;

        Ok(())
    }

    /// 序列化事件
    fn serialize_event(&self, event: &SystemEvent) -> String {
        json!({
            "event_type": event.event_type(),
            "timestamp": event.timestamp(),
            "data": event,
        })
        .to_string()
    }

    /// 重试机制
    async fn retry_operation<F, Fut>(
        &self,
        operation: F,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>>,
    {
        let mut last_error = None;

        for attempt in 0..self.max_retries {
            match operation().await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    warn!("Redis operation failed on attempt {}: {}", attempt + 1, e);
                    last_error = Some(e);

                    if attempt < self.max_retries - 1 {
                        tokio::time::sleep(Duration::from_millis(100 * (1 << attempt))).await;
                    }
                }
            }
        }

        Err(last_error.unwrap_or_else(|| "All retry attempts failed".into()))
    }
}

#[async_trait]
impl EventHandler for RedisEventHandler {
    async fn handle(
        &self,
        event: &SystemEvent,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // debug!("Processing event for Redis storage: {}", event.event_type());

        // 并发执行所有Redis操作
        let results = tokio::join!(
            self.retry_operation(|| self.save_event_details(event)),
            self.retry_operation(|| self.save_timeseries_data(event)),
        );

        // 检查结果，记录错误但不中断
        if let Err(e) = results.0 {
            error!("Failed to save event details to Redis: {e}");
        }
        if let Err(e) = results.1 {
            error!("Failed to save timeseries data to Redis: {e}");
        }
        Ok(())
    }

    fn name(&self) -> &'static str {
        "RedisEventHandler"
    }
}
