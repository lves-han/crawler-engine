use error::ModuleError;
use crate::sync::{SyncTrait, redis::RedisSync};
use crate::model::{Request, Response};
use error::Result;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use utils::redis_lock::DistributedLockManager;

/// 同步服务，负责处理错误计数和状态同步
pub struct SyncService {
    pub synchronizer: Arc<dyn SyncTrait>, // 用于状态同步
    locker: Arc<DistributedLockManager>,
}

impl SyncService {
    pub fn new(cache_pool: Arc<deadpool_redis::Pool>, locker: Arc<DistributedLockManager>) -> Self {
        Self {
            synchronizer: Arc::new(RedisSync::new(
                Arc::clone(&cache_pool),
                Arc::clone(&locker),
                "crawler_cache",
                60 * 60,
            )), // 默认1小时过期
            locker,
        }
    }

    /// 记录请求错误
    pub async fn record_request_error(&self, request: &Request) {
        self.locker
            .with_lock("error_times", 10, Duration::from_secs(10), async {
                self.synchronizer
                    .record_error(&request.task_id())
                    .await
                    .ok();
                self.synchronizer
                    .record_error(&request.module_id())
                    .await
                    .ok();
                // self.synchronizer
                //     .record_error(&request.id.to_string())
                //     .await
                //     .ok();
            })
            .await
            .ok();
    }

    /// 记录请求错误
    pub async fn record_response_error(&self, response: &Response) {
        self.locker
            .with_lock("error_times", 10, Duration::from_secs(10), async {
                self.synchronizer
                    .record_error(&response.task_id())
                    .await
                    .ok();
                self.synchronizer
                    .record_error(&response.module_id())
                    .await
                    .ok();
                // self.synchronizer
                //     .record_error(&response.id.to_string())
                //     .await
                //     .ok();
            })
            .await
            .ok();
    }

    /// 获取错误次数
    pub async fn get_error_times(&self, id: &str) -> usize {
        self.locker
            .acquire_lock("error_times", 10, Duration::from_secs(10))
            .await
            .ok();

        let error_times =
            if let Ok(Some(error_times)) = self.synchronizer.sync(id, "error_times").await {
                error_times.as_u64().unwrap_or(0) as usize
            } else {
                0
            };

        self.locker.release_lock("error_times").await.ok();
        error_times
    }
    pub async fn get_module_error_times(&self, module_id: &str) -> usize {
        self.get_error_times(module_id).await
    }
    pub async fn get_task_error_times(&self, task_id: &str) -> usize {
        self.get_error_times(task_id).await
    }
    /// 同步任务状态
    pub async fn sync_task_status(&self, task_id: &str, error_times: usize) -> Result<()> {
        self.synchronizer
            .send(
                task_id,
                "error_times",
                &serde_json::Value::from(error_times),
            )
            .await?;
        Ok(())
    }

    /// 加载任务状态
    pub async fn load_task_status(&self, task_id: &str) -> usize {
        if let Ok(Some(error_times)) = self.synchronizer.sync(task_id, "error_times").await {
            error_times.as_i64().unwrap_or(0) as usize
        } else {
            0
        }
    }

    /// 同步模块状态
    pub async fn sync_module_status(
        &self,
        module_id: &str,
        error_times: usize,
        finished: bool,
    ) -> Result<()> {
        self.synchronizer
            .send(
                module_id,
                "error_times",
                &serde_json::Value::from(error_times),
            )
            .await?;
        self.synchronizer
            .send(module_id, "finished", &serde_json::Value::from(finished))
            .await?;
        Ok(())
    }

    /// 加载模块状态
    pub async fn load_module_status(&self, module_id: &str) -> (usize, bool) {
        let error_times =
            if let Ok(Some(error_times)) = self.synchronizer.sync(module_id, "error_times").await {
                error_times.as_i64().unwrap_or(0) as usize
            } else {
                0
            };

        let finished =
            if let Ok(Some(finished)) = self.synchronizer.sync(module_id, "finished").await {
                finished.as_bool().unwrap_or(false)
            } else {
                false
            };

        (error_times, finished)
    }

    /// 批量同步配置
    pub async fn sync_configs<T: serde::Serialize>(&self, configs: &[(String, T)]) -> Result<()> {
        for (id, config) in configs {
            let config_value = serde_json::to_value(config).map_err(|e| {
                ModuleError::ConfigSync(format!("Serialization failed: {e}").into())
            })?;
            self.synchronizer.send(id, "config", &config_value).await?;
        }
        Ok(())
    }

    /// 获取同步接口的引用（保持向后兼容）
    pub fn get_sync(&self) -> Arc<dyn SyncTrait> {
        Arc::clone(&self.synchronizer)
    }
    pub async fn lock_module(&self, module_id:&str)  {
        let ts = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs();
        self.synchronizer
            .send(module_id, "module_locker",&serde_json::to_value(ts).unwrap())
            .await
            .ok();
    }
    pub async fn is_module_locker(&self,module_id:&str,ttl:u64) -> bool {
        if let Ok(Some(ts)) = self.synchronizer.sync(module_id, "module_locker").await {
            if let Some(ts) = ts.as_u64() {
                let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs();
                if now - ts < ttl {
                    return true;
                }
            }
        }
        false
    }
    pub async fn release_module_locker(&self,module_id:&str)  {
        self.synchronizer.clear(module_id, "module_locker").await.ok();
    }
}
