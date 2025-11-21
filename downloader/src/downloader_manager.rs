use crate::{Downloader, WebSocketDownloader};
use crate::request_downloader::RequestDownloader;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::RwLock;
use kernel::model::download_config::DownloadConfig;
use kernel::model::Request;
use kernel::state::State;
// 下载器工厂函数类型

pub struct DownloaderManager {
    pub state: Arc<State>,
    pub default_downloader: Box<dyn Downloader>,
    // 注册的下载器工厂
    pub downloader: Arc<RwLock<HashMap<String, Box<dyn Downloader>>>>,
    // task下载器配置
    pub config: Arc<RwLock<HashMap<String, DownloadConfig>>>,
    // task下载器实例
    pub task_downloader: Arc<RwLock<HashMap<String, Box<dyn Downloader>>>>,
    downloader_expire: Arc<RwLock<HashMap<String, u64>>>,
    pub wss_downloader: Arc<WebSocketDownloader>,
}

impl DownloaderManager {
    pub fn new(state: Arc<State>) -> Self {
        DownloaderManager {
            state:state.clone(),
            default_downloader: Box::new(RequestDownloader::new(
                Arc::clone(&state.limiter),
                Arc::clone(&state.locker),
                Arc::clone(&state.sync_service),
            )),
            //下载器工厂列表
            downloader: Arc::new(RwLock::new(HashMap::new())),
            //task下载器配置
            config: Arc::new(RwLock::new(HashMap::new())),
            //task下载器实例
            task_downloader: Arc::new(RwLock::new(HashMap::new())),
            downloader_expire: Arc::new(RwLock::new(HashMap::new())),
            wss_downloader: Arc::new(WebSocketDownloader::new()),
        }
    }

    /// 注册下载器工厂
    pub async fn register(&self, downloader: Box<dyn Downloader>) {
        self.downloader
            .write()
            .await
            .insert(downloader.name(), downloader);
    }

    // pub async fn set_config(&self, id: &str, config: DownloadConfig) {
    //     self.task_downloader
    //         .write()
    //         .await
    //         .get(id)
    //         .map(async |downloader| downloader.set_config(id,config).await);
    //     self.config.write().await.insert(id.to_string(), config);
    // }
    pub async fn set_limit(&self, limit_id: &str, limit: f32) {
        if let Some(downloader) = self.task_downloader.read().await.get(limit_id).cloned() {
            downloader.set_limit(limit_id, limit).await;
        }
    }
    // 获取当前时间戳的辅助函数
    fn current_timestamp() -> u64 {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    // 清理过期的下载器
    async fn cleanup_expired_downloader(&self) {
        let downloader_expire = self
            .state
            .config
            .read()
            .await
            .defaults
            .download
            .downloader_expire;

        let current_time = Self::current_timestamp();
        
        // 收集需要删除的key
        let mut expired_keys: Vec<String> = {
            let expire_guard = self.downloader_expire.read().await;
            expire_guard
                .iter()
                .filter_map(|(k, &v)| {
                    if current_time - v > downloader_expire {
                        Some(k.clone())
                    } else {
                        None
                    }
                })
                .collect()
        };

        // 检查健康状态
        let check_list: Vec<(String, Box<dyn Downloader>)> = {
            let task_downloader = self.task_downloader.read().await;
            task_downloader
                .iter()
                .filter(|(k, _)| !expired_keys.contains(k))
                .map(|(k, v)| (k.clone(), dyn_clone::clone_box(v.as_ref())))
                .collect()
        };

        for (key, downloader) in check_list {
            if downloader.health_check().await.is_err() {
                expired_keys.push(key);
            }
        }

        // 删除过期的下载器
        if !expired_keys.is_empty() {
            let mut task_downloader = self.task_downloader.write().await;
            let mut downloader_expire = self.downloader_expire.write().await;

            for key in expired_keys {
                task_downloader.remove(&key);
                downloader_expire.remove(&key);
            }
        }
    }

    pub async fn get_downloader(
        &self,
        request: &Request,
        download_config: DownloadConfig,
    ) -> Box<dyn Downloader> {
        // 清理过期的下载器
        self.cleanup_expired_downloader().await;

        let current_time = Self::current_timestamp();
        let module_id = request.module_id();

        // 获取或插入配置
        let config = {
            let mut config_guard = self.config.write().await;
            config_guard.entry(module_id.clone()).or_insert(download_config).clone()
        };

        // 更新过期时间
        self.downloader_expire
            .write()
            .await
            .insert(module_id.clone(), current_time);

        // 获取或创建下载器
        let downloader = {
            let mut task_downloader = self.task_downloader.write().await;

            
            // downloader.set_config(id, download_config.clone()).await;
            if let Some(existing_downloader) = task_downloader.get(&module_id) {
                dyn_clone::clone_box(existing_downloader.as_ref())
            } else {
                let downloader = {
                    let downloader_guard = self.downloader.read().await;
                    if let Some(registered_downloader) =
                        downloader_guard.get(&config.downloader)
                    {
                        dyn_clone::clone_box(registered_downloader.as_ref())
                    } else {
                        dyn_clone::clone_box(self.default_downloader.as_ref())
                    }
                };
                task_downloader.insert(module_id.clone(), dyn_clone::clone_box(downloader.as_ref()));
                downloader
            }
        };

        // 确保配置是最新的
        downloader.set_config(&request.limit_id, config).await;
        downloader
    }
    // pub async fn get_downloader_with_config(
    //     &self,
    //     id: &str,
    //     config: DownloadConfig,
    // ) -> Box<RequestDownloader> {
    //
    //     self.get_downloader(id, config).await
    // }
    pub async fn clear(&self) {
        self.config.write().await.clear();
        self.task_downloader.write().await.clear();
    }
}
