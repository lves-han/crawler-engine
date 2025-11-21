use crate::sync::sync_service::SyncService;
use crate::error_tracker::{ErrorTracker, ErrorTrackerConfig};
// no direct filesystem path usage here
use utils::connector::{create_redis_pool, postgres_connection};

use utils::config::Config;

use log::info;
use std::sync::Arc;
use tokio::sync::RwLock;
use utils::distributed_rate_limit::{DistributedSlidingWindowRateLimiter, RateLimitConfig};
use utils::redis_lock::DistributedLockManager;

#[derive(Clone)]
pub struct State {
    pub db: Arc<sea_orm::DatabaseConnection>,
    pub config: Arc<RwLock<Config>>,
    pub sync_service: Arc<SyncService>,
    pub cookie_sync_service: Arc<SyncService>,
    pub locker: Arc<DistributedLockManager>,
    pub limiter: Arc<DistributedSlidingWindowRateLimiter>,
    pub state_sync: Arc<SyncService>, // use SyncService as the global state backend
    pub error_tracker: Arc<ErrorTracker>,
}
impl State {
    pub async fn new(path: &str) -> Self {
        let config = Config::load(path).expect("failed to parse config.toml");

        let db = Arc::new(
            postgres_connection(&config.db.postgres)
                .await
                .expect("Failed to connect to postgres"),
        );
        info!("PostgresSQL database connected successfully");
        let cache_pool =
            Arc::new(create_redis_pool(&config.cache_redis).expect("Failed to connect cache"));
        {
            let mut cnn = cache_pool
                .get()
                .await
                .expect("Failed to get cache connection");
            let _pong: String = redis::cmd("PING")
                .query_async(&mut *cnn)
                .await
                .expect("Failed to ping");
            // todo: 测试环境下每次都删除所有缓存，生产环境下需要注释掉
            // todo: 生产环境需要设计一个缓存清理的机制，尤其是module_error_times task_error_times这种缓存
            // let _: () = redis::cmd("flushall")
            //     .query_async(&mut *cnn)
            //     .await
            //     .expect("Failed to flushall");
        }
        info!("cache pool connect successfully");
        let cookie_pool =
            Arc::new(create_redis_pool(&config.cookie.redis).expect("Failed to connect cookie"));
        info!("cookie pool connect successfully");
        let locker_pool =
            Arc::new(create_redis_pool(&config.locker_redis).expect("Failed to connect locker"));
        info!("locker pool connect successfully");
        let limit_pool =
            Arc::new(create_redis_pool(&config.limit_redis).expect("Failed to connect limit"));
        info!("limit pool connect successfully");

        let locker = Arc::new(DistributedLockManager::new(Arc::clone(&locker_pool)));

        let limiter = Arc::new(DistributedSlidingWindowRateLimiter::new(
            Arc::clone(&limit_pool),
            locker.clone(),
            "crawler_rate_limit",
            RateLimitConfig {
                max_requests_per_second: config.defaults.download.rate_limit,
                window_size_millis: 1000,
            },
        ));
        let sync_service = Arc::new(SyncService::new(cache_pool.clone(), locker.clone()));
        let cookie_sync_service = Arc::new(SyncService::new(cookie_pool.clone(), locker.clone()));
        info!("Redis connection pool created successfully");

        // Use the same Redis-backed SyncService for global state operations
        let state_sync = Arc::clone(&sync_service);
        
        // 初始化错误跟踪器
        let error_tracker_config = ErrorTrackerConfig {
            task_max_errors: config.crawler.task_max_errors,
            module_max_errors: config.crawler.module_max_errors,
            request_max_retries: config.crawler.request_max_retries,
            parse_max_retries: config.crawler.request_max_retries, // 使用相同配置
            enable_success_decay: true,
            success_decay_amount: 1,
            enable_time_window: false,
            time_window_seconds: 3600,
            consecutive_error_threshold: 3,
            error_ttl: config.defaults.config.cache_ttl, // 从配置读取错误记录过期时间
        };
        let error_tracker = Arc::new(ErrorTracker::new(
            Arc::clone(&sync_service),
            error_tracker_config,
        ));

        State {
            db,
            config: Arc::new(RwLock::new(config)),
            sync_service,
            cookie_sync_service,
            locker,
            limiter,
            state_sync,
            error_tracker,
        }
    }
}
