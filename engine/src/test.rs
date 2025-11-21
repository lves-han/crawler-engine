mod core;
mod prelude;

use std::path::{Path, PathBuf};
use utils::logger;
use utils::logger::{LogSender, LoggerConfig, set_log_sender};
use std::sync::Arc;
use tokio::signal;

#[tokio::main]
async fn main() {

    // 日志用于记录程序的运行信息，不记录数据
    let mut logger_config = LoggerConfig::default();
    // Default: show SQL & bind params while keeping other logs at info
    // You can override with RUST_LOG env var (e.g., RUST_LOG=info,sqlx=trace,sea_orm=trace)
    logger_config.level = "debug,sqlx=error,sea_orm=error".to_string();
    // 只初始化一次，暂时不注入队列 sender（Engine 构造后再补充）
    logger::init_logger(logger_config).await.ok();

    let config_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("config.toml");
    let state = Arc::new(kernel::State::new(config_path.to_str().unwrap()).await);

    // 将配置中的 proxy_path 规范化为绝对路径（相对路径以 engine crate 根目录为基准）
    {
        let base_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let mut cfg = state.config.write().await;
        let proxy_original = cfg.crawler.proxy_path.clone();
        let proxy_normalized = normalize_path(&base_dir, &proxy_original);
        let module_original = cfg.modules.path.clone();
        let module_normalized = normalize_path(&base_dir, &module_original);
        cfg.crawler.proxy_path = proxy_normalized;
        cfg.modules.path = module_normalized;
    }
    

    let engine = core::engine::Engine::new(Arc::clone(&state)).await;
    // 引擎创建完成后动态注入日志队列 sender
    let _ = set_log_sender(LogSender {
        sender: engine.queue_manager.get_log_pop_channel(),
        level: "WARN".to_string(),
    });
    // 并行等待：
    // 1) 引擎运行完成（例如内部收到关闭信号后返回）
    // 2) 收到 Ctrl+C，触发优雅关闭
    let start_fut = engine.start();
    tokio::pin!(start_fut);
    tokio::select! {
        _ = &mut start_fut => {
            // 引擎正常退出
        }
        _ = signal::ctrl_c() => {
            // 收到中断信号，通知引擎优雅退出
            engine.shutdown().await;
            // 等待引擎完成（可选：再次等待 start 完成）
        }
    }
}

// 将传入路径转换为绝对路径：
// 1. 已是绝对路径则返回（尝试 canonicalize）
// 2. 以 base 进行拼接
// 3. 支持简单的 ~/ 前缀展开
fn normalize_path(base: &Path, input: &str) -> String {
    // 处理 ~/
    let expanded = if let Some(stripped) = input.strip_prefix("~/") {
        if let Ok(home) = std::env::var("HOME") { format!("{home}/{stripped}") } else { input.to_string() }
    } else { input.to_string() };

    let p = Path::new(&expanded);
    let abs_candidate = if p.is_absolute() { p.to_path_buf() } else { base.join(p) };
    // canonicalize 若失败则返回拼接结果
    std::fs::canonicalize(&abs_candidate).unwrap_or(abs_candidate).display().to_string()
}
