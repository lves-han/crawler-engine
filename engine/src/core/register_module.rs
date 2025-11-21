#![allow(unused)]
use error::{DynLibError, Result};
use kernel::TaskManager;
use libloading::Library;
use log::{error, info, warn};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;

struct LoadedLib {
    _lib: Library,
    _path: PathBuf,
}

type RegisterFutureFn = extern "Rust" fn(Arc<TaskManager>) -> std::pin::Pin<Box<dyn Future<Output=()> + Send + 'static>>;

// Platform-specific dynamic library extension (without dot)
#[cfg(target_os = "macos")]
const DYLIB_EXT: &str = "dylib";
#[cfg(target_os = "linux")]
const DYLIB_EXT: &str = "so";
#[cfg(target_os = "windows")]
const DYLIB_EXT: &str = "dll";
// Fallback for other targets (treat as macOS style by default)
#[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
const DYLIB_EXT: &str = "dylib";

fn is_dynamic_lib(path: &Path) -> bool {
    path.extension()
        .and_then(|e| e.to_str())
        .map(|ext| ext.eq_ignore_ascii_case(DYLIB_EXT))
        .unwrap_or(false)
}
fn discover_module_files(dir: &Path) -> Result<Vec<PathBuf>> {
    if !dir.exists() {
        return Ok(vec![]);
    }
    let mut files = Vec::new();
    for entry in fs::read_dir(dir).map_err(|_e| {
        DynLibError::InvalidPath(format!("Reading plugin dir {}", dir.display()).into())
    })? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() && is_dynamic_lib(&path) {
            files.push(path);
        }
    }
    Ok(files)
}

fn load_module(path: &Path, mgr: Arc<TaskManager>) -> Result<(LoadedLib, tokio::task::JoinHandle<()>)> {
    unsafe {
        let lib = Library::new(path).map_err(|_| {
            DynLibError::LoadError(format!("Loading library {}", path.display()).into())
        })?;
        let fut_sym = lib.get::<RegisterFutureFn>(b"register_plugin_future").map_err(|_| {
            DynLibError::SymbolNotFound(
                format!("Missing register_plugin_future in {} (Rust ABI only)", path.display()).into(),
            )
        })?;
        let fut = fut_sym(Arc::clone(&mgr));
        let path_str = path.display().to_string();
        let handle = tokio::spawn(async move {
            fut.await;
            info!("Plugin registration future finished for {path_str}");
        });
        Ok((LoadedLib { _lib: lib, _path: path.to_path_buf() }, handle))
    }
}

/// 解析热更副本文件名，返回 (original_stem, timestamp_secs, size, full_path)
/// 约定命名： <stem>-<secs>-<size>.<ext>
fn parse_hot_filename(path: &Path) -> Option<(String, u64, u64, PathBuf)> {
    let stem_part = path.file_stem()?.to_str()?; // includes stem-secs-size combined
    // We split from the right, expecting at least 2 '-' separating secs & size
    let mut parts: Vec<&str> = stem_part.rsplitn(3, '-').collect();
    if parts.len() != 3 { return None; }
    parts.reverse();
    let stem = parts[0].to_string();
    let secs: u64 = parts[1].parse().ok()?;
    let size: u64 = parts[2].parse().ok()?;
    Some((stem, secs, size, path.to_path_buf()))
}

/// 保留每个 original stem 最近 N 个版本 (按 timestamp_secs 降序)，删除其余
fn retain_latest_versions(hot_dir: &Path, keep: usize) {
    let Ok(entries) = fs::read_dir(hot_dir) else { return; };
    use std::collections::HashMap;
    let mut groups: HashMap<String, Vec<(u64,u64,PathBuf)>> = HashMap::new();
    for e in entries.flatten() {
        let p = e.path();
        if !(p.is_file() && is_dynamic_lib(&p)) { continue; }
        if let Some((stem, secs, size, full)) = parse_hot_filename(&p) {
            groups.entry(stem).or_default().push((secs,size,full));
        }
    }
    for (stem, mut list) in groups { // list: (secs,size,path)
        // sort desc by secs then size as tiebreaker
        list.sort_by(|a,b| b.0.cmp(&a.0).then_with(|| b.1.cmp(&a.1)));
        if list.len() <= keep { continue; }
        for (_secs,_size,path) in list.into_iter().skip(keep) {
            if let Err(e) = fs::remove_file(&path) {
                warn!("Retention: remove old version {} (stem={}) failed: {}", path.display(), stem, e);
            } else {
                info!("Retention: removed old version {} (stem={})", path.display(), stem);
            }
        }
    }
}

#[derive(Default)]
pub struct ModuleLoader {
    loaded_libs: Arc<RwLock<Vec<LoadedLib>>>,
    m_times: Arc<RwLock<HashMap<PathBuf, std::time::SystemTime>>>,
    join_handles: Arc<RwLock<Vec<tokio::task::JoinHandle<()>>>>,
}
impl ModuleLoader {
    /// 等待所有已延迟的模块注册 Future 完成（错误会被吞掉，仅记录日志）
    pub async fn await_registrations(&self) {
        let mut handles = self.join_handles.write().await;
        let current: Vec<_> = handles.drain(..).collect();
        drop(handles);
        for h in current {
            if let Err(e) = h.await { error!("Deferred module registration task panicked: {e:?}"); }
        }
    }
    pub async fn load(&self, dir: &Path, task_manager: Arc<TaskManager>) {
        task_manager.clear_factory_cache().await;
        let candidates = discover_module_files(dir).unwrap_or_default();
        if candidates.is_empty() {
            warn!("No crawler found in {dir:?}");
            return;
        }

        // 准备 .hot 目录
        let hot_dir = dir.join(".hot");
        if !hot_dir.exists() { let _ = fs::create_dir_all(&hot_dir); }

        // 不再全量清空 .hot，后面按保留策略裁剪

        for lib_path in candidates {
            let changed = match fs::metadata(&lib_path).and_then(|m| m.modified()) {
                Ok(modified) => match self.m_times.read().await.get(&lib_path) {
                    Some(old) => modified > *old,
                    None => true,
                },
                Err(_) => false,
            };
            if !changed { continue; }

            // 生成唯一副本名： <stem>-<secs>-<size>.<ext>
            let meta = match fs::metadata(&lib_path) { Ok(m) => m, Err(e) => { error!("Meta error {}: {}", lib_path.display(), e); continue; } };
            let size = meta.len();
            let secs = meta.modified().ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_secs()).unwrap_or(0);
            let stem = lib_path.file_stem().and_then(|s| s.to_str()).unwrap_or("module");
            let unique_name = format!("{stem}-{secs}-{size}.{DYLIB_EXT}");
            let unique_path = hot_dir.join(unique_name);
            if let Err(e) = fs::copy(&lib_path, &unique_path) {
                error!("Copy {} failed: {}", lib_path.display(), e);
                continue;
            }

            // 加载副本
            match load_module(&unique_path, Arc::clone(&task_manager)) {
                Ok((lib, handle)) => {
                    if let Ok(m) = meta.modified() { self.m_times.write().await.insert(lib_path.clone(), m); }
                    info!("Hot loaded {} (async registration)", lib_path.display());
                    self.join_handles.write().await.push(handle);
                    self.loaded_libs.write().await.push(lib);
                }
                Err(e) => {
                    error!("Failed to hot load {}: {:#}", lib_path.display(), e);
                }
            }
        }
        // 版本保留策略：保留每个模块最近 10 个热更副本
        retain_latest_versions(&hot_dir, 10);
        // 始终等待所有延迟注册完成，确保 load 返回时所有模块已准备就绪
        self.await_registrations().await;
        info!("All deferred module registrations completed");
    }
}
