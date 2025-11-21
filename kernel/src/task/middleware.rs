#![allow(unused)]


use std::sync::Arc;
use crate::middleware::DownloadMiddleware;
use crate::model::ModuleConfig;

pub struct DownloaderMiddleware {
    pub name: String,
    pub config: ModuleConfig,
    pub work: Arc<dyn DownloadMiddleware>,
}

pub struct DataMiddleware {
    pub name: String,
    pub config: ModuleConfig,
    pub work: Arc<dyn DownloadMiddleware>,
}
