#![allow(unused)]
use super::{
    assembler::ModuleAssembler, factory::TaskFactory, repository::TaskRepository, task::Task,
};
use std::sync::Arc;
use tokio::sync::RwLock;
use crate::model::{Request, Response};
use crate::model::message::{ErrorTaskModel, ParserTaskModel, TaskModel};
use crate::state::State;
use crate::sync::sync_service::SyncService;
use error::Result;
use crate::ModuleTrait;

pub struct TaskManager {
    factory: TaskFactory,
    pub sync_service: Arc<SyncService>,
    module_assembler: Arc<RwLock<ModuleAssembler>>,
}

impl TaskManager {
    /// 创建新的任务管理器
    pub fn new(state: Arc<State>) -> Self {
        let repository = TaskRepository::new((*state.db).clone());

        let module_assembler = Arc::new(RwLock::new(ModuleAssembler::new()));
        let factory = TaskFactory::new(
            repository,
            Arc::clone(&state.sync_service),
            Arc::clone(&state.cookie_sync_service),
            Arc::clone(&module_assembler),
            Arc::clone(&state),
        );

        Self {
            factory,
            sync_service: Arc::clone(&state.sync_service),
            module_assembler,
        }
    }

    /// 记录请求错误
    pub async fn record_request_error(&self, request: &Request) {
        self.sync_service.record_request_error(request).await;
    }
    pub async fn record_response_error(&self, response: &Response) {
        self.sync_service.record_response_error(response).await;
    }
    /// 获取错误次数
    pub async fn error_times(&self, id: &str) -> usize {
        self.sync_service.get_error_times(id).await
    }

    /// 添加工作模块
    pub async fn add_module(&self, work: Arc<dyn ModuleTrait>) {
        let mut assembler = self.module_assembler.write().await;
        assembler.register_module(work);
    }
    pub async fn add_modules(&self, works: Vec<Arc<dyn ModuleTrait>>) {
        let mut assembler = self.module_assembler.write().await;
        for work in works {
            assembler.register_module(work);
        }
    }
    pub async fn exists_module(&self, name: &str) -> bool {
        let assembler = self.module_assembler.read().await;
        assembler.get_module(name).is_some()
    }
    pub async fn remove_work(&self, name: &str) {
        let mut assembler = self.module_assembler.write().await;
        assembler.remove_module(name);
    }
    pub async fn remove_by_origin(&self, origin: &std::path::Path) {
        let mut assembler = self.module_assembler.write().await;
        assembler.remove_by_origin(origin);
    }
    pub async fn module_names(&self) -> Vec<String> {
        let assembler = self.module_assembler.read().await;
        assembler.module_names()
    }
    pub async fn set_origin(&self, names: &[String], origin: &std::path::Path) {
        let mut assembler = self.module_assembler.write().await;
        assembler.set_origin(names, origin);
    }
    /// 根据任务模型加载任务，并同步初始状态
    pub async fn load_with_model(&self, task_model: &TaskModel) -> Result<Task> {
        self.factory.load_with_model(task_model).await
    }

    /// 根据解析任务模型加载任务，并加载历史状态或初始状态
    pub async fn load_parser(&self, parser_model: &ParserTaskModel) -> Result<Task> {
        self.factory.load_parser_model(parser_model).await
    }

    /// 根据解析错误模型加载任务，并增加错误次数
    pub async fn load_error(&self, error_model: &ErrorTaskModel) -> Result<Task> {
        self.factory.load_error_model(error_model).await
    }
    pub async fn load_with_response(&self, response: &Response) -> Result<Task> {
        self.factory.load_with_response(response).await
    }
    
    pub async fn clear_factory_cache(&self) {
        self.factory.clear_cache().await;
    }
    
}
