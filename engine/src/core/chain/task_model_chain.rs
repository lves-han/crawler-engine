use crate::core::events::EventConfigLoad::{
    ConfigLoadCompleted, ConfigLoadFailed, ConfigLoadPrepared, ConfigLoadRetry, ConfigLoadStarted,
};
use crate::core::events::EventParserError::{
    ParserErrorCompleted, ParserErrorFailed, ParserErrorReceived, ParserErrorRetry,
    ParserErrorStarted,
};
use crate::core::events::EventParserTaskModel::{
    ParserTaskModelCompleted, ParserTaskModelFailed, ParserTaskModelReceived, ParserTaskModelRetry,
    ParserTaskModelStarted,
};

use crate::core::events::EventRequestPublish::{
    RequestPublishCompleted, RequestPublishFailed, RequestPublishPrepared, RequestPublishRetry,
    RequestPublishSend,
};

use crate::core::events::EventTaskModel::{
    TaskModelCompleted, TaskModelFailed, TaskModelReceived, TaskModelRetry, TaskModelStarted,
};
use crate::core::events::{
    ConfigLoadEvent, EventModule, EventTask, ModuleEvent, ParserErrorEvent, ParserTaskModelEvent,
    RequestEvent, TaskEvent,
};
use crate::core::events::{DynFailureEvent, DynRetryEvent, EventBus, SystemEvent, TaskModelEvent};
use crate::core::processors::event_processor::{EventAwareTypedChain, EventProcessorTrait};
use kernel::Module;
use kernel::{Task, TaskManager};

use async_trait::async_trait;
use error::{Error, ModuleError, Result};
use kernel::model::login_info::LoginInfo;
use kernel::model::message::{ErrorTaskModel, ParserTaskModel, TaskModel};
use kernel::model::{ModuleConfig, Request};
use kernel::state::State;
use kernel::sync::SyncAble;
use kernel::sync::sync_service::SyncService;
use log::{debug, error, warn};
use message_queue::QueueManager;
use processor_chain::processors::processor::{
    ProcessorContext, ProcessorResult, ProcessorTrait, RetryPolicy,
};
use processor_chain::processors::processor_chain::ErrorStrategy;
use std::sync::Arc;
use uuid::Uuid;

pub struct TaskModelProcessor {
    task_manager: Arc<TaskManager>,
    state: Arc<State>,
    queue_manager: Arc<QueueManager>,
}
#[async_trait]
impl ProcessorTrait<TaskModel, Task> for TaskModelProcessor {
    fn name(&self) -> &'static str {
        "TaskProcessor"
    }
    async fn process(&self, input: TaskModel, context: ProcessorContext) -> ProcessorResult<Task> {
        debug!(
            "[TaskModelProcessor<TaskModel>] begin: account={} platform={} module={:?} retry_count={}",
            input.account,
            input.platform,
            input.module,
            context.retry_policy.as_ref().map(|r| r.current_retry).unwrap_or(0)
        );

        // 首先检查 Task 是否已被标记为终止（在加载 Task 之前）
        // Task ID 格式：{platform}:{account}:{run_id}
        let task_id = format!("{}:{}:{}", input.platform, input.account, input.run_id);
        match self.state.error_tracker.should_task_continue(&task_id).await {
            Ok(kernel::ErrorDecision::Continue) => {
                debug!(
                    "[TaskModelProcessor<TaskModel>] task can continue: task_id={}",
                    task_id
                );
            }
            Ok(kernel::ErrorDecision::Terminate(reason)) => {
                error!(
                    "[TaskModelProcessor<TaskModel>] task terminated (pre-check): task_id={} reason={}",
                    task_id,
                    reason
                );
                // 不要将已终止的 Task 重新入队，直接返回致命错误
                return ProcessorResult::FatalFailure(
                    ModuleError::TaskMaxError(reason.into()).into(),
                );
            }
            Err(e) => {
                warn!(
                    "[TaskModelProcessor<TaskModel>] error tracker check failed: task_id={} error={}, continue anyway",
                    task_id, e
                );
            }
            _ => {}
        }

        let task = self.task_manager.load_with_model(&input).await;

        match task {
            Ok(task) => {
                if task.is_empty() {
                    return ProcessorResult::FatalFailure(
                        ModuleError::ModuleNotFound(
                            format!(
                                "No modules found for the given TaskModel, task_model: {input:?}"
                            )
                            .into(),
                        )
                        .into(),
                    );
                }
                // 在TaskModelProcessor阶段进行加锁校验：如果目标模块均处于锁定状态，则根据消息类型将其重新入队
                let default_locker_ttl = self.state.config.read().await.crawler.module_locker_ttl;
                let mut all_locked = true;
                for m in task.modules.iter() {
                    if !self
                        .state
                        .sync_service
                        .is_module_locker(m.id().as_ref(), default_locker_ttl)
                        .await
                    {
                        all_locked = false;
                        break;
                    }
                }
                if all_locked {
                    warn!(
                        "[TaskModelProcessor<TaskModel>] all target modules locked, requeue TaskModel: account={} platform={}",
                        input.account, input.platform
                    );
                    let sender = self.queue_manager.get_task_push_channel();
                    if let Err(e) = sender.send(input.clone()).await {
                        warn!(
                            "[TaskModelProcessor<TaskModel>] requeue TaskModel failed, will retry: {}",
                            e
                        );
                    }
                    return ProcessorResult::RetryableFailure(
                        context.retry_policy.unwrap_or_default(),
                    );
                }

                ProcessorResult::Success(task)
            }
            Err(e) => {
                warn!("[TaskModelProcessor<TaskModel>] load_with_model failed, will retry: {e}");
                ProcessorResult::RetryableFailure(
                    context
                        .retry_policy
                        .unwrap_or(RetryPolicy::default().with_reason(e.to_string())),
                )
            }
        }
    }
    async fn pre_process(&self, _input: &TaskModel, _context: &ProcessorContext) -> Result<()> {
        Ok(())
    }
    async fn handle_error(
        &self,
        input: &TaskModel,
        error: Error,
        _context: &ProcessorContext,
    ) -> ProcessorResult<Task> {

        let sender = self.queue_manager.get_error_push_channel();
        let error_msg = ErrorTaskModel {
            id: Default::default(),
            account_task: input.clone(),
            error_msg: error.to_string(),
            timestamp: chrono::Utc::now().timestamp_millis() as u64,
            metadata: Default::default(),
            context: Default::default(),
            run_id: input.run_id,
            prefix_request: Uuid::nil(),
        };
        if let Err(e) = sender.send(error_msg).await {
            error!("[TaskModelProcessor<TaskModel>] failed to enqueue ErrorTaskModel: {e}");
        }
        ProcessorResult::FatalFailure(
            ModuleError::ModuleNotFound("Failed to load task, re-queued".into()).into(),
        )
    }
}
#[async_trait]
impl EventProcessorTrait<TaskModel, Task> for TaskModelProcessor {
    fn pre_status(&self, input: &TaskModel) -> SystemEvent {
        SystemEvent::TaskModel(TaskModelReceived(input.into()))
    }

    fn finish_status(&self, input: &TaskModel, output: &Task) -> SystemEvent {
        let mut task_model_event: TaskModelEvent = input.into();
        task_model_event.modules = Some(output.get_module_names());
        SystemEvent::TaskModel(TaskModelCompleted(task_model_event))
    }

    fn working_status(&self, input: &TaskModel) -> SystemEvent {
        SystemEvent::TaskModel(TaskModelStarted(input.into()))
    }

    fn error_status(&self, input: &TaskModel, error: &Error) -> SystemEvent {
        let task_model_error: DynFailureEvent<TaskModelEvent> = DynFailureEvent {
            data: input.into(),
            error: error.to_string(),
        };
        SystemEvent::TaskModel(TaskModelFailed(task_model_error))
    }

    fn retry_status(&self, input: &TaskModel, retry_policy: &RetryPolicy) -> SystemEvent {
        let task_model_retry: DynRetryEvent<TaskModelEvent> = DynRetryEvent {
            data: input.into(),
            retry_count: retry_policy.current_retry,
            reason: retry_policy.reason.clone().unwrap_or("".to_string()),
        };
        SystemEvent::TaskModel(TaskModelRetry(task_model_retry))
    }
}
#[async_trait]
impl ProcessorTrait<ParserTaskModel, Task> for TaskModelProcessor {
    fn name(&self) -> &'static str {
        "TaskModelProcessor"
    }

    async fn process(
        &self,
        input: ParserTaskModel,
        context: ProcessorContext,
    ) -> ProcessorResult<Task> {
        debug!(
            "[TaskModelProcessor<ParserTaskModel>] start: account={} platform={} crawler={:?} retry_count={}",
            input.account_task.account,
            input.account_task.platform,
            input.account_task.module,
            context
                .retry_policy
                .as_ref()
                .map(|r| r.current_retry)
                .unwrap_or(0)
        );
        
        // 首先检查 Task 是否已被标记为终止
        let task_id = format!("{}:{}:{}", input.account_task.platform, input.account_task.account, input.run_id);
        match self.state.error_tracker.should_task_continue(&task_id).await {
            Ok(kernel::ErrorDecision::Continue) => {
                debug!(
                    "[TaskModelProcessor<ParserTaskModel>] task can continue: task_id={}",
                    task_id
                );
            }
            Ok(kernel::ErrorDecision::Terminate(reason)) => {
                error!(
                    "[TaskModelProcessor<ParserTaskModel>] task terminated (pre-check): task_id={} reason={}",
                    task_id,
                    reason
                );
                return ProcessorResult::FatalFailure(
                    ModuleError::TaskMaxError(reason.into()).into(),
                );
            }
            Err(e) => {
                warn!(
                    "[TaskModelProcessor<ParserTaskModel>] error tracker check failed: task_id={} error={}, continue anyway",
                    task_id, e
                );
            }
            _ => {}
        }
        
        let task = self.task_manager.load_parser(&input).await;
        match task {
            Ok(task) => {
                ProcessorResult::Success(task)
            }
            Err(e) => {
                warn!("[TaskModelProcessor<ParserTaskModel>] load_parser failed, will retry: {e}");
                ProcessorResult::RetryableFailure(
                    context
                        .retry_policy
                        .unwrap_or(RetryPolicy::default().with_reason(e.to_string())),
                )
            }
        }
    }
    async fn pre_process(
        &self,
        _input: &ParserTaskModel,
        _context: &ProcessorContext,
    ) -> Result<()> {
        Ok(())
    }
    async fn handle_error(
        &self,
        input: &ParserTaskModel,
        error: Error,
        _context: &ProcessorContext,
    ) -> ProcessorResult<Task> {
        let sender = self.queue_manager.get_error_push_channel();
        let error_msg = ErrorTaskModel {
            id: Default::default(),
            account_task: input.account_task.clone(),
            error_msg: error.to_string(),
            timestamp: chrono::Utc::now().timestamp_millis() as u64,
            metadata: Default::default(),
            context: Default::default(),
            run_id: input.run_id,
            prefix_request: input.prefix_request,
        };
        if let Err(e) = sender.send(error_msg).await {
            error!("[TaskModelProcessor<ParserTaskModel>] failed to enqueue ErrorTaskModel: {e}");
        }
        ProcessorResult::FatalFailure(
            ModuleError::ModuleNotFound("Failed to load task, re-queued".into()).into(),
        )
    }
}
#[async_trait]
impl EventProcessorTrait<ParserTaskModel, Task> for TaskModelProcessor {
    fn pre_status(&self, input: &ParserTaskModel) -> SystemEvent {
        SystemEvent::ParserTaskModel(ParserTaskModelReceived(input.into()))
    }

    fn finish_status(&self, input: &ParserTaskModel, output: &Task) -> SystemEvent {
        let mut evt: ParserTaskModelEvent = input.into();
        evt.modules = Some(output.get_module_names());
        SystemEvent::ParserTaskModel(ParserTaskModelCompleted(evt))
    }

    fn working_status(&self, input: &ParserTaskModel) -> SystemEvent {
        SystemEvent::ParserTaskModel(ParserTaskModelStarted(input.into()))
    }

    fn error_status(&self, input: &ParserTaskModel, err: &Error) -> SystemEvent {
        let failure: DynFailureEvent<ParserTaskModelEvent> = DynFailureEvent {
            data: input.into(),
            error: err.to_string(),
        };
        SystemEvent::ParserTaskModel(ParserTaskModelFailed(failure))
    }

    fn retry_status(&self, input: &ParserTaskModel, retry_policy: &RetryPolicy) -> SystemEvent {
        let retry: DynRetryEvent<ParserTaskModelEvent> = DynRetryEvent {
            data: input.into(),
            retry_count: retry_policy.current_retry,
            reason: retry_policy.reason.clone().unwrap_or_default(),
        };
        SystemEvent::ParserTaskModel(ParserTaskModelRetry(retry))
    }
}
#[async_trait]
impl ProcessorTrait<ErrorTaskModel, Task> for TaskModelProcessor {
    fn name(&self) -> &'static str {
        "TaskModelProcessor"
    }

    async fn process(
        &self,
        input: ErrorTaskModel,
        context: ProcessorContext,
    ) -> ProcessorResult<Task> {
        debug!(
            "[TaskModelProcessor<ErrorTaskModel>] start: account={} platform={} crawler={:?} retry_count={}",
            input.account_task.account,
            input.account_task.platform,
            input.account_task.module,
            context
                .retry_policy
                .as_ref()
                .map(|r| r.current_retry)
                .unwrap_or(0)
        );
        
        // 首先检查 Task 是否已被标记为终止
        let task_id = format!("{}:{}:{}", input.account_task.platform, input.account_task.account, input.run_id);
        match self.state.error_tracker.should_task_continue(&task_id).await {
            Ok(kernel::ErrorDecision::Continue) => {
                debug!(
                    "[TaskModelProcessor<ErrorTaskModel>] task can continue: task_id={}",
                    task_id
                );
            }
            Ok(kernel::ErrorDecision::Terminate(reason)) => {
                error!(
                    "[TaskModelProcessor<ErrorTaskModel>] task terminated (pre-check): task_id={} reason={}",
                    task_id,
                    reason
                );
                return ProcessorResult::FatalFailure(
                    ModuleError::TaskMaxError(reason.into()).into(),
                );
            }
            Err(e) => {
                warn!(
                    "[TaskModelProcessor<ErrorTaskModel>] error tracker check failed: task_id={} error={}, continue anyway",
                    task_id, e
                );
            }
            _ => {}
        }
        
        let task = self.task_manager.load_error(&input).await;
        match task {
            Ok(task) => {
                ProcessorResult::Success(task)
            }
            Err(e) => {
                warn!("[TaskModelProcessor<ErrorTaskModel>] load_error failed, will retry: {e}");
                ProcessorResult::RetryableFailure(
                    context
                        .retry_policy
                        .unwrap_or(RetryPolicy::default().with_reason(e.to_string())),
                )
            }
        }
    }
    async fn pre_process(&self, input: &ErrorTaskModel, _context: &ProcessorContext) -> Result<()> {
        // 从 ErrorTaskModel 生成 task_id 和 module_id
        // task_id 格式：{platform}:{account}:{run_id}
        let task_id = format!(
            "{}:{}:{}",
            input.account_task.platform, input.account_task.account, input.run_id
        );

        // 记录到 error tracker
        // 对于 ErrorTaskModel，我们将其视为一个通用错误并记录到 Task 级别
        // 这样可以追踪重试次数和错误模式
        debug!(
            "[TaskModelProcessor<ErrorTaskModel>] recording error to tracker: task_id={} error={}",
            task_id, input.error_msg
        );

        // 如果有具体的 module 信息，记录到 module 级别
        if let Some(modules) = &input.account_task.module {
            for module_name in modules {
                // Module ID 格式必须与 Module.id() 保持一致：{account}-{platform}-{module}
                let module_id = format!(
                    "{}-{}-{}",
                    input.account_task.account,
                    input.account_task.platform,
                    module_name
                );

                // 创建一个简单的 Error 用于分类
                let error = ModuleError::ModuleNotFound(input.error_msg.clone().into()).into();

                // 记录解析错误（因为 ErrorTaskModel 通常是解析或处理阶段的错误）
                if input.prefix_request != Uuid::nil() {
                    let request_id = input.prefix_request.to_string();
                    debug!(
                        "[TaskModelProcessor<ErrorTaskModel>] recording parse error: task_id={} module_id={} request_id={}",
                        task_id, module_id, request_id
                    );
                    let _ = self
                        .state
                        .error_tracker
                        .record_parse_error(&task_id, &module_id, &request_id, &error)
                        .await;
                } else {
                    warn!(
                        "[TaskModelProcessor<ErrorTaskModel>] skipping error record: no prefix_request, task_id={} module_id={}",
                        task_id, module_id
                    );
                }
            }
        }

        Ok(())
    }
    // async fn handle_error(
    //     &self,
    //     input: &ParserErrorMessage,
    //     _error: Error,
    //     _context: &ProcessorContext,
    // ) -> ProcessorResult<Task> {
    //     // 由于使用的是动态加载dylib,task_manager.load_error可能未能加载到正确的库
    //     let sender = self.queue_manager.get_error_push_channel();
    //     sender.send(input.to_owned()).await.unwrap();
    //     ProcessorResult::FatalFailure(
    //         ModuleError::ModuleNotFound("Failed to load task, re-queued".into()).into(),
    //     )
    // }
}
#[async_trait]
impl EventProcessorTrait<ErrorTaskModel, Task> for TaskModelProcessor {
    fn pre_status(&self, input: &ErrorTaskModel) -> SystemEvent {
        SystemEvent::ParserError(ParserErrorReceived(input.into()))
    }

    fn finish_status(&self, input: &ErrorTaskModel, output: &Task) -> SystemEvent {
        let mut evt: ParserErrorEvent = input.into();
        evt.modules = Some(output.get_module_names());
        SystemEvent::ParserError(ParserErrorCompleted(evt))
    }

    fn working_status(&self, input: &ErrorTaskModel) -> SystemEvent {
        SystemEvent::ParserError(ParserErrorStarted(input.into()))
    }

    fn error_status(&self, input: &ErrorTaskModel, err: &Error) -> SystemEvent {
        let failure: DynFailureEvent<ParserErrorEvent> = DynFailureEvent {
            data: input.into(),
            error: err.to_string(),
        };
        SystemEvent::ParserError(ParserErrorFailed(failure))
    }

    fn retry_status(&self, input: &ErrorTaskModel, retry_policy: &RetryPolicy) -> SystemEvent {
        let retry: DynRetryEvent<ParserErrorEvent> = DynRetryEvent {
            data: input.into(),
            retry_count: retry_policy.current_retry,
            reason: retry_policy.reason.clone().unwrap_or_default(),
        };
        SystemEvent::ParserError(ParserErrorRetry(retry))
    }
}

pub struct TaskModuleProcessor {
    state: Arc<State>,
    queue_manager: Arc<QueueManager>,
}
#[async_trait]
impl ProcessorTrait<Task, Vec<Module>> for TaskModuleProcessor {
    fn name(&self) -> &'static str {
        "TaskModuleProcessor"
    }

    async fn process(
        &self,
        input: Task,
        context: ProcessorContext,
    ) -> ProcessorResult<Vec<Module>> {
        debug!(
            "[TaskModuleProcessor] start: task_id={} module_count={}",
            input.id(),
            input.modules.len()
        );
        let mut modules: Vec<Module> = Vec::new();
        let default_locker_ttl = self.state.config.read().await.crawler.module_locker_ttl;
        for mut module in input.modules {
            // 使用 ErrorTracker 检查 Module 是否应该继续
            match self
                .state
                .error_tracker
                .should_module_continue(&module.id())
                .await
            {
                Ok(kernel::ErrorDecision::Continue) => {
                    debug!(
                        "[TaskModuleProcessor] module can continue: module_id={}",
                        module.id()
                    );
                }
                Ok(kernel::ErrorDecision::Terminate(reason)) => {
                    // Module 已达到错误阈值，跳过该 Module，继续处理其他 Module
                    error!(
                        "[TaskModuleProcessor] skip terminated module: module_id={} reason={}",
                        module.id(),
                        reason
                    );
                    // 不返回错误，只是跳过这个 Module
                    continue;
                }
                Err(e) => {
                    warn!(
                        "[TaskModuleProcessor] error tracker check failed for module: module_id={} error={}, continue anyway",
                        module.id(),
                        e
                    );
                }
                _ => {}
            }

            module.locker_ttl = module
                .config
                .get_config_value("module_locker_ttl")
                .and_then(|v| v.as_u64())
                .unwrap_or(default_locker_ttl);
            modules.push(module);
        }

        let mut filtered_modules: Vec<Module> = Vec::new();
        for x in modules.into_iter() {
            filtered_modules.push(x);
           
        }
        let modules = filtered_modules;
        if !modules.is_empty() {
            context.metadata.write().await.insert(
                "task_meta".to_string(),
                serde_json::to_value(input.metadata).unwrap(),
            );
            context.metadata.write().await.insert(
                "login_info".to_string(),
                serde_json::to_value(input.login_info).unwrap(),
            );
        }
        ProcessorResult::Success(modules)
    }
}

#[async_trait]
impl EventProcessorTrait<Task, Vec<Module>> for TaskModuleProcessor {
    fn pre_status(&self, input: &Task) -> SystemEvent {
        SystemEvent::Task(EventTask::TaskReceived(input.into()))
    }

    fn finish_status(&self, input: &Task, _output: &Vec<Module>) -> SystemEvent {
        SystemEvent::Task(EventTask::TaskCompleted(input.into()))
    }

    fn working_status(&self, input: &Task) -> SystemEvent {
        SystemEvent::Task(EventTask::TaskStarted(input.into()))
    }

    fn error_status(&self, input: &Task, err: &Error) -> SystemEvent {
        let failure: DynFailureEvent<TaskEvent> = DynFailureEvent {
            data: input.into(),
            error: err.to_string(),
        };
        SystemEvent::Task(EventTask::TaskFailed(failure))
    }

    fn retry_status(&self, input: &Task, retry_policy: &RetryPolicy) -> SystemEvent {
        let retry: DynRetryEvent<TaskEvent> = DynRetryEvent {
            data: input.into(),
            retry_count: retry_policy.current_retry,
            reason: retry_policy.reason.clone().unwrap_or_default(),
        };
        SystemEvent::Task(EventTask::TaskRetry(retry))
    }
}

pub struct TaskProcessor {
    sync_service: Arc<SyncService>,
    state: Arc<State>,
}
#[async_trait]
impl ProcessorTrait<Module, Vec<Request>> for TaskProcessor {
    fn name(&self) -> &'static str {
        "TaskProcessor"
    }

    async fn process(
        &self,
        input: Module,
        context: ProcessorContext,
    ) -> ProcessorResult<Vec<Request>> {
        debug!(
            "[TaskProcessor] start generate: module_id={} module_name={}",
            input.id(),
            input.module.name()
        );
        
        // 在生成 Request 之前检查 Module 是否已被终止
        match self.state.error_tracker.should_module_continue(&input.id()).await {
            Ok(kernel::ErrorDecision::Continue) => {
                debug!(
                    "[TaskProcessor] module can continue generating requests: module_id={}",
                    input.id()
                );
            }
            Ok(kernel::ErrorDecision::Terminate(reason)) => {
                error!(
                    "[TaskProcessor] module terminated, skip request generation: module_id={} reason={}",
                    input.id(),
                    reason
                );
                // 返回空的 Request 列表，后处理会释放锁
                return ProcessorResult::Success(Vec::new());
            }
            Err(e) => {
                warn!(
                    "[TaskProcessor] error tracker check failed: module_id={} error={}, continue anyway",
                    input.id(), e
                );
            }
            _ => {}
        }
        
        // Task.metadata
        let meta = match context.metadata.read().await.get("task_meta") {
            Some(m) => m.as_object().cloned().unwrap_or_default(),
            None => {
                warn!("[TaskProcessor] task_meta missing in context, will retry");
                return ProcessorResult::RetryableFailure(context.retry_policy.unwrap_or_default());
            }
        };
        let login_info = if input.module.should_login() {
            match context.metadata.read().await.get("login_info") {
                Some(m) => match serde_json::from_value::<LoginInfo>(m.clone()) {
                    Ok(info) => Some(info),
                    Err(e) => {
                        warn!("[TaskProcessor] invalid login_info, will retry: {e}");
                        return ProcessorResult::RetryableFailure(
                            context
                                .retry_policy
                                .unwrap_or_default()
                                .with_reason("not found login info".to_string()),
                        );
                    }
                },
                None => {
                    warn!("[TaskProcessor] missing login_info, will retry");
                    return ProcessorResult::RetryableFailure(
                        context
                            .retry_policy
                            .unwrap_or_default()
                            .with_reason("not found login info".to_string()),
                    );
                }
            }
        } else {
            None
        };
        // TaskModel
        // ParserTaskModel.meta=>Task.metadata=>Context.meta=>Module.generate=>ModuleTrait.generate
        let requests: Vec<Request> = match input.generate(meta, login_info).await {
            Ok(reqs) => {
                debug!(
                    "[TaskProcessor] generate completed: request_count={}",
                    reqs.len()
                );
                reqs
            }
            Err(e) => {
                warn!("[TaskProcessor] generate error, will retry: {e}");
                return ProcessorResult::RetryableFailure(
                    context
                        .retry_policy
                        .unwrap_or(RetryPolicy::default().with_reason(e.to_string())),
                );
            }
        };
        ProcessorResult::Success(requests)
    }
    async fn post_process(
        &self,
        input: &Module,
        output: &Vec<Request>,
        _context: &ProcessorContext,
    ) -> Result<()> {
        if output.is_empty() {
            self.sync_service.release_module_locker(&input.id()).await;
        }
        Ok(())
    }
}
#[async_trait]
impl EventProcessorTrait<Module, Vec<Request>> for TaskProcessor {
    fn pre_status(&self, input: &Module) -> SystemEvent {
        SystemEvent::Module(EventModule::ModuleGenerate(input.into()))
    }

    fn finish_status(&self, input: &Module, _output: &Vec<Request>) -> SystemEvent {
        SystemEvent::Module(EventModule::ModuleCompleted(input.into()))
    }

    fn working_status(&self, input: &Module) -> SystemEvent {
        SystemEvent::Module(EventModule::ModuleStarted(input.into()))
    }

    fn error_status(&self, input: &Module, err: &Error) -> SystemEvent {
        let failure: DynFailureEvent<ModuleEvent> = DynFailureEvent {
            data: input.into(),
            error: err.to_string(),
        };
        SystemEvent::Module(EventModule::ModuleFailed(failure))
    }

    fn retry_status(&self, input: &Module, retry_policy: &RetryPolicy) -> SystemEvent {
        let retry: DynRetryEvent<ModuleEvent> = DynRetryEvent {
            data: input.into(),
            retry_count: retry_policy.current_retry,
            reason: retry_policy.reason.clone().unwrap_or_default(),
        };
        SystemEvent::Module(EventModule::ModuleRetry(retry))
    }
}
pub struct RequestPublish {
    queue_manager: Arc<QueueManager>,
    sync_service: Arc<SyncService>,
}

#[async_trait]
impl ProcessorTrait<Request, ()> for RequestPublish {
    fn name(&self) -> &'static str {
        "RequestPublish"
    }

    async fn process(&self, input: Request, context: ProcessorContext) -> ProcessorResult<()> {
        debug!(
            "[RequestPublish] publish request: request_id={} module_id={}",
            input.id,
            input.module_id()
        );
        if let Err(e) = self
            .queue_manager
            .get_request_push_channel()
            .send(input)
            .await
        {
            error!("Failed to send request to queue: {e}");
            warn!("[RequestPublish] will retry due to queue send error");
            return ProcessorResult::RetryableFailure(
                context
                    .retry_policy
                    .unwrap_or(RetryPolicy::default().with_reason(e.to_string())),
            );
        }
        ProcessorResult::Success(())
    }
    async fn handle_error(
        &self,
        input: &Request,
        error: Error,
        _context: &ProcessorContext,
    ) -> ProcessorResult<()> {
        // If we can't publish the request after retries, release the module lock
        // to avoid locking out future runs of this module.
        self.sync_service
            .release_module_locker(&input.module_id())
            .await;
        ProcessorResult::FatalFailure(error)
    }
}
#[async_trait]
impl EventProcessorTrait<Request, ()> for RequestPublish {
    fn pre_status(&self, input: &Request) -> SystemEvent {
        SystemEvent::RequestPublish(RequestPublishPrepared(input.into()))
    }

    fn finish_status(&self, input: &Request, _output: &()) -> SystemEvent {
        SystemEvent::RequestPublish(RequestPublishCompleted(input.into()))
    }

    fn working_status(&self, input: &Request) -> SystemEvent {
        SystemEvent::RequestPublish(RequestPublishSend(input.into()))
    }

    fn error_status(&self, input: &Request, err: &Error) -> SystemEvent {
        let failure: DynFailureEvent<RequestEvent> = DynFailureEvent {
            data: input.into(),
            error: err.to_string(),
        };
        SystemEvent::RequestPublish(RequestPublishFailed(failure))
    }

    fn retry_status(&self, input: &Request, retry_policy: &RetryPolicy) -> SystemEvent {
        let retry: DynRetryEvent<RequestEvent> = DynRetryEvent {
            data: input.into(),
            retry_count: retry_policy.current_retry,
            reason: retry_policy.reason.clone().unwrap_or_default(),
        };
        SystemEvent::RequestPublish(RequestPublishRetry(retry))
    }
}
pub struct ConfigProcessor {
    pub sync_service: Arc<SyncService>,
}
#[async_trait]
impl ProcessorTrait<Request, (Request, Option<ModuleConfig>)> for ConfigProcessor {
    fn name(&self) -> &'static str {
        "ConfigProcessor"
    }

    async fn process(
        &self,
        input: Request,
        context: ProcessorContext,
    ) -> ProcessorResult<(Request, Option<ModuleConfig>)> {
        // ModuleConfig在factory::load_with_model里进行了上传，使用module::id()作为唯一标识
        // 这里进行下载
        match ModuleConfig::sync(&input.module_id(), self.sync_service.synchronizer.clone()).await {
            Ok(Some(config)) => ProcessorResult::Success((input, Some(config))),
            Ok(None) => ProcessorResult::Success((input, None)),
            Err(e) => {
                error!(
                    "Failed to fetch config for module {}: {}",
                    input.task_id(),
                    e
                );
                ProcessorResult::RetryableFailure(
                    context
                        .retry_policy
                        .unwrap_or(RetryPolicy::default().with_reason(e.to_string())),
                )
            }
        }
    }
    async fn pre_process(&self, _input: &Request, _context: &ProcessorContext) -> Result<()> {
        self.sync_service.lock_module(&_input.module_id()).await;
        Ok(())
    }
    async fn handle_error(
        &self,
        _input: &Request,
        _error: Error,
        _context: &ProcessorContext,
    ) -> ProcessorResult<(Request, Option<ModuleConfig>)> {
        ProcessorResult::Success((_input.clone(), None))
    }
}
#[async_trait]
impl EventProcessorTrait<Request, (Request, Option<ModuleConfig>)> for ConfigProcessor {
    fn pre_status(&self, input: &Request) -> SystemEvent {
        let ev: ConfigLoadEvent = (&(input.clone(), None)).into();
        SystemEvent::ConfigLoad(ConfigLoadPrepared(ev))
    }

    fn finish_status(
        &self,
        _input: &Request,
        out: &(Request, Option<ModuleConfig>),
    ) -> SystemEvent {
        let ev: ConfigLoadEvent = out.into();
        SystemEvent::ConfigLoad(ConfigLoadCompleted(ev))
    }

    fn working_status(&self, input: &Request) -> SystemEvent {
        let ev: ConfigLoadEvent = (&(input.clone(), None)).into();
        SystemEvent::ConfigLoad(ConfigLoadStarted(ev))
    }

    fn error_status(&self, input: &Request, err: &Error) -> SystemEvent {
        let ev: ConfigLoadEvent = (&(input.clone(), None)).into();
        let failure = DynFailureEvent {
            data: ev,
            error: err.to_string(),
        };
        SystemEvent::ConfigLoad(ConfigLoadFailed(failure))
    }

    fn retry_status(&self, input: &Request, retry_policy: &RetryPolicy) -> SystemEvent {
        let ev: ConfigLoadEvent = (&(input.clone(), None)).into();
        let retry = DynRetryEvent {
            data: ev,
            retry_count: retry_policy.current_retry,
            reason: retry_policy.reason.clone().unwrap_or_default(),
        };
        SystemEvent::ConfigLoad(ConfigLoadRetry(retry))
    }
}

/// task_model -> task -> request -> () (publish request to queue)
pub async fn create_task_model_chain(
    task_manager: Arc<TaskManager>,
    queue_manager: Arc<QueueManager>,
    event_bus: Arc<EventBus>,
    state: Arc<State>,
) -> EventAwareTypedChain<TaskModel, Vec<()>> {
    let task_model_processor = TaskModelProcessor {
        task_manager: task_manager.clone(),
        state: state.clone(),
        queue_manager: queue_manager.clone(),
    };
    let request_publish = RequestPublish {
        queue_manager: queue_manager.clone(),
        sync_service: state.sync_service.clone(),
    };
    let task_module_processor = TaskModuleProcessor {
        state: state.clone(),
        queue_manager: queue_manager.clone(),
    };
    let task_processor = TaskProcessor {
        sync_service: state.sync_service.clone(),
        state: state.clone(),
    };
    EventAwareTypedChain::<TaskModel, TaskModel>::new(event_bus)
        .then::<Task, _>(task_model_processor)
        .then::<Vec<Module>, _>(task_module_processor)
        .then_map_vec_parallel_with_strategy::<Vec<Request>, _>(
            task_processor,
            16,
            ErrorStrategy::Skip,
        )
        .flatten_vec()
        .then_map_vec_parallel_with_strategy::<(), _>(request_publish, 16, ErrorStrategy::Skip)
}
pub async fn create_parser_task_chain(
    task_manager: Arc<TaskManager>,
    queue_manager: Arc<QueueManager>,
    event_bus: Arc<EventBus>,
    state: Arc<State>,
) -> EventAwareTypedChain<ParserTaskModel, Vec<()>> {
    let task_model_processor = TaskModelProcessor {
        task_manager: task_manager.clone(),
        state: state.clone(),
        queue_manager: queue_manager.clone(),
    };
    let request_publish = RequestPublish {
        queue_manager: queue_manager.clone(),
        sync_service: state.sync_service.clone(),
    };
    let task_module_processor = TaskModuleProcessor {
        state: state.clone(),
        queue_manager: queue_manager.clone(),
    };
    let task_processor = TaskProcessor {
        sync_service: state.sync_service.clone(),
        state: state.clone(),
    };
    EventAwareTypedChain::<ParserTaskModel, ParserTaskModel>::new(event_bus)
        .then::<Task, _>(task_model_processor)
        .then::<Vec<Module>, _>(task_module_processor)
        .then_map_vec_parallel_with_strategy::<Vec<Request>, _>(
            task_processor,
            16,
            ErrorStrategy::Skip,
        )
        .flatten_vec()
        .then_map_vec_parallel_with_strategy::<(), _>(request_publish, 16, ErrorStrategy::Skip)
}
pub async fn create_error_task_chain(
    task_manager: Arc<TaskManager>,
    queue_manager: Arc<QueueManager>,
    event_bus: Arc<EventBus>,
    state: Arc<State>,
) -> EventAwareTypedChain<ErrorTaskModel, Vec<()>> {
    let task_model_processor = TaskModelProcessor {
        task_manager: task_manager.clone(),
        state: state.clone(),
        queue_manager: queue_manager.clone(),
    };

    let request_publish = RequestPublish {
        queue_manager: queue_manager.clone(),
        sync_service: state.sync_service.clone(),
    };
    let task_module_processor = TaskModuleProcessor {
        state: state.clone(),
        queue_manager: queue_manager.clone(),
    };
    let task_processor = TaskProcessor {
        sync_service: state.sync_service.clone(),
        state: state.clone(),
    };
    EventAwareTypedChain::<ErrorTaskModel, ErrorTaskModel>::new(event_bus)
        .then::<Task, _>(task_model_processor)
        .then::<Vec<Module>, _>(task_module_processor)
        .then_map_vec_parallel_with_strategy::<Vec<Request>, _>(
            task_processor,
            16,
            ErrorStrategy::Skip,
        )
        .flatten_vec()
        .then_map_vec_parallel_with_strategy::<(), _>(request_publish, 16, ErrorStrategy::Skip)
}
