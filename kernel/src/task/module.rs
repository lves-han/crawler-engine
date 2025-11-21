use crate::model::entity::{AccountModel, PlatformModel};
use crate::model::login_info::LoginInfo;
use crate::model::{Cookies, Headers, ModuleConfig, Request};
use crate::sync::SyncTrait;
use crate::task::module_processor_with_chain::ModuleProcessorWithChain;
use crate::{ModuleTrait, ParserData, Response};
use error::{RequestError, Result};
use log::warn;
use serde::ser::SerializeStruct;
use serde::{Serialize, Serializer};
use serde_json::Map;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Clone)]
pub struct Module {
    pub config: ModuleConfig,
    pub account: AccountModel,
    pub platform: PlatformModel,
    pub error_times: usize,
    pub finished: bool,
    pub data_middleware: Vec<String>,
    pub download_middleware: Vec<String>,
    pub module: Arc<dyn ModuleTrait>,
    pub locker: bool,
    pub locker_ttl: u64,
    pub processor: ModuleProcessorWithChain,
    pub run_id: Uuid,
    pub prefix_request: Uuid,
    // Optional execution context to drive precise step selection (e.g., retry current step)
    pub pending_ctx: Option<crate::model::ExecutionMark>,
}
impl Module {
    pub async fn generate(
        &self,
        task_meta: Map<String, serde_json::Value>,
        login_info: Option<LoginInfo>,
    ) -> Result<Vec<Request>> {
        if self.module.should_login() && login_info.is_none() {
            return Err(RequestError::NotLogin("module need login".into()).into());
        }
        let request_list = self
            .processor
            .execute_request(
                self.config.clone(),
                task_meta.clone(),
                login_info.clone(),
                self.pending_ctx.clone(),
                Some(self.prefix_request),
            )
            .await?;

        // let request_list = self
        //     .module
        //     .generate(
        //         self.config.clone(),
        //         task_meta.clone(),
        //         login_info.clone(),
        //         self.state_handle.clone(),
        //     )
        //     .await?;

        // Capture needed values for the mapping closure.
        let module_name = self.module.name().clone();
        let platform_name = self.platform.name.clone();
        let download_middleware = self.download_middleware.clone();
        let data_middleware = self.data_middleware.clone();
        let account_name = self.account.name.clone();
        let finished = self.finished;
        let limit_id = self
            .config
            .get_config_value("limit_id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let headers = self.module.headers().await;
        let cookies = self.module.cookies().await;
        let requests = request_list
            .into_iter()
            .map(|mut request| {
                if request.id.is_nil() {
                    request.id = Uuid::now_v7();
                }
                request.module = module_name.clone();
                request.platform = platform_name.clone();
                request.download_middleware = download_middleware.clone();
                request.data_middleware = data_middleware.clone();
                request.account = account_name.clone();
                request.task_finished = finished;
                // inherit run_id from Module/Task so ModuleProcessor and downstream can isolate state per run
                request.run_id = self.run_id;
                // Set chain prefix from Task/Module level; used to recover previous request on failures
                request.prefix_request = self.prefix_request;

                if request.headers.is_empty() && !headers.is_empty() {
                    request = request.with_headers(headers.clone());
                }
                if !cookies.is_empty() {
                    request = request.with_cookies(cookies.clone());
                }

                // 添加来自于登录信息的cookies和headers
                // 自动添加UA
                // 如果有登录信息则自动添加UA，没有登录信息需要手动在trait中添加UA
                if let Some(ref info) = login_info {
                    let cookies = Cookies::from(info);
                    let headers = Headers::from(info);
                    request.headers.merge(&headers);
                    request.cookies.merge(&cookies);
                    request = request.with_login_info(info);
                }
                request.limit_id = limit_id.clone().unwrap_or(request.module_id());
                // request.meta = request.meta.add_module_config(&self.config);
                request = request
                    .with_module_config(&self.config)
                    .with_task_config(task_meta.clone());
                // add downloader from config
                if let Some(downloader) = self.config.get_config::<String>("downloader") {
                    request.downloader = downloader;
                }
                else {
                    request.downloader = "request_downloader".to_string();
                }
                request
            })
            .collect::<Vec<_>>();
        Ok(requests)
    }

    pub async fn add_step(&self) {
        // Run module-level pre_process once before registering steps
        if let Err(e) = self
            .module
            .pre_process(Some(self.config.clone()))
            .await
        {
            // Keep non-breaking: log and continue to allow default/no-op usage
            warn!(
                "module pre_process failed for {}: {}",
                self.module.name(),
                e
            );
        }
        let node = self.module.add_step().await;
        for n in node {
            self.processor.add_step_node(n).await;
        }
    }
    pub async fn parser(
        &self,
        response: Response,
        config: Option<ModuleConfig>,
    ) -> Result<ParserData> {
        // Capture current step to determine if this is the last step
        let current_step = response.context.step_idx.unwrap_or(0) as usize;
        // Clone config for potential post_process
        let cfg_for_post = config.clone();

        let mut data = self.processor.execute_parser(response, config).await?;
        // Enrich returned Data with module/account/platform identifiers
        for d in data.data.iter_mut() {
            d.module = self.module.name();
            d.account = self.account.name.clone();
            d.platform = self.platform.name.clone();
        }


        // If we've reached the last step and there's no further task to advance,
        // trigger the module-level post_process hook.
        let total_steps = self.processor.get_execution_status().await?.total_steps;
        let is_last_step = current_step + 1 >= total_steps && total_steps > 0;
        let no_next_task = data.parser_task.is_none();
        if is_last_step && no_next_task {
            self.module.post_process(cfg_for_post).await?;
            
        }
        Ok(data)
    }

    pub fn id(&self) -> String {
        format!(
            "{}-{}-{}",
            self.account.name,
            self.platform.name,
            self.module.name()
        )
    }
    pub async fn sync_status(&self, sync: Arc<dyn SyncTrait>) -> Result<()> {
        sync.send(
            self.id().as_str(),
            "error_times",
            &serde_json::Value::from(self.error_times),
        )
            .await?;
        sync.send(
            self.id().as_str(),
            "finished",
            &serde_json::Value::from(self.finished),
        )
            .await?;
        Ok(())
    }
    pub async fn load_status(&mut self, sync: Arc<dyn SyncTrait>) {
        if let Ok(error_times) = sync.sync(self.id().as_str(), "error_times").await
            && let Some(error_times) = error_times
        {
            self.error_times = error_times.as_i64().unwrap_or(0) as usize;
        } else {
            self.error_times = 0;
        }
        if let Ok(finished) = sync.sync(self.id().as_str(), "finished").await
            && let Some(finished) = finished
        {
            self.finished = finished.as_bool().unwrap_or(false);
        } else {
            self.finished = false;
        }
    }
}

impl Serialize for Module {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("Module", 8)?;
        state.serialize_field("config", &self.config)?;
        state.serialize_field("account", &self.account)?;
        state.serialize_field("platform", &self.platform)?;
        state.serialize_field("error_times", &self.error_times)?;
        state.serialize_field("data_middleware", &self.data_middleware)?;
        state.serialize_field("download_middleware", &self.download_middleware)?;
        state.serialize_field("module", &self.module.name())?;
        state.end()
    }
}
