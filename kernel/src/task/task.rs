use crate::model::entity::{AccountModel, PlatformModel};
use crate::model::{Request, login_info::LoginInfo};
use crate::task::module::Module;
use error::Result;
use uuid::Uuid;

/// 增强的任务结构，包含更完整的配置信息
#[derive(Clone)]
pub struct Task {
    pub account: AccountModel,
    pub platform: PlatformModel,
    pub login_info: Option<LoginInfo>,
    pub modules: Vec<Module>,
    pub metadata: serde_json::Map<String, serde_json::Value>,
    pub run_id:Uuid,
    pub prefix_request:Uuid
}


impl Task {
    /// 获取任务的唯一标识符
    pub fn id(&self) -> String {
        format!("{}-{}", self.account.name, self.platform.name)
    }

    /// 获取工作模块的名称列表
    pub fn get_module_names(&self) -> Vec<String> {
        self.modules.iter().map(|module| module.id()).collect()
    }

    /// 并发执行所有模块的请求生成
    pub async fn build_requests(&self) -> Result<Vec<Request>> {
        use futures::future::join_all;
        // 1) 并发创建每个模块的请求流
        let futures = self
            .modules
            .iter()
            .map(|module| module.generate(self.metadata.clone(), self.login_info.clone()));

        let streams = join_all(futures).await; // Vec<Pin<Box<dyn Stream<Item=Request> + Send + '_>>>

        // 2) 合并所有请求流
        let mut all_requests = Vec::new();
        for stream in streams {
            match stream {
                Ok(mut reqs) => all_requests.append(&mut reqs),
                Err(e) => return Err(e),
            }
        }
        Ok(all_requests)
    }
    pub fn is_empty(&self)->bool{
        self.modules.is_empty()
    }
}
