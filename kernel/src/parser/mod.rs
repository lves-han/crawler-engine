use crate::model::data::{Data, StoreTrait};

use crate::model::message::{ErrorTaskModel, ParserTaskModel};
#[derive(Debug)]
#[derive(Default)]
pub struct ParserData {
    pub data: Vec<Data>,
    pub parser_task: Option<ParserTaskModel>,
    pub error_task: Option<ErrorTaskModel>,
    pub stop: Option<bool>,
}
impl ParserData {
    pub fn with_data(mut self, data: Vec<impl StoreTrait>) -> Self {
        self.data = data.into_iter().map(|d| d.build()).collect();
        self
    }
    pub fn with_task(mut self, task: ParserTaskModel) -> Self {
        self.parser_task = Some(task);
        self
    }
    pub fn with_error(mut self, error: ErrorTaskModel) -> Self {
        self.error_task = Some(error);
        self
    }

    /// wss 连接关闭标志，用于通知上层模块关闭连接
    /// 模块级别的停止标志，true=>后续的请求将不再被处理，
    pub fn with_stop(mut self, stop: bool) -> Self {
        self.stop = Some(stop);
        self
    }
}

// #[async_trait]
// pub trait ParserTrait {
//     async fn parser(
//         &self,
//         response: Response,
//         config: Option<ModuleConfig>,
//         state: Option<Arc<crate::state_store::StateHandle>>,
//     ) -> Result<ParserData>;
// }
