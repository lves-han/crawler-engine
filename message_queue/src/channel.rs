
use kernel::model::message::{ParserTaskModel, ErrorTaskModel};
use utils::logger::LogModel;
use kernel::model::message::TaskModel;
use kernel::model::{Request, Response};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::mpsc::{Receiver, Sender, channel};

pub struct Channel {
    // 多生产者 - 可以克隆发送者
    pub task_sender: Sender<TaskModel>,
    pub request_sender: Sender<Request>,
    pub download_request_sender: Sender<Request>,
    pub response_sender: Sender<Response>,
    pub parser_task_sender: Sender<ParserTaskModel>,
    pub error_sender: Sender<ErrorTaskModel>,
    pub log_sender: Sender<LogModel>,
    // 多消费者 - 接收者被 Arc<Mutex> 包装以支持共享
    pub task_receiver: Arc<Mutex<Receiver<TaskModel>>>,
    pub request_receiver: Arc<Mutex<Receiver<Request>>>,
    pub download_request_receiver: Arc<Mutex<Receiver<Request>>>,
    pub response_receiver: Arc<Mutex<Receiver<Response>>>,
    pub parser_task_receiver: Arc<Mutex<Receiver<ParserTaskModel>>>,
    pub error_receiver: Arc<Mutex<Receiver<ErrorTaskModel>>>,
    pub log_receiver: Arc<Mutex<Receiver<LogModel>>>,
}
impl Clone for Channel {
    fn clone(&self) -> Self {
        Channel {
            task_sender: self.task_sender.clone(),
            request_sender: self.request_sender.clone(),
            download_request_sender: self.download_request_sender.clone(),
            response_sender: self.response_sender.clone(),
            parser_task_sender: self.parser_task_sender.clone(),
            error_sender: self.error_sender.clone(),
            log_sender: self.log_sender.clone(),
            task_receiver: Arc::clone(&self.task_receiver),
            request_receiver: Arc::clone(&self.request_receiver),
            download_request_receiver: Arc::clone(&self.download_request_receiver),
            response_receiver: Arc::clone(&self.response_receiver),
            parser_task_receiver: Arc::clone(&self.parser_task_receiver),
            error_receiver: Arc::clone(&self.error_receiver),
            log_receiver: Arc::clone(&self.log_receiver),
        }
    }
}

impl Channel {
    pub fn new(capacity: usize) -> Self {
        // 默认通道容量
        let (task_sender, task_receiver) = channel(capacity);
        let (request_sender, request_receiver) = channel(capacity);
        let (response_sender, response_receiver) = channel(capacity);
        let (error_sender, error_receiver) = channel(capacity);
        let (parser_task_sender, parser_task_receiver) = channel(capacity);
        let (download_request_sender, download_request_receiver) = channel(capacity);
        let (log_sender, log_receiver) = channel(1024);
        Channel {
            task_sender,
            request_sender,
            download_request_sender,
            response_sender,
            parser_task_sender,
            error_sender,
            log_sender,
            task_receiver: Arc::new(Mutex::new(task_receiver)),
            request_receiver: Arc::new(Mutex::new(request_receiver)),
            download_request_receiver: Arc::new(Mutex::new(download_request_receiver)),
            response_receiver: Arc::new(Mutex::new(response_receiver)),
            parser_task_receiver: Arc::new(Mutex::new(parser_task_receiver)),
            error_receiver: Arc::new(Mutex::new(error_receiver)),
            log_receiver: Arc::new(Mutex::new(log_receiver)),
        }
    }
}
