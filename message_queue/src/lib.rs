pub mod channel;
pub mod kafka;
pub mod queue_manager;
pub mod redis;

pub use crate::channel::Channel;
use async_trait::async_trait;
use utils::logger::LogModel;
use kernel::model::message::{ErrorTaskModel, ParserTaskModel};
use kernel::model::message::{TaskModel, TopicType};
use kernel::model::{Request, Response};
use error::Result;
pub use queue_manager::QueueManager;
pub use redis::RedisQueue;
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Mutex;

#[async_trait]
pub trait MessageQueue: Send + Sync {
    async fn push_task(&self, task_receiver: Arc<Mutex<Receiver<TaskModel>>>);
    async fn pop_task(&self, task_sender: Sender<TaskModel>);

    async fn push_request(&self, request_receiver: Arc<Mutex<Receiver<Request>>>);
    async fn pop_request(&self, request_sender: Sender<Request>);
    async fn push_response(&self, response_receiver: Arc<Mutex<Receiver<Response>>>);
    async fn pop_response(&self, response_sender: Sender<Response>);
    async fn push_parser_task(&self, parser_task_receiver: Arc<Mutex<Receiver<ParserTaskModel>>>);
    async fn pop_parser_task(&self, parser_task_sender: Sender<ParserTaskModel>);
    async fn push_error_message(&self, error_receiver: Arc<Mutex<Receiver<ErrorTaskModel>>>);
    async fn pop_error_message(&self, error_sender: Sender<ErrorTaskModel>);
    async fn push_log(&self, log_receiver: Arc<Mutex<Receiver<LogModel>>>);
    async fn size(&self, name: String, topic_type: &TopicType) -> Result<usize>;
    async fn clear(&self, name: String, topic_type: &TopicType) -> Result<()>;

    async fn subscribe_sender(&self, channel: Arc<Channel>) {
        // 使用 tokio::join! 并发执行所有的 pop 任务
        // 每个 pop 函数都是一个死循环，负责从外部消息队列(Kafka/Redis)获取数据
        // 并发送到对应的内部 sender 通道
        tokio::join!(
            self.pop_task(channel.task_sender.clone()),
            self.pop_request(channel.download_request_sender.clone()),
            self.pop_parser_task(channel.parser_task_sender.clone()),
            self.pop_error_message(channel.error_sender.clone()),
            self.pop_response(channel.response_sender.clone())
        );
    }

    async fn subscribe_receiver(&self, channel: Arc<Channel>) {
        tokio::join!(
            self.push_request(channel.request_receiver.clone()),
            self.push_response(channel.response_receiver.clone()),
            self.push_log(channel.log_receiver.clone()),
            self.push_parser_task(channel.parser_task_receiver.clone()),
            self.push_error_message(channel.error_receiver.clone())
        );
    }
}
