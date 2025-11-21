use crate::channel::Channel;
use crate::{MessageQueue, ErrorTaskModel, ParserTaskModel};
use std::sync::Arc;
use utils::logger::LogModel;
use kernel::model::message::TaskModel;
use kernel::model::{Request, Response};
use tokio::sync::Mutex;
use tokio::sync::mpsc::{Receiver, Sender};

pub struct QueueManager {
    pub channel: Arc<Channel>,
    pub queue: Option<Arc<dyn MessageQueue>>,
}

impl QueueManager {
    pub fn new(queue: Option<Arc<dyn MessageQueue>>) -> Self {
        QueueManager {
            channel: Arc::new(Channel::new(32)),
            queue,
        }
    }
    pub fn with_queue(&mut self, queue: Arc<dyn MessageQueue>) {
        self.queue = Some(queue);
    }
    pub fn subscribe(&self) {
        if let Some(queue) = &self.queue {
            let queue_clone = Arc::clone(queue);
            let channel_clone = Arc::clone(&self.channel);

            // 在后台运行 sender 订阅
            tokio::spawn(async move {
                queue_clone.subscribe_sender(channel_clone).await;
            });

            let queue_clone = Arc::clone(queue);
            let channel_clone = Arc::clone(&self.channel);
            // 在后台运行 receiver 订阅
            tokio::spawn(async move {
                queue_clone.subscribe_receiver(channel_clone).await;
            });
        }
    }
    pub fn get_task_push_channel(&self) -> Sender<TaskModel> {
        self.channel.task_sender.clone()
    }
    pub fn get_task_pop_channel(&self) -> Arc<Mutex<Receiver<TaskModel>>> {
        Arc::clone(&self.channel.task_receiver)
    }
    pub fn get_request_pop_channel(&self) -> Arc<Mutex<Receiver<Request>>> {
        self.channel.download_request_receiver.clone()
    }
    pub fn get_request_push_channel(&self) -> Sender<Request> {
        if self.queue.is_none() {
            return self.channel.download_request_sender.clone();
        }
        self.channel.request_sender.clone()
    }
    pub fn get_response_push_channel(&self) -> Sender<Response> {
        self.channel.response_sender.clone()
    }
    pub fn get_response_pop_channel(&self) -> Arc<Mutex<Receiver<Response>>> {
        Arc::clone(&self.channel.response_receiver)
    }
    pub fn get_parser_task_pop_channel(&self) -> Arc<Mutex<Receiver<ParserTaskModel>>> {
        self.channel.parser_task_receiver.clone()
    }
    pub fn get_parser_task_push_channel(&self) -> Sender<ParserTaskModel> {
        self.channel.parser_task_sender.clone()
    }
    pub fn get_error_pop_channel(&self) -> Arc<Mutex<Receiver<ErrorTaskModel>>> {
        Arc::clone(&self.channel.error_receiver)
    }
    pub fn get_error_push_channel(&self) -> Sender<ErrorTaskModel> {
        self.channel.error_sender.clone()
    }
    pub fn get_log_pop_channel(&self) -> Sender<LogModel> {
        self.channel.log_sender.clone()
    }
}
