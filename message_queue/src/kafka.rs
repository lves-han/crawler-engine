use error::{QueueError, Result};
use kernel::model::message::ErrorTaskModel;
use kernel::model::message::ParserTaskModel;
use std::option::Option;

use crate::MessageQueue;
use kernel::model::message::{TaskModel, TopicType};
use kernel::model::{Request, Response};
use log::{error, info};
use rdkafka::Message;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::sync::mpsc::{Receiver, Sender};
use utils::logger::LogModel;

#[derive(Clone)]
pub struct KafkaQueue {
    broker: String,
    username: Option<String>,
    password: Option<String>,
    // 连接池
    producer_pool: Arc<Mutex<Vec<FutureProducer>>>,
    consumer_pool: Arc<Mutex<HashMap<String, StreamConsumer>>>, // key: group_id:topic
    max_pool_size: usize,
}

impl KafkaQueue {
    pub fn new(broker: String, username: Option<String>, password: Option<String>) -> Result<Self> {
        Ok(KafkaQueue {
            broker,
            username,
            password,
            producer_pool: Arc::new(Mutex::new(Vec::new())),
            consumer_pool: Arc::new(Mutex::new(HashMap::new())),
            max_pool_size: 10, // 默认连接池大小
        })
    }

    // 主题与消费组常量（所有节点统一这些 group id，确保同一条消息只会被一个节点处理）
    const TOPIC_TASK: &'static str = "task";
    const TOPIC_REQUEST: &'static str = "request";
    const TOPIC_RESPONSE: &'static str = "response";
    const TOPIC_PARSER_TASK: &'static str = "parser_task";
    const TOPIC_ERROR: &'static str = "error_task";
    const TOPIC_LOG: &'static str = "log";

    const GROUP_TASK: &'static str = "crawler_task_group";
    const GROUP_REQUEST: &'static str = "crawler_request_group";
    const GROUP_RESPONSE: &'static str = "crawler_response_group";
    const GROUP_PARSER_TASK: &'static str = "crawler_parser_group";
    const GROUP_ERROR: &'static str = "crawler_error_group";
    const GROUP_LOG: &'static str = "crawler_log_group";

    // 创建生产者配置
    fn create_producer_config(&self) -> ClientConfig {
        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", &self.broker);

        if let (Some(username), Some(password)) = (&self.username, &self.password) {
            config
                .set("security.protocol", "SASL_PLAINTEXT")
                .set("sasl.mechanism", "PLAIN")
                .set("sasl.username", username)
                .set("sasl.password", password);
        }

        config
    }

    // 创建消费者配置
    fn create_consumer_config(&self, group_id: &str) -> ClientConfig {
        let mut config = self.create_producer_config();
        config
            .set("group.id", group_id)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .set("allow.auto.create.topics", "true") // 允许自动创建主题
            .set("session.timeout.ms", "30000") // 会话超时
            .set("heartbeat.interval.ms", "10000") // 心跳间隔
            .set("fetch.wait.max.ms", "500"); // 降低拉取等待时间
        config
    }

    // 从连接池获取或创建生产者
    async fn get_producer(&self) -> Result<FutureProducer> {
        let mut pool = self.producer_pool.lock().await;

        if let Some(producer) = pool.pop() {
            Ok(producer)
        } else {
            let config = self.create_producer_config();
            config
                .create()
                .map_err(|_| QueueError::ConnectionFailed.into())
        }
    }

    // 将生产者归还到连接池
    async fn return_producer(&self, producer: FutureProducer) {
        let mut pool = self.producer_pool.lock().await;
        if pool.len() < self.max_pool_size {
            pool.push(producer);
        }
        // 如果连接池已满，直接丢弃连接
    }

    // 从连接池获取或创建消费者
    async fn get_consumer(&self, group_id: &str, topic: &str) -> Result<StreamConsumer> {
        let mut pool = self.consumer_pool.lock().await;

        let key = format!("{group_id}:{topic}");
        if let Some(consumer) = pool.remove(&key) {
            Ok(consumer)
        } else {
            let config = self.create_consumer_config(group_id);
            let consumer: StreamConsumer =
                config.create().map_err(|_| QueueError::ConnectionFailed)?;

            consumer
                .subscribe(&[topic])
                .map_err(|_| QueueError::ConnectionFailed)?;

            Ok(consumer)
        }
    }

    // 将消费者归还到连接池
    async fn return_consumer(&self, group_id: String, topic: String, consumer: StreamConsumer) {
        let mut pool = self.consumer_pool.lock().await;
        if pool.len() < self.max_pool_size {
            let key = format!("{group_id}:{topic}");
            pool.insert(key, consumer);
        }
        // 如果连接池已满，直接丢弃连接
    }

    // 发布消息到指定topic（使用连接池）
    async fn publish_to_topic(&self, topic: &str, data: Vec<u8>) -> Result<()> {
        let producer = self.get_producer().await?;
        let record = FutureRecord::<(), Vec<u8>>::to(topic).payload(&data);

        let result = producer
            .send(record, Duration::from_secs(10))
            .await
            .map_err(|(e, _)| QueueError::PushFailed(Box::new(e)));

        // 归还连接到池中
        self.return_producer(producer).await;
        result.map(|_| ()).map_err(|e| e.into())
    }

    // 订阅topic并接收消息（使用连接池）
    async fn subscribe_and_receive(&self, topic: &str, group_id: &str) -> Result<Option<Vec<u8>>> {
        let consumer = self.get_consumer(group_id, topic).await?;

        // 设置超时接收消息
        let result = match tokio::time::timeout(Duration::from_millis(1000), consumer.recv()).await
        {
            Ok(Ok(message)) => {
                if let Some(payload) = message.payload() {
                    // 手动提交offset
                    consumer
                        .commit_message(&message, rdkafka::consumer::CommitMode::Async)
                        .map_err(|e| QueueError::PopFailed(Box::new(e)))?;

                    Ok(Some(payload.to_vec()))
                } else {
                    Ok(None)
                }
            }
            Ok(Err(_)) => Ok(None),
            Err(_) => Ok(None), // 超时
        };

        // 归还连接到池中
        self.return_consumer(group_id.to_string(), topic.to_string(), consumer).await;
        result
    }
}

#[async_trait::async_trait]
impl MessageQueue for KafkaQueue {
    async fn push_task(&self, task_receiver: Arc<Mutex<Receiver<TaskModel>>>) {
        let self_clone = self.clone();

        tokio::spawn(async move {
            while let Some(task_model) = task_receiver.lock().await.recv().await {
                info!("Publishing task_model to topic: {}", Self::TOPIC_TASK);
                if let Ok(data) = serde_json::to_vec(&task_model) {
                    if let Err(e) = self_clone.publish_to_topic(Self::TOPIC_TASK, data).await {
                        error!("Failed to publish task_model: {e:?}");
                    }
                }
            }
        });
    }

    async fn pop_task(&self, task_sender: Sender<TaskModel>) {
        let self_clone = Arc::new(self.clone());

        tokio::spawn(async move {
            loop {
                match self_clone
                    .subscribe_and_receive(Self::TOPIC_TASK, Self::GROUP_TASK)
                    .await
                {
                    Ok(Some(data)) => {
                        if let Ok(task_model) = serde_json::from_slice::<TaskModel>(&data) {
                            if task_sender.send(task_model).await.is_err() {
                                error!("Failed to send task to internal channel");
                                break;
                            }
                        }
                    }
                    Ok(None) => {
                        // 没有消息，短暂等待
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                    Err(e) => {
                        error!("Error receiving task: {e:?}");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        });
    }

    async fn push_request(&self, request_receiver: Arc<Mutex<Receiver<Request>>>) {
        let self_clone = self.clone();

        tokio::spawn(async move {
            while let Some(request) = request_receiver.lock().await.recv().await {
                info!("Publishing request to topic: {}", Self::TOPIC_REQUEST);
                if let Ok(data) = serde_json::to_vec(&request) {
                    if let Err(e) = self_clone.publish_to_topic(Self::TOPIC_REQUEST, data).await {
                        error!("Failed to publish request: {e:?}");
                    }
                }
            }
        });
    }

    async fn pop_request(&self, request_sender: Sender<Request>) {
        let self_clone = self.clone();

        tokio::spawn(async move {
            loop {
                // 这里简化处理，实际应该订阅所有 *:request 的topic
                // Kafka不像Redis支持模式匹配，需要预先知道topic名称
                match self_clone
                    .subscribe_and_receive(Self::TOPIC_REQUEST, Self::GROUP_REQUEST)
                    .await
                {
                    Ok(Some(data)) => {
                        if let Ok(request) = serde_json::from_slice::<Request>(&data) {
                            info!("Popping response to topic: {request:?}");
                            if request_sender.send(request).await.is_err() {
                                error!("Failed to send request to internal channel");
                                break;
                            }
                        }
                    }
                    Ok(None) => {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                    Err(e) => {
                        error!("Error receiving request: {e:?}");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        });
    }

    async fn push_response(&self, response_receiver: Arc<Mutex<Receiver<Response>>>) {
        let self_clone = self.clone();

        tokio::spawn(async move {
            while let Some(response) = response_receiver.lock().await.recv().await {
                if let Ok(data) = serde_json::to_vec(&response) {
                    if let Err(e) = self_clone.publish_to_topic(Self::TOPIC_RESPONSE, data).await {
                        error!("Failed to publish response: {e:?}");
                    }
                }
            }
        });
    }

    async fn pop_response(&self, response_sender: Sender<Response>) {
        let self_clone = self.clone();

        tokio::spawn(async move {
            loop {
                // 这里简化处理，实际应该订阅所有 *:response 的topic
                // Kafka不像Redis支持模式匹配，需要预先知道topic名称
                match self_clone
                    .subscribe_and_receive(Self::TOPIC_RESPONSE, Self::GROUP_RESPONSE)
                    .await
                {
                    Ok(Some(data)) => {
                        if let Ok(response) = serde_json::from_slice::<Response>(&data) {
                            info!("Popping response to topic: {response:?}");
                            if response_sender.send(response).await.is_err() {
                                error!("Failed to send response to internal channel");
                                break;
                            }
                        }
                    }
                    Ok(None) => {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                    Err(e) => {
                        error!("Error receiving request: {e:?}");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        });
    }

    async fn push_parser_task(
        &self,
        parser_task_receiver: Arc<Mutex<Receiver<ParserTaskModel>>>,
    ) {
        let self_clone = self.clone();
        tokio::spawn(async move {
            while let Some(parser_task) = parser_task_receiver.lock().await.recv().await {
                if let Ok(data) = serde_json::to_vec(&parser_task) {
                    if let Err(e) = self_clone.publish_to_topic(Self::TOPIC_PARSER_TASK, data).await {
                        error!("Failed to publish parser task: {e:?}");
                    }
                }
            }
        });
    }

    async fn pop_parser_task(&self, parser_task_sender: Sender<ParserTaskModel>) {
        let self_clone = self.clone();

        tokio::spawn(async move {
            loop {
                match self_clone
                    .subscribe_and_receive(Self::TOPIC_PARSER_TASK, Self::GROUP_PARSER_TASK)
                    .await
                {
                    Ok(Some(data)) => {
                        if let Ok(parser_task) = serde_json::from_slice::<ParserTaskModel>(&data)
                        {
                            if parser_task_sender.send(parser_task).await.is_err() {
                                error!("Failed to send parser task to internal channel");
                                break;
                            }
                        }
                    }
                    Ok(None) => {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                    Err(e) => {
                        error!("Error receiving parser task: {e:?}");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        });
    }

    async fn push_error_message(&self, error_receiver: Arc<Mutex<Receiver<ErrorTaskModel>>>) {
        let self_clone = self.clone();
        tokio::spawn(async move {
            while let Some(error_msg) = error_receiver.lock().await.recv().await {
                if let Ok(data) = serde_json::to_vec(&error_msg) {
                    if let Err(e) = self_clone.publish_to_topic(Self::TOPIC_ERROR, data).await {
                        error!("Failed to publish error: {e:?}");
                    }
                }
            }
        });
    }

    async fn pop_error_message(&self, error_sender: Sender<ErrorTaskModel>) {
        let self_clone = self.clone();

        tokio::spawn(async move {
            loop {
                match self_clone
                    .subscribe_and_receive(Self::TOPIC_ERROR, Self::GROUP_ERROR)
                    .await
                {
                    Ok(Some(data)) => {
                        if let Ok(error_msg) = serde_json::from_slice::<ErrorTaskModel>(&data) {
                            if error_sender.send(error_msg).await.is_err() {
                                error!("Failed to send error to internal channel");
                                break;
                            }
                        }
                    }
                    Ok(None) => {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                    Err(e) => {
                        error!("Error receiving error: {e:?}");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        });
    }

    async fn push_log(&self, log_receiver: Arc<Mutex<Receiver<LogModel>>>) {
        let self_clone = self.clone();
        tokio::spawn(async move {
            while let Some(log) = log_receiver.lock().await.recv().await {
                if let Ok(data) = serde_json::to_vec(&log) {
                    if let Err(e) = self_clone.publish_to_topic(Self::TOPIC_LOG, data).await {
                        error!("Failed to publish error: {e:?}");
                    }
                }
            }
        });
    }

    async fn size(&self, _name: String, _topic_type: &TopicType) -> Result<usize> {
        // Kafka需要通过admin API获取topic信息，这里简化返回0
        Ok(0)
    }

    async fn clear(&self, _name: String, _topic_type: &TopicType) -> Result<()> {
        // Kafka不支持清空topic，只能删除重建，这里简化处理
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn test_kafka() {
        // 测试代码可以在这里添加
    }
}
