use error::Result;
use error::error::QueueError;
use utils::logger::LogModel;
use kernel::model::message::{TaskModel, TopicType};
use utils::config::RedisConfig;
use async_trait::async_trait;
use log::{debug, error, info};
// no need for AsyncCommands; we use low-level redis::cmd
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::mpsc::{Receiver, Sender};
use crate::MessageQueue;
use kernel::model::{Request,Response};
use kernel::model::message::{ParserTaskModel, ErrorTaskModel};
// use uuid::Uuid; // no longer needed

pub struct RedisQueue {
    redis_url: String,
}

impl RedisQueue {
    pub fn new(redis_config: &RedisConfig) -> RedisQueue {
        let server = format!("{}:{}", redis_config.redis_host, redis_config.redis_port);
        let redis_url = match (&redis_config.redis_username, &redis_config.redis_password) {
            (Some(user), Some(pass)) => format!("redis://{user}:{pass}@{server}?protocol=resp3"),
            (Some(user), None) => format!("redis://{user}@{server}?protocol=resp3"),
            (None, Some(pass)) => format!("redis://:{pass}@{server}?protocol=resp3"),
            (None, None) => format!("redis://{server}?protocol=resp3"),
        };

        RedisQueue { redis_url }
    }

    // 使用 Redis List 的各个键名
    // 为避免与之前的 Stream 键冲突，统一加前缀
    const LIST_TASK: &'static str = "queue:task";
    const LIST_REQUEST: &'static str = "queue:request";
    const LIST_RESPONSE: &'static str = "queue:response";
    const LIST_PARSER_TASK: &'static str = "queue:parser_task";
    const LIST_ERROR: &'static str = "queue:error_task";
    const LIST_LOG: &'static str = "queue:log";

    // RPUSH 写入 List（FIFO）
    async fn rpush(redis_url: &str, list: &str, data: Vec<u8>) -> Result<()> {
        let client = redis::Client::open(redis_url).map_err(|_e| QueueError::ConnectionFailed)?;
        let mut conn = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|_e| QueueError::ConnectionFailed)?;

        // RPUSH list data
        let _: i64 = redis::cmd("RPUSH")
            .arg(list)
            .arg(data)
            .query_async(&mut conn)
            .await
            .map_err(|e| QueueError::PushFailed(Box::new(e)))?;
        Ok(())
    }
    // 通用消费逻辑：BLPOP 阻塞拉取 List
    async fn blpop_loop<T, F>(
        redis_url: String,
        list: &'static str,
        mut handler: F,
    ) where
        T: for<'de> serde::Deserialize<'de> + Send + 'static,
        F: FnMut(T) -> std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send>> + Send + 'static,
    {
        // 建立长连接
        let client = match redis::Client::open(redis_url.as_str()) {
            Ok(c) => c,
            Err(e) => {
                error!("Failed to create Redis client: {e:?}");
                return;
            }
        };

        let mut conn = match client.get_multiplexed_async_connection().await {
            Ok(c) => c,
            Err(e) => {
                error!("Failed to connect to Redis: {e:?}");
                return;
            }
        };

        loop {
            // BLPOP list 1s 超时，避免死等，便于处理错误/重连
            let result: redis::Value = match redis::cmd("BLPOP").arg(list).arg(1).query_async(&mut conn).await {
                Ok(v) => v,
                Err(e) => {
                    error!("Error while BLPOP from list {list}: {e:?}");
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    continue;
                }
            };
            match result {
                redis::Value::Array(arr) if arr.len() >= 2 => {
                    // arr[0] 是 key，arr[1] 是 value（BulkString）
                    if let redis::Value::BulkString(bytes) = &arr[1] {
                        match serde_json::from_slice::<T>(bytes) {
                            Ok(msg) => {
                                let _ = handler(msg).await;
                            }
                            Err(e) => {
                                error!("Failed to deserialize list item from {list}: {e}");
                            }
                        }
                    }
                }
                redis::Value::Nil => {
                    // 超时无消息，继续循环
                }
                _ => {}
            }
        }
    }
}

#[async_trait]
impl MessageQueue for RedisQueue {

    async fn push_task(&self, task_receiver: Arc<Mutex<Receiver<TaskModel>>>) {
        let redis_url = self.redis_url.clone();

        tokio::spawn(async move {
            while let Some(task_model) = task_receiver.lock().await.recv().await {
                if let Ok(data) = serde_json::to_vec(&task_model) {
                    if let Err(e) = Self::rpush(&redis_url, Self::LIST_TASK, data).await {
                        error!("Failed to rpush task_model: {e:?}");
                    } else {
                        info!("Successfully appended task_model to list: {}", Self::LIST_TASK);
                    }
                } else {
                    error!("Failed to serialize task_model");
                }
            }
        });
    }
    
    async fn pop_task(&self, task_sender: Sender<TaskModel>) {
        let redis_url = self.redis_url.clone();
        tokio::spawn(async move {
            Self::blpop_loop::<TaskModel, _>(
                redis_url,
                Self::LIST_TASK,
                move |msg: TaskModel| {
                    let sender = task_sender.clone();
                    Box::pin(async move { sender.send(msg).await.is_ok() })
                },
            )
            .await;
        });
    }

    

    async fn push_request(&self, request_receiver: Arc<Mutex<Receiver<Request>>>) {
        let redis_url = self.redis_url.clone();

        tokio::spawn(async move {
            while let Some(request) = request_receiver.lock().await.recv().await {
                if let Ok(data) = serde_json::to_vec(&request) {
                    if let Err(e) = Self::rpush(&redis_url, Self::LIST_REQUEST, data).await {
                        error!("Failed to rpush request: {e:?}");
                    } else {
                        debug!("Successfully appended request to list: {}", Self::LIST_REQUEST);
                    }
                } else {
                    error!("Failed to serialize request");
                }
            }
        });
    }

    async fn pop_request(&self, request_sender: Sender<Request>) {
        let redis_url = self.redis_url.clone();
        tokio::spawn(async move {
            Self::blpop_loop::<Request, _>(
                redis_url,
                Self::LIST_REQUEST,
                move |msg: Request| {
                    let sender = request_sender.clone();
                    Box::pin(async move { sender.send(msg).await.is_ok() })
                },
            )
            .await;
        });
    }

    async fn push_response(&self, response_receiver: Arc<Mutex<Receiver<Response>>>) {
        let redis_url = self.redis_url.clone();
        tokio::spawn(async move {
            while let Some(response) = response_receiver.lock().await.recv().await {
                if let Ok(data) = serde_json::to_vec(&response) {
                    if let Err(e) = Self::rpush(&redis_url, Self::LIST_RESPONSE, data).await {
                        error!("Failed to rpush response: {e:?}");
                    } else {
                        info!("Successfully appended response to list: {}", Self::LIST_RESPONSE);
                    }
                } else {
                    error!("Failed to serialize response");
                }
            }
            info!("Response receiver channel closed, stopping push_response task");
        });
    }

    async fn pop_response(&self, response_sender: Sender<Response>) {
        let redis_url = self.redis_url.clone();
        tokio::spawn(async move {
            Self::blpop_loop::<Response, _>(
                redis_url,
                Self::LIST_RESPONSE,
                move |msg: Response| {
                    let sender = response_sender.clone();
                    Box::pin(async move { sender.send(msg).await.is_ok() })
                },
            )
            .await;
        });
    }

    async fn push_parser_task(
        &self,
        parser_task_receiver: Arc<Mutex<Receiver<ParserTaskModel>>>,
    ) {
        let redis_url = self.redis_url.clone();
        tokio::spawn(async move {
            while let Some(message) = parser_task_receiver.lock().await.recv().await {
                if let Ok(data) = serde_json::to_vec(&message) {
                    if let Err(e) = Self::rpush(&redis_url, Self::LIST_PARSER_TASK, data).await {
                        error!("Failed to rpush parser task: {e:?}");
                    } else {
                        info!("Successfully appended parser task to list: {}", Self::LIST_PARSER_TASK);
                    }
                } else {
                    error!("Failed to serialize parser task");
                }
            }
            info!("Parser task receiver channel closed, stopping push_parser_task task");
        });
    }

    async fn pop_parser_task(&self, parser_task_sender: Sender<ParserTaskModel>) {
        let redis_url = self.redis_url.clone();
        tokio::spawn(async move {
            Self::blpop_loop::<ParserTaskModel, _>(
                redis_url,
                Self::LIST_PARSER_TASK,
                move |msg: ParserTaskModel| {
                    let sender = parser_task_sender.clone();
                    Box::pin(async move { sender.send(msg).await.is_ok() })
                },
            )
            .await;
        });
    }

    async fn push_error_message(&self, error_receiver: Arc<Mutex<Receiver<ErrorTaskModel>>>) {
        let redis_url = self.redis_url.clone();
        tokio::spawn(async move {
            while let Some(error_msg) = error_receiver.lock().await.recv().await {
                if let Ok(data) = serde_json::to_vec(&error_msg) {
                    if let Err(e) = Self::rpush(&redis_url, Self::LIST_ERROR, data).await {
                        error!("Failed to rpush error message: {e:?}");
                    } else {
                        info!("Successfully appended error message to list: {}", Self::LIST_ERROR);
                    }
                } else {
                    error!("Failed to serialize error message");
                }
            }
            info!("Error message receiver channel closed, stopping push_error_message task");
        });
    }

    async fn pop_error_message(&self, error_sender: Sender<ErrorTaskModel>) {
        let redis_url = self.redis_url.clone();
        tokio::spawn(async move {
            Self::blpop_loop::<ErrorTaskModel, _>(
                redis_url,
                Self::LIST_ERROR,
                move |msg: ErrorTaskModel| {
                    let sender = error_sender.clone();
                    Box::pin(async move { sender.send(msg).await.is_ok() })
                },
            )
            .await;
        });
    }

    async fn push_log(&self, log_receiver: Arc<Mutex<Receiver<LogModel>>>) {
        let redis_url = self.redis_url.clone();

        tokio::spawn(async move {
            while let Some(log) = log_receiver.lock().await.recv().await {
                if let Ok(data) = serde_json::to_vec(&log) {
                    if let Err(e) = Self::rpush(&redis_url, Self::LIST_LOG, data).await {
                        error!("Failed to rpush log to list: {e:?}");
                    }
                }
            }
        });
    }

    async fn size(&self, _name: String, topic_type: &TopicType) -> Result<usize> {
        // 根据 TopicType 映射到对应的 List
        let list = match topic_type {
            TopicType::Task => Self::LIST_TASK,
            TopicType::Request => Self::LIST_REQUEST,
            TopicType::Response => Self::LIST_RESPONSE,
            TopicType::ParserTask => Self::LIST_PARSER_TASK,
            TopicType::Error => Self::LIST_ERROR,
        };
        let client = redis::Client::open(self.redis_url.as_str())
            .map_err(|_e| QueueError::ConnectionFailed)?;
        let mut conn = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|_e| QueueError::ConnectionFailed)?;
        let len: i64 = redis::cmd("LLEN")
            .arg(list)
            .query_async(&mut conn)
            .await
            .unwrap_or(0);
        Ok(len.max(0) as usize)
    }

    async fn clear(&self, _name: String, topic_type: &TopicType) -> Result<()> {
        // 使用 DEL 删除 List
        let list = match topic_type {
            TopicType::Task => Self::LIST_TASK,
            TopicType::Request => Self::LIST_REQUEST,
            TopicType::Response => Self::LIST_RESPONSE,
            TopicType::ParserTask => Self::LIST_PARSER_TASK,
            TopicType::Error => Self::LIST_ERROR,
        };
        let client = redis::Client::open(self.redis_url.as_str())
            .map_err(|_e| QueueError::ConnectionFailed)?;
        let mut conn = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|_e| QueueError::ConnectionFailed)?;
        let _: i64 = redis::cmd("DEL")
            .arg(list)
            .query_async(&mut conn)
            .await
            .unwrap_or(0);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn test_redis() {}
}
