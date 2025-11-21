use crate::v8::{JsReturn, V8Engine};
use std::path::Path;
use std::sync::{mpsc, Arc, Mutex};
use tokio::sync::oneshot;

/// A job submitted to the single-threaded JS worker.
pub struct JsJob {
    pub func: String,
    pub args: Vec<String>,
    pub reply: oneshot::Sender<Result<String, String>>, // normalized string result
}

/// Single-threaded V8 worker. Owns the V8 engine on a dedicated OS thread.
/// Safe to clone and share; communicates via channels only.
#[derive(Clone)]
pub struct JsWorker {
    tx: Arc<Mutex<mpsc::Sender<JsJob>>>,
}

impl JsWorker {
    /// Spawn a worker thread and load the given JS file into V8.
    pub fn new(js_path: &Path) -> Result<Self, String> {
        let (tx, rx) = mpsc::channel::<JsJob>();
        let (ready_tx, ready_rx) = mpsc::channel::<Result<(), String>>();
        let path = js_path.to_path_buf();

        std::thread::Builder::new()
            .name("v8-js-worker".to_string())
            .spawn(move || {
                let mut engine = match V8Engine::new() {
                    Ok(e) => e,
                    Err(e) => {
                        let _ = ready_tx.send(Err(format!("init V8 failed: {e}")));
                        return;
                    }
                };
                if let Err(e) = engine.exec_file(&path) {
                    let _ = ready_tx.send(Err(format!("exec file failed: {e}")));
                    return;
                }
                let _ = ready_tx.send(Ok(()));

                while let Ok(job) = rx.recv() {
                    let res = Self::call_engine(&mut engine, &job.func, &job.args)
                        .map_err(|e| e);
                    let _ = job.reply.send(res);
                }
            })
            .map_err(|e| format!("spawn worker failed: {e}"))?;

        match ready_rx.recv() {
            Ok(Ok(())) => Ok(Self { tx: Arc::new(Mutex::new(tx)) }),
            Ok(Err(e)) => Err(e),
            Err(e) => Err(format!("worker init channel closed: {e}")),
        }
    }

    /// Call a global JS function with stringified return value.
    pub async fn call_js(&self, func: &str, args: Vec<String>) -> Result<String, String> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let job = JsJob { func: func.to_string(), args, reply: reply_tx };
        self.tx
            .lock()
            .map_err(|_| "worker sender poisoned".to_string())?
            .send(job)
            .map_err(|e| format!("send job failed: {e}"))?;
        reply_rx.await.map_err(|e| format!("recv reply failed: {e}"))?
    }

    fn call_engine(engine: &mut V8Engine, func: &str, args: &[String]) -> Result<String, String> {
        let arg_refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        match engine.call_function(func, &arg_refs) {
            Ok(JsReturn::Text(s)) => Ok(s),
            Ok(JsReturn::Number(n)) => Ok(n.to_string()),
            Ok(JsReturn::Bool(b)) => Ok(b.to_string()),
            Ok(JsReturn::Json(v)) => Ok(v.to_string()),
            Ok(JsReturn::Bytes(bytes)) => Ok(Self::hex(&bytes)),
            Err(e) => Err(format!("call_function error: {e}")),
        }
    }

    #[inline]
    fn hex(bytes: &[u8]) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut out = String::with_capacity(bytes.len() * 2);
        for &b in bytes {
            out.push(HEX[(b >> 4) as usize] as char);
            out.push(HEX[(b & 0x0f) as usize] as char);
        }
        out
    }
}
