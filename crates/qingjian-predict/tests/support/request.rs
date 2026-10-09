//! 本地服务器收到的一次请求；测试决定何时放行响应。

use std::sync::mpsc::{Receiver, Sender};
use std::time::Duration;

use serde_json::Value;

pub struct Request {
    pub body: Value,

    pub(super) response: Sender<String>,

    pub(super) closed: Receiver<()>,
}

impl Request {
    pub fn letters(&self) -> String {
        let user = self.body["messages"][1]["content"].as_str().unwrap();
        serde_json::from_str::<Value>(user).unwrap()["letters"]
            .as_str()
            .unwrap()
            .to_owned()
    }

    pub fn respond(&self, sentence: &str) {
        let content = serde_json::json!({ "words": [], "sentence": sentence }).to_string();
        let body = serde_json::json!({
            "id": "local-fixture",
            "object": "chat.completion",
            "created": 0,
            "model": "local-fixture",
            "choices": [{
                "index": 0,
                "message": { "role": "assistant", "content": content },
                "finish_reason": "stop"
            }]
        });
        self.response.send(body.to_string()).unwrap();
    }

    pub fn wait_closed(&self, timeout: Duration) -> bool {
        self.closed.recv_timeout(timeout).is_ok()
    }
}
