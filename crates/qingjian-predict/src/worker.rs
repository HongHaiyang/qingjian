//! 后台线程：收请求、防抖、查缓存、发网络请求、回结果。

use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender};
use std::time::{Duration, Instant};

use qingjian_core::Prediction;
use tokio::sync::watch;

use crate::prompt::Reply;

use crate::cache::PredictionCache;
use crate::chat_client::ChatClient;
use crate::error::PredictError;
use crate::pending::PendingRequest;

/// 缓存条数。
const CACHE_CAPACITY: usize = 64;

pub struct Worker {
    /// 请求入口。主线程 drop 掉发送端后线程自然退出。
    requests: Receiver<PendingRequest>,

    /// 取消代次；发送端关闭也会停止正在等待的 HTTP。
    cancellations: watch::Receiver<u64>,

    /// 结果出口。
    responses: Sender<Prediction>,

    /// 网络客户端。
    client: ChatClient,

    /// 防抖窗口。
    debounce: Duration,

    /// 结果缓存。
    cache: PredictionCache,
}

/// 缓存里存的是解析后的回复。
type Cached = Reply;

impl Worker {
    pub fn new(
        requests: Receiver<PendingRequest>,
        responses: Sender<Prediction>,
        client: ChatClient,
        debounce: Duration,
        cancellations: watch::Receiver<u64>,
    ) -> Self {
        Self {
            requests,
            cancellations,
            responses,
            client,
            debounce,
            cache: PredictionCache::with_capacity(CACHE_CAPACITY),
        }
    }

    /// 阻塞运行直到发送端全部关闭。
    pub fn run(mut self) -> Result<(), PredictError> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        while let Ok(first) = self.requests.recv() {
            let Some(pending) = self.debounce(first) else {
                return Ok(());
            };
            if self.cancellations.has_changed().is_err() {
                return Ok(());
            }
            if pending.generation != *self.cancellations.borrow_and_update() {
                continue;
            }
            let request = pending.request;
            let key = PredictionCache::key(&request);
            if let Some(reply) = self.cache.get(&key) {
                tracing::debug!(sequence = request.sequence, kind = ?request.kind, "联想命中缓存");
                self.reply(request.sequence, reply.clone());
                continue;
            }
            let start = Instant::now();
            let result = runtime.block_on(async {
                tokio::select! {
                    biased;
                    _ = self.cancellations.changed() => None,
                    result = self.client.complete(&request) => Some(result),
                }
            });
            if pending.generation != *self.cancellations.borrow() || result.is_none() {
                tracing::debug!(sequence = request.sequence, "联想已取消");
                continue;
            }
            match result.expect("取消已处理") {
                Ok(reply) => {
                    tracing::info!(
                        sequence = request.sequence,
                        elapsed_ms = start.elapsed().as_millis(),
                        words = reply.words.len(),
                        sentence = reply.sentence.is_some(),
                        "联想完成"
                    );
                    // 空回复不进缓存：模型偶尔什么都不给（问字尤其），缓存住就等于这个问题以后永远没答案，
                    // 用户重打一遍也只会命中缓存（2026-09-07 `?mumumu` 就是这么卡住的）
                    if !reply.is_empty() {
                        self.cache.insert(key, reply.clone());
                    }
                    self.reply(request.sequence, reply);
                }
                Err(error) => {
                    tracing::warn!(sequence = request.sequence, %error, "联想失败");
                }
            }
        }
        Ok(())
    }

    /// 防抖：在窗口内持续收到新请求就一直等，只保留最后一个。发送端关闭返回 `None`。
    fn debounce(&self, mut latest: PendingRequest) -> Option<PendingRequest> {
        loop {
            match self.requests.recv_timeout(self.debounce) {
                Ok(newer) => latest = newer,
                Err(RecvTimeoutError::Timeout) => return Some(latest),
                Err(RecvTimeoutError::Disconnected) => return None,
            }
        }
    }

    fn reply(&self, sequence: u64, reply: Cached) {
        // 接收端没了说明 Predictor 已经被 drop，线程随后也会退出
        let _ = self.responses.send(Prediction {
            sequence,
            words: reply.words,
            sentence: reply.sentence,
        });
    }
}
