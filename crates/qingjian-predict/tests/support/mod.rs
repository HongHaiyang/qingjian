//! 云联想黑盒测试：本地 HTTP 服务与最小 Engine。

mod request;
mod server;

pub use server::Server;

use std::time::{Duration, Instant};

use qingjian_core::dictionary::Dictionary;
use qingjian_core::{Engine, Prediction};
use qingjian_predict::{CloudPredictor, PredictConfig};

pub const DEBOUNCE: Duration = Duration::from_millis(100);
pub const WAIT: Duration = Duration::from_secs(2);

pub fn engine(server: &Server) -> Engine {
    let config = PredictConfig {
        base_url: server.base_url(),
        model: "local-fixture".to_owned(),
        api_key: Some("local-fixture-key".to_owned()),
        reasoning_effort: String::new(),
        debounce_ms: DEBOUNCE.as_millis() as u64,
        timeout_ms: 5000,
        ..PredictConfig::default()
    };
    let dictionary = Dictionary::parse("开发\tkai fa\t9000\n开饭\tkai fan\t800\n").unwrap();
    Engine::new(dictionary).with_predictor(Box::new(CloudPredictor::new(&config).unwrap()))
}

pub fn submit(engine: &mut Engine, input: &str) -> u64 {
    engine.set_input(input);
    engine.request_prediction(None, &[]).unwrap()
}

pub fn poll(engine: &mut Engine) -> Prediction {
    let deadline = Instant::now() + WAIT;
    loop {
        if let Some(prediction) = engine.poll_prediction() {
            return prediction;
        }
        assert!(Instant::now() < deadline, "最新联想结果没有返回");
        std::thread::sleep(Duration::from_millis(5));
    }
}
