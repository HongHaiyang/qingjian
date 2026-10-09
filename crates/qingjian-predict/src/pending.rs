//! 待发联想及其取消代次；取消之前入队的请求不能在取消后发出。

use qingjian_core::PredictionRequest;

pub struct PendingRequest {
    pub request: PredictionRequest,

    pub generation: u64,
}
