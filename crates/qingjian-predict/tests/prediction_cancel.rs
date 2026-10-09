//! 云联想的公开接口：显式取消释放旧请求，正常提交仍串行并合并待发请求。

mod support;

use support::{DEBOUNCE, Server, WAIT, engine, poll, submit};

#[test]
fn cancel_before_debounce_does_not_send_http() {
    let server = Server::new();
    let mut engine = engine(&server);
    submit(&mut engine, "kaifa");
    engine.cancel_prediction();

    assert!(
        server.receive(DEBOUNCE * 3).is_none(),
        "已经取消的防抖请求仍被发送到 HTTP 服务"
    );
    assert_eq!(engine.poll_prediction(), None);
}

#[test]
fn entering_private_input_cancels_the_pending_http_request() {
    let server = Server::new();
    let mut engine = engine(&server);
    submit(&mut engine, "kaifa");
    engine.set_private(true);

    assert!(
        server.receive(DEBOUNCE * 3).is_none(),
        "进入私密输入后，之前待发的联想仍被发送"
    );
    assert_eq!(engine.poll_prediction(), None);
}

#[test]
fn input_too_short_for_a_new_prediction_cancels_the_pending_http_request() {
    let server = Server::new();
    let mut engine = engine(&server);
    submit(&mut engine, "kaifa");
    engine.set_input("k");
    assert_eq!(engine.request_prediction(None, &[]), None);

    assert!(
        server.receive(DEBOUNCE * 3).is_none(),
        "拼音已不足以请求联想，之前待发的联想仍被发送"
    );
    assert_eq!(engine.poll_prediction(), None);
}

#[test]
fn cancel_in_flight_allows_a_new_request_before_the_old_timeout() {
    let server = Server::new();
    let mut engine = engine(&server);
    submit(&mut engine, "kaifa");
    let first = server.receive(WAIT).expect("第一条 HTTP 请求没有到达");
    assert_eq!(first.letters(), "kaifa");

    engine.cancel_prediction();
    let latest_sequence = submit(&mut engine, "kaifan");
    let latest = server
        .receive(WAIT)
        .expect("取消后新请求仍在等待旧请求的 5 秒超时");
    assert_eq!(latest.letters(), "kaifan");
    assert!(first.wait_closed(WAIT), "取消后旧 HTTP 连接没有释放");
    latest.respond("开饭啦");

    let prediction = poll(&mut engine);
    assert_eq!(prediction.sequence, latest_sequence);
    assert_eq!(prediction.sentence.as_deref(), Some("开饭啦"));
    assert_eq!(engine.poll_prediction(), None);
}

#[test]
fn ordinary_submissions_keep_single_flight_and_only_send_the_latest_pending_request() {
    let server = Server::new();
    let mut engine = engine(&server);
    submit(&mut engine, "kaifa");
    let first = server.receive(WAIT).expect("第一条 HTTP 请求没有到达");

    submit(&mut engine, "kaifang");
    let latest_sequence = submit(&mut engine, "kaifan");
    assert!(
        server.receive(DEBOUNCE * 3).is_none(),
        "普通输入不应抢占在飞请求或并发发送新请求"
    );
    first.respond("旧联想");

    let latest = server.receive(WAIT).expect("最新待发请求没有到达");
    assert_eq!(latest.letters(), "kaifan");
    latest.respond("开饭啦");
    let prediction = poll(&mut engine);
    assert_eq!(prediction.sequence, latest_sequence);
    assert_eq!(prediction.sentence.as_deref(), Some("开饭啦"));
    assert!(server.receive(DEBOUNCE * 3).is_none());
    assert_eq!(engine.poll_prediction(), None);
}

#[test]
fn cancelled_input_can_be_requested_again_without_reviving_old_pending_input() {
    let server = Server::new();
    let mut engine = engine(&server);
    submit(&mut engine, "kaifa");
    let first = server.receive(WAIT).expect("第一条 HTTP 请求没有到达");
    submit(&mut engine, "kaifang");

    engine.cancel_prediction();
    let latest_sequence = submit(&mut engine, "kaifa");
    let latest = server.receive(WAIT).expect("取消后的相同输入没有重新发送");
    assert_eq!(latest.letters(), "kaifa");
    assert!(first.wait_closed(WAIT), "取消后旧 HTTP 连接没有释放");
    latest.respond("开发新功能");

    let prediction = poll(&mut engine);
    assert_eq!(prediction.sequence, latest_sequence);
    assert_eq!(prediction.sentence.as_deref(), Some("开发新功能"));
    assert!(
        server.receive(DEBOUNCE * 3).is_none(),
        "取消前待发的输入又被发送"
    );
    assert_eq!(engine.poll_prediction(), None);
}

#[test]
fn dropping_the_engine_releases_the_in_flight_http_request() {
    let server = Server::new();
    let mut engine = engine(&server);
    submit(&mut engine, "kaifa");
    let request = server.receive(WAIT).expect("第一条 HTTP 请求没有到达");

    drop(engine);

    assert!(
        request.wait_closed(WAIT),
        "Engine 已销毁，旧 HTTP 连接仍在等待"
    );
    assert!(server.receive(DEBOUNCE * 3).is_none());
}

#[test]
fn a_successful_cached_reply_is_reused_after_cancellation() {
    let server = Server::new();
    let mut engine = engine(&server);
    let first_sequence = submit(&mut engine, "kaifa");
    let request = server.receive(WAIT).expect("第一条 HTTP 请求没有到达");
    request.respond("开发新功能");
    let first = poll(&mut engine);
    assert_eq!(first.sequence, first_sequence);
    assert_eq!(first.sentence.as_deref(), Some("开发新功能"));

    engine.cancel_prediction();
    let cached_sequence = submit(&mut engine, "kaifa");
    let cached = poll(&mut engine);
    assert_eq!(cached.sequence, cached_sequence);
    assert_eq!(cached.sentence, first.sentence);
    assert!(
        server.receive(DEBOUNCE * 3).is_none(),
        "取消清掉了已成功的缓存"
    );
    assert_eq!(engine.poll_prediction(), None);
}
