//! 只监听 loopback 的可控 HTTP 服务；所有读写与线程退出都有截止时间。

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use serde_json::Value;

use super::request::Request;

pub struct Server {
    address: SocketAddr,

    requests: Receiver<Request>,

    stop: Arc<AtomicBool>,

    thread: Option<JoinHandle<()>>,
}

impl Server {
    pub fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let (send, requests) = mpsc::channel();
        let stop = Arc::new(AtomicBool::new(false));
        let stopped = stop.clone();
        let thread = thread::spawn(move || {
            let mut connections = Vec::new();
            while !stopped.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        let send = send.clone();
                        let stopped = stopped.clone();
                        connections.push(thread::spawn(move || serve(stream, send, stopped)));
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(error) => panic!("本地 HTTP 服务无法接收请求: {error}"),
                }
            }
            for connection in connections {
                connection.join().unwrap();
            }
        });
        Self {
            address,
            requests,
            stop,
            thread: Some(thread),
        }
    }

    pub fn base_url(&self) -> String {
        format!("http://{}/v1", self.address)
    }

    pub fn receive(&self, timeout: Duration) -> Option<Request> {
        self.requests.recv_timeout(timeout).ok()
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            thread.join().unwrap();
        }
    }
}

fn serve(mut stream: TcpStream, requests: Sender<Request>, stop: Arc<AtomicBool>) {
    stream
        .set_read_timeout(Some(Duration::from_millis(100)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_millis(100)))
        .unwrap();
    let Some(body) = read_body(&mut stream, &stop) else {
        return;
    };
    let (response, replies) = mpsc::channel();
    let (closed, closure) = mpsc::channel();
    if requests
        .send(Request {
            body,
            response,
            closed: closure,
        })
        .is_err()
    {
        return;
    }
    stream.set_nonblocking(true).unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while !stop.load(Ordering::Relaxed) && Instant::now() < deadline {
        if let Ok(body) = replies.try_recv() {
            stream.set_nonblocking(false).unwrap();
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.write_all(response.as_bytes());
            return;
        }
        match stream.peek(&mut [0_u8; 1]) {
            Ok(0) => {
                let _ = closed.send(());
                return;
            }
            Err(error) if error.kind() != std::io::ErrorKind::WouldBlock => return,
            _ => thread::sleep(Duration::from_millis(5)),
        }
    }
}

fn read_body(stream: &mut TcpStream, stop: &AtomicBool) -> Option<Value> {
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut data = Vec::new();
    let mut buffer = [0_u8; 4096];
    while !stop.load(Ordering::Relaxed) && Instant::now() < deadline {
        match stream.read(&mut buffer) {
            Ok(0) => return None,
            Ok(read) => data.extend_from_slice(&buffer[..read]),
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                continue;
            }
            Err(_) => return None,
        }
        if let Some(end) = data.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
            let headers = std::str::from_utf8(&data[..end]).unwrap();
            assert!(headers.starts_with("POST /v1/chat/completions "));
            let length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().unwrap())
                })
                .unwrap();
            if data.len() >= end + 4 + length {
                return Some(serde_json::from_slice(&data[end + 4..end + 4 + length]).unwrap());
            }
        }
        assert!(data.len() < 65536, "本地测试请求超出大小上限");
    }
    None
}
