//! Rust 回环测试：覆盖请求/响应/事件/二进制请求/并发/CRC 重同步。

use std::io::Write;
use std::net::{TcpListener, TcpStream};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::Duration;

use ipcprotocol::json::{self, Json};
use ipcprotocol::message_stream::MessageStream;
use ipcprotocol::rpc_protocol::{RpcError, RpcProtocol};

fn boxed_stream(s: &TcpStream) -> (Box<dyn std::io::Read + Send>, Box<dyn std::io::Write + Send>) {
    (Box::new(s.try_clone().unwrap()), Box::new(s.try_clone().unwrap()))
}

struct ServerHandle {
    addr: std::net::SocketAddr,
    sync_rx: mpsc::Receiver<()>,
    ready_rx: Mutex<mpsc::Receiver<Arc<RpcProtocol>>>,
}

fn start_server() -> ServerHandle {
    let ln = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = ln.local_addr().unwrap();
    let (sync_tx, sync_rx) = mpsc::channel();
    let (ready_tx, ready_rx) = mpsc::channel::<Arc<RpcProtocol>>();
    thread::spawn(move || {
        let (conn, _) = ln.accept().unwrap();
        let (r, w) = boxed_stream(&conn);
        let ms = MessageStream::new(r, w);
        ms.on_sync_complete(Box::new(move || {
            let _ = sync_tx.send(());
        }));
        let rpc = RpcProtocol::new(ms);
        rpc.on_error(Box::new(|_| {}));
        register_test_methods(&rpc);
        let _ = ready_tx.send(rpc.clone());
        // 保持连接存活直到进程结束
        std::mem::forget(conn);
    });
    ServerHandle { addr, sync_rx, ready_rx: Mutex::new(ready_rx) }
}

fn register_test_methods(rpc: &Arc<RpcProtocol>) {
    rpc.register_method("add", Box::new(|a| {
        Ok(Json::Num(a[0].as_f64().unwrap() + a[1].as_f64().unwrap()))
    }));
    rpc.register_method("echo", Box::new(|a| Ok(a[0].clone())));
    rpc.register_method("greet", Box::new(|a| {
        Ok(Json::Str(format!("你好, {}!", a[0].as_str().unwrap())))
    }));
    rpc.register_method("blob", Box::new(|_| Ok(Json::Bytes(vec![0, 1, 2, 250, 255]))));
    rpc.register_method("nothing", Box::new(|_| Ok(Json::Null)));
    rpc.register_method("fail", Box::new(|a| Err(format!("boom: {}", json::stringify(&a[0])))));
    {
        let rpc2 = rpc.clone();
        rpc.register_method("pushEvents", Box::new(move |_| {
            let _ = rpc2.send_text("text-event");
            let mut v = Json::obj();
            v.insert("n", Json::Num(42.0));
            let _ = rpc2.send_json(&v);
            let _ = rpc2.send_binary(&[9, 8, 7]);
            Ok(Json::Str("sent".into()))
        }));
    }
    rpc.register_method("upload", Box::new(|a| {
        let data = a[0].as_bytes().unwrap();
        let mut m = Json::obj();
        m.insert("size", Json::Num(data.len() as f64));
        m.insert("tag", a[1].clone());
        m.insert("head", Json::Arr(data[..4].iter().map(|b| Json::Num(*b as f64)).collect()));
        Ok(m)
    }));
    rpc.register_method("slow", Box::new(|_| {
        thread::sleep(Duration::from_millis(500));
        Ok(Json::Str("late".into()))
    }));
}

fn dial(server: &ServerHandle) -> (Arc<RpcProtocol>, TcpStream) {
    let conn = TcpStream::connect(server.addr).unwrap();
    // 等服务端注册完方法再启动客户端读循环
    let _server_rpc = server.ready_rx.lock().unwrap().recv_timeout(Duration::from_secs(3)).unwrap();
    let (r, w) = boxed_stream(&conn);
    let rpc = RpcProtocol::new(MessageStream::new(r, w));
    rpc.on_error(Box::new(|_| {}));
    (rpc, conn)
}

#[test]
fn json_request_and_unicode() {
    let server = start_server();
    let (rpc, _conn) = dial(&server);
    let v = rpc.send_request("add", Json::Arr(vec![Json::Num(3.0), Json::Num(4.0)])).unwrap();
    assert_eq!(v.as_f64(), Some(7.0));

    let mut obj = Json::obj();
    obj.insert("x", Json::Arr(vec![Json::Num(1.0), Json::Str("二".into()), Json::Bool(true), Json::Null]));
    let v = rpc.send_request("echo", obj).unwrap();
    assert_eq!(v.get("x").unwrap().as_arr().unwrap()[1].as_str(), Some("二"));
    rpc.close();
}

#[test]
fn text_binary_null_responses() {
    let server = start_server();
    let (rpc, _conn) = dial(&server);
    let v = rpc.send_request("greet", Json::Str("世界".into())).unwrap();
    assert_eq!(v.as_str(), Some("你好, 世界!"));
    let v = rpc.send_request("blob", Json::Null).unwrap();
    assert_eq!(v.as_bytes(), Some(&[0u8, 1, 2, 250, 255][..]));
    let v = rpc.send_request("nothing", Json::Null).unwrap();
    assert_eq!(v, Json::Null);
    rpc.close();
}

#[test]
fn error_responses() {
    let server = start_server();
    let (rpc, _conn) = dial(&server);
    let err = rpc.send_request("fail", Json::Str("x".into())).unwrap_err();
    match err {
        RpcError::Remote(v) => assert_eq!(v.as_str(), Some("boom: \"x\"")),
        _ => panic!("expected remote error"),
    }
    let err = rpc.send_request("nope", Json::Null).unwrap_err();
    match err {
        RpcError::Remote(v) => assert_eq!(v.as_str(), Some("Method not found: nope")),
        _ => panic!("expected remote error"),
    }
    rpc.close();
}

#[test]
fn binary_request_75kb() {
    let server = start_server();
    let (rpc, _conn) = dial(&server);
    let data: Vec<u8> = (0..75 * 1024).map(|i| (i % 251) as u8).collect();
    let v = rpc
        .send_request_binary("upload", &data, vec![Json::Str("tag-1".into())])
        .unwrap();
    assert_eq!(v.get("size").unwrap().as_f64(), Some(data.len() as f64));
    assert_eq!(v.get("tag").unwrap().as_str(), Some("tag-1"));
    let head = v.get("head").unwrap().as_arr().unwrap();
    assert_eq!(head.len(), 4);
    assert_eq!(head[3].as_f64(), Some(3.0));
    rpc.close();
}

#[test]
fn events() {
    let server = start_server();
    let (rpc, _conn) = dial(&server);
    let events: Arc<Mutex<Vec<Json>>> = Arc::new(Mutex::new(Vec::new()));
    let (tx, rx) = mpsc::channel();
    {
        let events = events.clone();
        rpc.on_event(Box::new(move |_ct, content| {
            let mut ev = events.lock().unwrap();
            ev.push(content);
            if ev.len() == 3 {
                let _ = tx.send(());
            }
        }));
    }
    rpc.send_request("pushEvents", Json::Null).unwrap();
    rx.recv_timeout(Duration::from_secs(3)).unwrap();
    let ev = events.lock().unwrap();
    assert_eq!(ev[0].as_str(), Some("text-event"));
    assert_eq!(ev[1].get("n").unwrap().as_f64(), Some(42.0));
    assert_eq!(ev[2].as_bytes(), Some(&[9u8, 8, 7][..]));
    rpc.close();
}

#[test]
fn concurrent_50() {
    let server = start_server();
    let (rpc, _conn) = dial(&server);
    let mut handles = Vec::new();
    for i in 0..50 {
        let rpc = rpc.clone();
        handles.push(thread::spawn(move || {
            let v = rpc
                .send_request("add", Json::Arr(vec![Json::Num(i as f64), Json::Num(1.0)]))
                .unwrap();
            assert_eq!(v.as_f64(), Some((i + 1) as f64));
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    rpc.close();
}

#[test]
fn crc_recovery() {
    let server = start_server();
    let (rpc, mut conn) = dial(&server);

    // 注入坏长度头 CRC 帧
    let payload = b"hello";
    let mut frame = Vec::new();
    frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    frame.extend_from_slice(&0xDEADBEEFu32.to_le_bytes());
    frame.extend_from_slice(payload);
    frame.extend_from_slice(&ipcprotocol::message_stream::crc32(payload).to_le_bytes());
    conn.write_all(&frame).unwrap();

    server.sync_rx.recv_timeout(Duration::from_secs(3)).unwrap();
    // 恢复后通信继续
    let v = rpc.send_request("add", Json::Arr(vec![Json::Num(10.0), Json::Num(20.0)])).unwrap();
    assert_eq!(v.as_f64(), Some(30.0));
    rpc.close();
}

#[test]
fn rpc_timeout() {
    let server = start_server();
    let (rpc, _conn) = dial(&server);

    assert_eq!(rpc.default_timeout(), Duration::from_secs(15));
    // 显式短超时触发 ETIMEDOUT
    let err = rpc
        .send_request_timeout("slow", Json::Null, Duration::from_millis(100))
        .unwrap_err();
    assert_eq!(err.code(), "ETIMEDOUT");
    assert!(matches!(err, RpcError::Timeout(_)));
    // pending 已清理，后续调用正常
    let v = rpc.send_request("add", Json::Arr(vec![Json::Num(1.0), Json::Num(2.0)])).unwrap();
    assert_eq!(v.as_f64(), Some(3.0));
    // 等 slow 的迟到响应到达（应被静默丢弃）
    thread::sleep(Duration::from_millis(600));
    let v = rpc.send_request("add", Json::Arr(vec![Json::Num(5.0), Json::Num(6.0)])).unwrap();
    assert_eq!(v.as_f64(), Some(11.0));
    rpc.close();
}

// ---- v1.1 用例（PROTOCOL.md §4.5/§8.1/§8.2） ----

/// 新建一对 rpc 端点（服务端只注册 add）。
fn pair(
    hb_interval: Duration,
    hb_timeout: Duration,
    auth_token: Option<&str>,
    required_token: Option<&str>,
) -> (Arc<RpcProtocol>, Arc<RpcProtocol>) {
    let ln = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = ln.local_addr().unwrap();
    let (ready_tx, ready_rx) = mpsc::channel::<Arc<RpcProtocol>>();
    let required = required_token.map(|s| s.to_string());
    thread::spawn(move || {
        let (conn, _) = ln.accept().unwrap();
        let (r, w) = boxed_stream(&conn);
        let server = RpcProtocol::new_with_options(MessageStream::new(r, w), hb_interval, hb_timeout, None, required);
        server.on_error(Box::new(|_| {}));
        server.register_method("add", Box::new(|a| {
            Ok(Json::Num(a[0].as_f64().unwrap() + a[1].as_f64().unwrap()))
        }));
        let _ = ready_tx.send(server.clone());
        std::mem::forget(conn);
    });
    let conn = TcpStream::connect(addr).unwrap();
    let (r, w) = boxed_stream(&conn);
    let cli = RpcProtocol::new_with_options(
        MessageStream::new(r, w),
        hb_interval,
        hb_timeout,
        auth_token.map(|s| s.to_string()),
        None,
    );
    cli.on_error(Box::new(|_| {}));
    let server = ready_rx.recv_timeout(Duration::from_secs(3)).unwrap();
    std::mem::forget(conn);
    (cli, server)
}

#[test]
fn hello_ok() {
    let (cli, server) = pair(Duration::ZERO, Duration::ZERO, Some("secret"), Some("secret"));
    let ack = cli.hello().unwrap();
    assert_eq!(ack.get("ok").unwrap().is_truthy(), true);
    assert_eq!(ack.get("version").unwrap().as_f64(), Some(1.0));
    let v = cli.send_request("add", Json::Arr(vec![Json::Num(1.0), Json::Num(2.0)])).unwrap();
    assert_eq!(v.as_f64(), Some(3.0));
    cli.close();
    server.close();
}

#[test]
fn hello_wrong_token() {
    let (cli, server) = pair(Duration::ZERO, Duration::ZERO, Some("wrong"), Some("secret"));
    let err = cli.hello().unwrap_err();
    assert_eq!(err.code(), "EAUTH");
    match err {
        RpcError::Coded { message, .. } => assert_eq!(message, "Unauthorized"),
        _ => panic!("expected coded error"),
    }
    cli.close();
    server.close();
}

#[test]
fn unauthorized_request() {
    let (cli, server) = pair(Duration::ZERO, Duration::ZERO, None, Some("secret"));
    let err = cli.send_request("add", Json::Arr(vec![Json::Num(1.0), Json::Num(2.0)])).unwrap_err();
    match err {
        RpcError::Remote(v) => assert_eq!(v.as_str(), Some("Unauthorized")),
        _ => panic!("expected remote error, got {:?}", err),
    }
    cli.close();
    server.close();
}

#[test]
fn hello_version_mismatch() {
    let ln = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = ln.local_addr().unwrap();
    thread::spawn(move || {
        let (conn, _) = ln.accept().unwrap();
        let (r, w) = boxed_stream(&conn);
        let ms = MessageStream::new(r, w);
        let ms2 = ms.clone();
        ms.on_message(Box::new(move |data: Vec<u8>| {
            // 裸对端：回 HELLO_ACK version=99（回显请求 id）
            let mut msg = vec![0u8; 18];
            msg[0] = 6; // TYPE_HELLO_ACK
            msg[1] = 1; // CONTENT_JSON
            msg[2..18].copy_from_slice(&data[2..18]);
            msg.extend_from_slice(b"{\"version\":99,\"ok\":true}");
            let _ = ms2.send(&msg);
        }));
        ms.start();
        std::mem::forget(conn);
    });
    let conn = TcpStream::connect(addr).unwrap();
    let (r, w) = boxed_stream(&conn);
    let cli = RpcProtocol::new_with_options(MessageStream::new(r, w), Duration::ZERO, Duration::ZERO, None, None);
    cli.on_error(Box::new(|_| {}));
    let err = cli.hello().unwrap_err();
    assert_eq!(err.code(), "EVERSION");
    cli.close();
    std::mem::forget(conn);
}

#[test]
fn heartbeat_keepalive() {
    let (cli, server) = pair(Duration::from_millis(100), Duration::from_secs(5), None, None);
    thread::sleep(Duration::from_millis(500)); // 期间应有多轮 PING/PONG 交换
    let v = cli.send_request("add", Json::Arr(vec![Json::Num(3.0), Json::Num(4.0)])).unwrap();
    assert_eq!(v.as_f64(), Some(7.0));
    cli.close();
    server.close();
}

#[test]
fn heartbeat_timeout() {
    let ln = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = ln.local_addr().unwrap();
    thread::spawn(move || {
        let (conn, _) = ln.accept().unwrap();
        std::mem::forget(conn); // 保持连接但不回任何数据
    });
    let conn = TcpStream::connect(addr).unwrap();
    let (r, w) = boxed_stream(&conn);
    let ms = MessageStream::new(r, w);
    let cli = RpcProtocol::new_with_options(ms.clone(), Duration::from_millis(100), Duration::from_millis(300), None, None);
    cli.on_error(Box::new(|_| {}));
    let err = cli.send_request("add", Json::Arr(vec![Json::Num(1.0), Json::Num(2.0)])).unwrap_err();
    assert_eq!(err.code(), "EHEARTBEAT");
    assert!(ms.is_closed(), "heartbeat timeout should close stream");
    cli.close();
    std::mem::forget(conn);
}

#[test]
fn resync_timeout() {
    let ln = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = ln.local_addr().unwrap();
    thread::spawn(move || {
        let (mut conn, _) = ln.accept().unwrap();
        let payload = b"hello";
        let mut frame = Vec::new();
        frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        frame.extend_from_slice(&0xDEADBEEFu32.to_le_bytes());
        frame.extend_from_slice(payload);
        frame.extend_from_slice(&ipcprotocol::message_stream::crc32(payload).to_le_bytes());
        let _ = conn.write_all(&frame); // 之后保持静默，不回 RESET_CONFIRM
        std::mem::forget(conn);
    });
    let conn = TcpStream::connect(addr).unwrap();
    let (r, w) = boxed_stream(&conn);
    let mut ms = MessageStream::new(r, w);
    ms.set_sync_timeout(Duration::from_millis(300));
    let cli = RpcProtocol::new_with_options(ms.clone(), Duration::ZERO, Duration::ZERO, None, None);
    let (code_tx, code_rx) = mpsc::channel::<String>();
    cli.on_error(Box::new(move |e| {
        let _ = code_tx.send(e.code);
    }));
    let err = cli.send_request("add", Json::Arr(vec![Json::Num(1.0), Json::Num(2.0)])).unwrap_err();
    assert!(err.code() == "LENGTH_CRC_FAILED" || err.code() == "ERESYNC_TIMEOUT", "got {}", err.code());
    loop {
        let code = code_rx.recv_timeout(Duration::from_secs(2)).expect("ERESYNC_TIMEOUT not emitted");
        if code == "ERESYNC_TIMEOUT" {
            break;
        }
    }
    assert!(ms.is_closed(), "resync timeout should close stream");
    cli.close();
    std::mem::forget(conn);
}
// ---- v1.1.3 自动握手（P2） ----

#[test]
fn auto_hello_first_request() {
    let (cli, server) = pair(Duration::ZERO, Duration::ZERO, Some("secret"), Some("secret"));
    // 不显式 hello，直接业务请求
    let v = cli.send_request("add", Json::Arr(vec![Json::Num(2.0), Json::Num(3.0)])).unwrap();
    assert_eq!(v.as_f64(), Some(5.0));
    assert!(cli.auth_sent());
    cli.close();
    server.close();
}

#[test]
fn auto_hello_concurrent() {
    let (cli, server) = pair(Duration::ZERO, Duration::ZERO, Some("secret"), Some("secret"));
    let mut handles = Vec::new();
    for i in 0..8u32 {
        let c = cli.clone();
        handles.push(std::thread::spawn(move || {
            c.send_request("add", Json::Arr(vec![Json::Num(i as f64), Json::Num(1.0)]))
        }));
    }
    for (i, h) in handles.into_iter().enumerate() {
        let v = h.join().unwrap().unwrap();
        assert_eq!(v.as_f64(), Some(i as f64 + 1.0));
    }
    cli.close();
    server.close();
}

#[test]
fn auto_hello_wrong_token() {
    let (cli, server) = pair(Duration::ZERO, Duration::ZERO, Some("wrong"), Some("secret"));
    let err = cli.send_request("add", Json::Arr(vec![Json::Num(1.0), Json::Num(1.0)])).unwrap_err();
    assert_eq!(err.code(), "EAUTH");
    cli.close();
    server.close();
}
