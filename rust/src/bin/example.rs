//! Rust 示例：server/client 双模式，用于与其他语言实现互操作。
//!
//! cargo run --offline --bin example -- server 9008
//! cargo run --offline --bin example -- client <port>

use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::Duration;

use ipcprotocol::json::Json;
use ipcprotocol::message_stream::MessageStream;
use ipcprotocol::rpc_protocol::{RpcError, RpcProtocol};

fn boxed_stream(s: &TcpStream) -> (Box<dyn std::io::Read + Send>, Box<dyn std::io::Write + Send>) {
    (Box::new(s.try_clone().unwrap()), Box::new(s.try_clone().unwrap()))
}

fn register_methods(rpc: &Arc<RpcProtocol>) {
    rpc.register_method("add", Box::new(|a| {
        Ok(Json::Num(a[0].as_f64().unwrap() + a[1].as_f64().unwrap()))
    }));
    rpc.register_method("echo", Box::new(|a| Ok(a[0].clone())));
    rpc.register_method("greet", Box::new(|a| {
        Ok(Json::Str(format!("hello {}", a[0].as_str().unwrap())))
    }));
    rpc.register_method("blob", Box::new(|_| Ok(Json::Bytes(vec![0, 1, 2, 255]))));
    rpc.register_method("nothing", Box::new(|_| Ok(Json::Null)));
    rpc.register_method("fail", Box::new(|_| Err("boom".into())));
    rpc.register_method("upload", Box::new(|a| {
        let data = a[0].as_bytes().unwrap();
        let mut m = Json::obj();
        m.insert("size", Json::Num(data.len() as f64));
        m.insert("tag", a[1].clone());
        m.insert("head", Json::Arr(data[..4].iter().map(|b| Json::Num(*b as f64)).collect()));
        Ok(m)
    }));
    {
        let rpc2 = rpc.clone();
        rpc.register_method("pushEvents", Box::new(move |_| {
            let mut v = Json::obj();
            v.insert("push", Json::Num(1.0));
            let _ = rpc2.send_json(&v);
            let _ = rpc2.send_text("srv-text");
            let _ = rpc2.send_binary(&[1, 2]);
            Ok(Json::Str("sent".into()))
        }));
    }
}

fn run_server(port: u16) {
    let ln = TcpListener::bind(("127.0.0.1", port)).unwrap();
    println!("{{\"port\":{}}}", ln.local_addr().unwrap().port());
    for conn in ln.incoming() {
        let conn = conn.unwrap();
        thread::spawn(move || {
            let (r, w) = boxed_stream(&conn);
            let rpc = RpcProtocol::new(MessageStream::new(r, w));
            rpc.on_error(Box::new(|_| {}));
            register_methods(&rpc);
            std::mem::forget(conn);
        });
    }
}

static PASSED: AtomicU32 = AtomicU32::new(0);

fn check(name: &str, ok: bool, detail: String) {
    if ok {
        PASSED.fetch_add(1, Ordering::SeqCst);
        println!("PASS {}", name);
    } else {
        println!("FAIL {}: {}", name, detail);
        std::process::exit(1);
    }
}

fn run_client(port: u16) {
    let conn = TcpStream::connect(("127.0.0.1", port)).unwrap();
    let (r, w) = boxed_stream(&conn);
    let rpc = RpcProtocol::new(MessageStream::new(r, w));
    rpc.on_error(Box::new(|_| {}));

    let v = rpc.send_request("add", Json::Arr(vec![Json::Num(3.0), Json::Num(4.0)])).unwrap();
    check("add", v.as_f64() == Some(7.0), format!("{:?}", v));

    let mut obj = Json::obj();
    obj.insert("name", Json::Str("测试".into()));
    obj.insert("list", Json::Arr(vec![Json::Num(1.0), Json::Num(2.5), Json::Bool(true), Json::Null]));
    let v = rpc.send_request("echo", obj).unwrap();
    check("echo",
        v.get("name").and_then(|x| x.as_str()) == Some("测试")
            && v.get("list").and_then(|x| x.as_arr()).map(|a| a[1].as_f64()) == Some(Some(2.5)),
        format!("{:?}", v));

    let v = rpc.send_request("greet", Json::Str("世界".into())).unwrap();
    check("greet", v.as_str() == Some("hello 世界"), format!("{:?}", v));

    let v = rpc.send_request("blob", Json::Null).unwrap();
    check("blob", v.as_bytes() == Some(&[0u8, 1, 2, 255][..]), format!("{:?}", v));

    let v = rpc.send_request("nothing", Json::Null).unwrap();
    check("nothing", v == Json::Null, format!("{:?}", v));

    let err = rpc.send_request("fail", Json::Null).unwrap_err();
    check("fail", matches!(&err, RpcError::Remote(v) if v.as_str() == Some("boom")), format!("{:?}", err));

    let err = rpc.send_request("nope", Json::Null).unwrap_err();
    check("methodNotFound",
        matches!(&err, RpcError::Remote(v) if v.as_str() == Some("Method not found: nope")),
        format!("{:?}", err));

    let data: Vec<u8> = (0..256 * 300).map(|i| (i & 0xFF) as u8).collect();
    let v = rpc.send_request_binary("upload", &data, vec![Json::Str("tag-1".into())]).unwrap();
    check("upload",
        v.get("size").and_then(|x| x.as_f64()) == Some(data.len() as f64)
            && v.get("tag").and_then(|x| x.as_str()) == Some("tag-1")
            && v.get("head").and_then(|x| x.as_arr()).map(|a| a[3].as_f64()) == Some(Some(3.0)),
        format!("{:?}", v));

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
    let _ = rx.recv_timeout(Duration::from_secs(3));
    let ev = events.lock().unwrap();
    check("events",
        ev.len() == 3
            && ev[0].get("push").and_then(|x| x.as_f64()) == Some(1.0)
            && ev[1].as_str() == Some("srv-text")
            && ev[2].as_bytes() == Some(&[1u8, 2][..]),
        format!("{:?}", ev));
    drop(ev);

    let mut handles = Vec::new();
    for i in 0..50 {
        let rpc = rpc.clone();
        handles.push(thread::spawn(move || {
            let v = rpc.send_request("add", Json::Arr(vec![Json::Num(i as f64), Json::Num(i as f64)])).unwrap();
            v.as_f64() == Some((i * 2) as f64)
        }));
    }
    let all_ok = handles.into_iter().all(|h| h.join().unwrap());
    check("concurrent50", all_ok, "concurrent mismatch".into());

    let v = rpc.send_request_to("echo", Json::Str("routed".into()), Some("some-target")).unwrap();
    check("toField", v.as_str() == Some("routed"), format!("{:?}", v));

    println!("ALL {} CHECKS PASSED", PASSED.load(Ordering::SeqCst));
    rpc.close();
    let _ = conn.shutdown(std::net::Shutdown::Both);
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("usage: example server|client <port>");
        std::process::exit(2);
    }
    let port: u16 = args[2].parse().unwrap();
    match args[1].as_str() {
        "server" => run_server(port),
        "client" => run_client(port),
        _ => std::process::exit(2),
    }
}