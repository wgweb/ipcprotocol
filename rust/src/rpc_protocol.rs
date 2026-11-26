//! RpcProtocol 消息层：请求/响应/事件，与 RpcProtocol.js 线格式兼容。

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex, Once, Weak};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::json::{self, Json};
use crate::message_stream::{MessageStream, StreamError};

pub const TYPE_REQUEST: u8 = 0;
pub const TYPE_RESPONSE: u8 = 1;
pub const TYPE_EVENT: u8 = 2;
pub const TYPE_PING: u8 = 3;
pub const TYPE_PONG: u8 = 4;
pub const TYPE_HELLO: u8 = 5;
pub const TYPE_HELLO_ACK: u8 = 6;

pub const CONTENT_TEXT: u8 = 0;
pub const CONTENT_JSON: u8 = 1;
pub const CONTENT_BINARY: u8 = 2;

/// 线协议版本（HELLO 协商用）。
pub const PROTOCOL_VERSION: f64 = 1.0;

/// 服务端方法处理器：args 为展开后的参数；Err(String) 转为 {"error": ...} 响应。
pub type Handler = Box<dyn Fn(Vec<Json>) -> Result<Json, String> + Send + Sync>;

/// RPC 错误：Remote 携带对端原始 error 值（对齐 JS reject(response.error)）；
/// Timeout 为本地超时（code=ETIMEDOUT）；Coded 携带显式错误码（EAUTH/EVERSION/EHEARTBEAT/流错误码）。
#[derive(Debug, Clone)]
pub enum RpcError {
    Remote(Json),
    Message(String),
    Timeout(String),
    Coded { message: String, code: String },
}

impl RpcError {
    /// 错误码，对齐 JS 修复版 RpcError.code。
    pub fn code(&self) -> String {
        match self {
            RpcError::Remote(_) => "EREMOTE".to_string(),
            RpcError::Timeout(_) => "ETIMEDOUT".to_string(),
            RpcError::Message(_) => String::new(),
            RpcError::Coded { code, .. } => code.clone(),
        }
    }
}

impl fmt::Display for RpcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RpcError::Remote(v) => write!(f, "rpc error: {}", json::stringify(v)),
            RpcError::Message(m) | RpcError::Timeout(m) => write!(f, "{}", m),
            RpcError::Coded { message, .. } => write!(f, "{}", message),
        }
    }
}

impl std::error::Error for RpcError {}

type EventCb = Box<dyn Fn(u8, Json) + Send>;
type PendingMap = HashMap<String, mpsc::Sender<Result<Json, RpcError>>>;

pub struct RpcProtocol {
    ms: MessageStream,
    handlers: Arc<Mutex<HashMap<String, Arc<Handler>>>>,
    pending: Arc<Mutex<PendingMap>>,
    on_event: Arc<Mutex<Option<EventCb>>>,
    on_error: Arc<Mutex<Option<Box<dyn Fn(StreamError) + Send>>>>,
    /// 默认 RPC 超时（毫秒），对齐 JS 修复版 15s；0 表示关闭。
    default_timeout_ms: AtomicU64,
    // ---- v1.1: 心跳与 HELLO 握手（PROTOCOL.md §8.1/§8.2） ----
    last_received: Arc<AtomicU64>,
    authenticated: Arc<AtomicBool>,
    auth_token: Option<String>,
    // ---- v1.1.2: 能力协商（PROTOCOL.md §8.4） ----
    capabilities: Vec<String>,
    impl_name: String,
    peer_capabilities: Mutex<Option<Vec<String>>>,
    server_capabilities: Mutex<Option<Vec<String>>>,
    heartbeat_disabled: Arc<AtomicBool>,
    heartbeat_reason: Mutex<Option<String>>,
    unauthorized_hits: AtomicU32,
    // ---- v1.1.3: 首个业务请求前自动握手（P2） ----
    needs_auth: bool,
    auth_sent: AtomicBool,
    auth_lock: Mutex<()>,
    close_once: Once,
}

/// v1.1.2 默认能力集合。
pub const DEFAULT_CAPABILITIES: [&str; 3] = ["ping", "binary", "event"];

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

impl RpcProtocol {
    /// 包装消息流（默认心跳 5s/15s，不鉴权）：先注册回调再启动读循环，避免早到消息丢失。
    pub fn new(ms: MessageStream) -> Arc<Self> {
        Self::new_with_options(ms, Duration::from_secs(5), Duration::from_secs(15), None, None)
    }

    /// 带心跳与鉴权配置的构造。
    /// heartbeat_interval 为零关闭心跳；required_token 非 None 时未通过 HELLO 鉴权的请求被拒绝。
    pub fn new_with_options(
        ms: MessageStream,
        heartbeat_interval: Duration,
        heartbeat_timeout: Duration,
        auth_token: Option<String>,
        required_token: Option<String>,
    ) -> Arc<Self> {
        // v1.1.2：未显式传 token 时回退环境变量 IPC_TOKEN（Node 主进程注入）
        let auth_token = auth_token
            .filter(|t| !t.is_empty())
            .or_else(|| std::env::var("IPC_TOKEN").ok().filter(|t| !t.is_empty()));
        // v1.1.3：区分服务端校验位与客户端握手完成，避免客户端把自己当作已鉴权而跳过 HELLO
        let needs_auth = required_token.is_some();
        let initially_authenticated = required_token.is_none() && auth_token.is_none();
        let rpc = Arc::new(RpcProtocol {
            ms: ms.clone(),
            handlers: Arc::new(Mutex::new(HashMap::new())),
            pending: Arc::new(Mutex::new(HashMap::new())),
            on_event: Arc::new(Mutex::new(None)),
            on_error: Arc::new(Mutex::new(None)),
            default_timeout_ms: AtomicU64::new(15000),
            last_received: Arc::new(AtomicU64::new(now_ms())),
            authenticated: Arc::new(AtomicBool::new(initially_authenticated)),
            auth_token,
            needs_auth,
            auth_sent: AtomicBool::new(false),
            auth_lock: Mutex::new(()),
            capabilities: DEFAULT_CAPABILITIES.iter().map(|s| s.to_string()).collect(),
            impl_name: "rust".to_string(),
            peer_capabilities: Mutex::new(None),
            server_capabilities: Mutex::new(None),
            heartbeat_disabled: Arc::new(AtomicBool::new(false)),
            heartbeat_reason: Mutex::new(None),
            unauthorized_hits: AtomicU32::new(0),
            close_once: Once::new(),
        });

        {
            let handlers = rpc.handlers.clone();
            let pending = rpc.pending.clone();
            let on_event = rpc.on_event.clone();
            let authenticated = rpc.authenticated.clone();
            let last_received = rpc.last_received.clone();
            let required = required_token.clone();
            let weak = Arc::downgrade(&rpc);
            let ms2 = ms.clone();
            ms.on_message(Box::new(move |data| {
                handle_message(&data, &handlers, &pending, &on_event, &ms2, &authenticated, &required, &last_received, &weak);
            }));
        }
        {
            let pending = rpc.pending.clone();
            let on_error = rpc.on_error.clone();
            ms.on_error(Box::new(move |e: StreamError| {
                reject_all(&pending, RpcError::Coded {
                    message: format!("Request failed: {}", e.message),
                    code: e.code.clone(),
                });
                let cb = on_error.lock().unwrap();
                if let Some(f) = &*cb {
                    f(e);
                }
            }));
        }
        ms.start();

        let interval_ms = heartbeat_interval.as_millis() as u64;
        let timeout_ms = heartbeat_timeout.as_millis() as u64;
        if interval_ms > 0 {
            let weak = Arc::downgrade(&rpc);
            thread::spawn(move || loop {
                thread::sleep(Duration::from_millis(interval_ms));
                let rpc = match weak.upgrade() {
                    Some(r) => r,
                    None => return,
                };
                if rpc.ms.is_closed() {
                    return;
                }
                if rpc.heartbeat_disabled.load(Ordering::SeqCst) {
                    return;
                }
                let silent = now_ms().saturating_sub(rpc.last_received.load(Ordering::SeqCst));
                if timeout_ms > 0 && silent >= timeout_ms {
                    let msg = format!("heartbeat timeout: no data for {}ms", silent);
                    reject_all(&rpc.pending, RpcError::Coded { message: msg.clone(), code: "EHEARTBEAT".into() });
                    let cb = rpc.on_error.lock().unwrap();
                    if let Some(f) = &*cb {
                        f(StreamError { message: msg, code: "EHEARTBEAT".into() });
                    }
                    drop(cb);
                    rpc.close();
                    return;
                }
                if silent >= interval_ms {
                    let _ = rpc.ms.send(&build_message(TYPE_PING, CONTENT_TEXT, &new_uuid(), b""));
                }
            });
        }
        rpc
    }

    pub fn register_method(&self, name: &str, h: Handler) {
        self.handlers.lock().unwrap().insert(name.to_string(), Arc::new(h));
    }

    pub fn on_event(&self, f: EventCb) {
        *self.on_event.lock().unwrap() = Some(f);
    }

    pub fn on_error(&self, f: Box<dyn Fn(StreamError) + Send>) {
        *self.on_error.lock().unwrap() = Some(f);
    }

    /// 设置默认 RPC 超时；Duration::ZERO 关闭。
    pub fn set_default_timeout(&self, timeout: Duration) {
        self.default_timeout_ms.store(timeout.as_millis() as u64, Ordering::SeqCst);
    }

    pub fn default_timeout(&self) -> Duration {
        Duration::from_millis(self.default_timeout_ms.load(Ordering::SeqCst))
    }

    /// 发送 JSON 请求并阻塞等待响应。
    pub fn send_request(&self, method: &str, params: Json) -> Result<Json, RpcError> {
        self.send_request_to(method, params, None)
    }

    /// 带可选 to 路由字段的请求（默认超时）。
    pub fn send_request_to(&self, method: &str, params: Json, to: Option<&str>) -> Result<Json, RpcError> {
        self.send_request_to_timeout(method, params, to, self.default_timeout())
    }

    /// 带显式超时的请求；Duration::ZERO 关闭超时。
    pub fn send_request_timeout(&self, method: &str, params: Json, timeout: Duration) -> Result<Json, RpcError> {
        self.send_request_to_timeout(method, params, None, timeout)
    }

    /// 带 to 路由与显式超时的请求；超时返回 RpcError::Timeout（code=ETIMEDOUT），对齐 JS 修复版。
    pub fn send_request_to_timeout(&self, method: &str, params: Json, to: Option<&str>, timeout: Duration) -> Result<Json, RpcError> {
        self.ensure_auth()?;
        let id = new_uuid();
        let (tx, rx) = mpsc::channel();
        self.pending.lock().unwrap().insert(id.clone(), tx);

        let mut payload = Json::obj();
        payload.insert("method", Json::Str(method.to_string()));
        payload.insert("params", params);
        if let Some(t) = to {
            payload.insert("to", Json::Str(t.to_string()));
        }
        let body = json::stringify(&payload);
        if let Err(e) = self.ms.send(&build_message(TYPE_REQUEST, CONTENT_JSON, &id, body.as_bytes())) {
            self.pending.lock().unwrap().remove(&id);
            return Err(RpcError::Message(e.to_string()));
        }
        self.wait_result(&id, rx, method, timeout)
    }

    /// 发送二进制请求（默认超时）：handler 实参为 [Bytes(data), ...args]。
    pub fn send_request_binary(&self, method: &str, data: &[u8], args: Vec<Json>) -> Result<Json, RpcError> {
        self.send_request_binary_timeout(method, data, args, self.default_timeout())
    }

    /// 带显式超时的二进制请求；Duration::ZERO 关闭超时。
    pub fn send_request_binary_timeout(&self, method: &str, data: &[u8], args: Vec<Json>, timeout: Duration) -> Result<Json, RpcError> {
        self.ensure_auth()?;
        let id = new_uuid();
        let (tx, rx) = mpsc::channel();
        self.pending.lock().unwrap().insert(id.clone(), tx);

        let body = encode_binary_request(method, data, &args);
        if let Err(e) = self.ms.send(&build_message(TYPE_REQUEST, CONTENT_BINARY, &id, &body)) {
            self.pending.lock().unwrap().remove(&id);
            return Err(RpcError::Message(e.to_string()));
        }
        self.wait_result(&id, rx, method, timeout)
    }

    /// 等待响应；超时先移除 pending 再返回 Timeout，避免迟到响应写入已废弃的 channel。
    fn wait_result(&self, id: &str, rx: mpsc::Receiver<Result<Json, RpcError>>, method: &str, timeout: Duration) -> Result<Json, RpcError> {
        if timeout.is_zero() {
            return rx.recv().map_err(|_| RpcError::Message("request channel closed".into()))?;
        }
        match rx.recv_timeout(timeout) {
            Ok(r) => r,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                self.pending.lock().unwrap().remove(id);
                Err(RpcError::Timeout(format!("RPC timeout after {}ms: {}", timeout.as_millis(), method)))
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => Err(RpcError::Message("request channel closed".into())),
        }
    }

    /// 发送单向事件。
    pub fn send_text(&self, text: &str) -> Result<(), RpcError> {
        self.ms
            .send(&build_message(TYPE_EVENT, CONTENT_TEXT, &new_uuid(), text.as_bytes()))
            .map_err(|e| RpcError::Message(e.to_string()))
    }

    pub fn send_json(&self, value: &Json) -> Result<(), RpcError> {
        let body = json::stringify(value);
        self.ms
            .send(&build_message(TYPE_EVENT, CONTENT_JSON, &new_uuid(), body.as_bytes()))
            .map_err(|e| RpcError::Message(e.to_string()))
    }

    pub fn send_binary(&self, data: &[u8]) -> Result<(), RpcError> {
        self.ms
            .send(&build_message(TYPE_EVENT, CONTENT_BINARY, &new_uuid(), data))
            .map_err(|e| RpcError::Message(e.to_string()))
    }

    /// 销毁：拒绝所有 pending 并关闭流。
    pub fn close(&self) {
        self.close_once.call_once(|| {
            reject_all(&self.pending, RpcError::Message("RPC Protocol destroyed".into()));
            self.ms.close();
        });
    }

    // ---- v1.1: HELLO 握手 ----

    /// 发起 HELLO 握手（默认超时）；ACK error → EAUTH，版本不符 → EVERSION。
    pub fn hello(&self) -> Result<Json, RpcError> {
        self.hello_timeout(self.default_timeout())
    }

    /// 带显式超时的 HELLO 握手；Duration::ZERO 关闭超时。
    pub fn hello_timeout(&self, timeout: Duration) -> Result<Json, RpcError> {
        let id = new_uuid();
        let (tx, rx) = mpsc::channel();
        self.pending.lock().unwrap().insert(id.clone(), tx);

        let mut payload = Json::obj();
        payload.insert("version", Json::Num(PROTOCOL_VERSION));
        payload.insert("caps", Json::Arr(self.capabilities.iter().map(|s| Json::Str(s.clone())).collect()));
        payload.insert("impl", Json::Str(self.impl_name.clone()));
        if let Some(token) = &self.auth_token {
            payload.insert("token", Json::Str(token.clone()));
        }
        let body = json::stringify(&payload);
        if let Err(e) = self.ms.send(&build_message(TYPE_HELLO, CONTENT_JSON, &id, body.as_bytes())) {
            self.pending.lock().unwrap().remove(&id);
            return Err(RpcError::Message(e.to_string()));
        }
        self.wait_result(&id, rx, "hello", timeout)
    }

    // ---- v1.1.3: 自动握手（P2） ----

    /// 首个业务请求前自动 HELLO：仅客户端（needs_auth == false 且持有 token）生效；
    /// 并发调用由 auth_lock 串行化，只握手一次。
    fn ensure_auth(&self) -> Result<(), RpcError> {
        if self.needs_auth || self.auth_token.is_none() {
            return Ok(());
        }
        if self.authenticated.load(Ordering::SeqCst) || self.auth_sent.load(Ordering::SeqCst) {
            return Ok(());
        }
        let _guard = self.auth_lock.lock().unwrap();
        if self.authenticated.load(Ordering::SeqCst) || self.auth_sent.load(Ordering::SeqCst) {
            return Ok(());
        }
        self.hello()?;
        self.auth_sent.store(true, Ordering::SeqCst);
        Ok(())
    }

    /// 是否已完成（或被跳过）自动握手。
    pub fn auth_sent(&self) -> bool {
        self.auth_sent.load(Ordering::SeqCst) || self.authenticated.load(Ordering::SeqCst)
    }

    // ---- v1.1.2: 能力协商（PROTOCOL.md §8.4） ----

    /// 本端声明的能力。
    pub fn capabilities(&self) -> Vec<String> { self.capabilities.clone() }

    /// 对端在 HELLO 中声明的能力（None = 旧实现）。
    pub fn peer_capabilities(&self) -> Option<Vec<String>> { self.peer_capabilities.lock().unwrap().clone() }

    /// 客户端收到 HELLO_ACK 里服务端声明的能力。
    pub fn server_capabilities(&self) -> Option<Vec<String>> { self.server_capabilities.lock().unwrap().clone() }

    /// 因能力协商停用心跳的原因（None = 心跳正常）。
    pub fn heartbeat_disabled_reason(&self) -> Option<String> { self.heartbeat_reason.lock().unwrap().clone() }

    pub fn set_impl_name(&mut self, name: &str) { self.impl_name = name.to_string(); }

    fn disable_heartbeat(&self, reason: &str) {
        if self.heartbeat_disabled.swap(true, Ordering::SeqCst) {
            return;
        }
        *self.heartbeat_reason.lock().unwrap() = Some(reason.to_string());
    }
}

fn reject_all(pending: &Arc<Mutex<PendingMap>>, err: RpcError) {
    let mut map = pending.lock().unwrap();
    for (_, tx) in map.drain() {
        let _ = tx.send(Err(err.clone()));
    }
}

fn handle_message(
    data: &[u8],
    handlers: &Arc<Mutex<HashMap<String, Arc<Handler>>>>,
    pending: &Arc<Mutex<PendingMap>>,
    on_event: &Arc<Mutex<Option<EventCb>>>,
    ms: &MessageStream,
    authenticated: &AtomicBool,
    required_token: &Option<String>,
    last_received: &AtomicU64,
    rpc_weak: &Weak<RpcProtocol>,
) {
    if data.len() < 18 {
        eprintln!("[ipc] message too short: {}", data.len());
        return;
    }
    let msg_type = data[0];
    let content_type = data[1];
    if msg_type > 6 {
        eprintln!("[ipc] unknown message type: {}", msg_type);
        return;
    }
    let id = bytes_to_uuid(&data[2..18]);
    let payload = &data[18..];
    last_received.store(now_ms(), Ordering::SeqCst); // 任何入站消息都刷新心跳基准

    let content = match deserialize(content_type, payload) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("[ipc] deserialize failed: {}", e);
            // 畸形 REQUEST 尽量回错误响应，避免调用方永久等待（对齐 JS 修复版）
            if msg_type == TYPE_REQUEST {
                send_error_response(ms, &id, "Invalid request payload");
            }
            return;
        }
    };

    match msg_type {
        TYPE_REQUEST => {
            if required_token.is_some() && !authenticated.load(Ordering::SeqCst) {
                send_error_response(ms, &id, "Unauthorized");
                // v1.1.2：回错误但不断链（允许随后 HELLO 重试）；连续 5 次才断开
                if let Some(rpc) = rpc_weak.upgrade() {
                    if rpc.unauthorized_hits.fetch_add(1, Ordering::SeqCst) >= 4 {
                        rpc.close();
                    }
                }
                return;
            }
            let handlers = handlers.clone();
            let ms = ms.clone();
            let raw = payload.to_vec();
            thread::spawn(move || {
                handle_request(&id, content_type, content, &raw, &handlers, &ms);
            });
        }
        TYPE_RESPONSE => {
            let tx = pending.lock().unwrap().remove(&id);
            if let Some(tx) = tx {
                if let Some(ev) = content.get("error") {
                    if ev.is_truthy() {
                        let _ = tx.send(Err(RpcError::Remote(ev.clone())));
                        return;
                    }
                }
                let _ = tx.send(Ok(content));
            }
        }
        TYPE_EVENT => {
            if required_token.is_some() && !authenticated.load(Ordering::SeqCst) {
                return; // 未鉴权事件直接丢弃
            }
            let cb = on_event.lock().unwrap();
            if let Some(f) = &*cb {
                f(content_type, content);
            }
        }
        TYPE_PING => {
            let _ = ms.send(&build_message(TYPE_PONG, CONTENT_TEXT, &id, b""));
        }
        TYPE_PONG => {
            // 仅刷新 last_received（上面已做）
        }
        TYPE_HELLO => handle_hello(ms, &id, &content, authenticated, required_token, rpc_weak),
        TYPE_HELLO_ACK => {
            let tx = pending.lock().unwrap().remove(&id);
            let parsed = parse_hello_ack(&content);
            if let Some(tx) = tx {
                if parsed.is_ok() {
                    if let Some(rpc) = rpc_weak.upgrade() {
                        let caps = read_caps(&content);
                        *rpc.server_capabilities.lock().unwrap() = caps.clone();
                        if let Some(list) = caps {
                            if !list.iter().any(|s| s == "ping") {
                                rpc.disable_heartbeat("server declares no 'ping' capability");
                            }
                        }
                    }
                }
                let _ = tx.send(parsed);
            }
        }
        _ => eprintln!("[ipc] unknown message type: {}", msg_type),
    }
}

/// 服务端 HELLO 处理：版本/token 校验，失败回 error ACK 后断链。
fn handle_hello(
    ms: &MessageStream,
    id: &str,
    content: &Json,
    authenticated: &AtomicBool,
    required_token: &Option<String>,
    rpc_weak: &Weak<RpcProtocol>,
) {
    let mut ack = Json::obj();
    ack.insert("version", Json::Num(PROTOCOL_VERSION));
    let mut ok = false;
    if matches!(content, Json::Obj(_)) {
        let version = content.get("version").and_then(|v| v.as_f64()).unwrap_or(0.0);
        if version != PROTOCOL_VERSION {
            ack.insert("error", Json::Str("Unsupported protocol version".into()));
        } else if let Some(required) = required_token {
            let token = content.get("token").and_then(|t| t.as_str()).unwrap_or("");
            if token != required {
                ack.insert("error", Json::Str("Unauthorized".into()));
            } else {
                authenticated.store(true, Ordering::SeqCst);
                ack.insert("ok", Json::Bool(true));
                ok = true;
            }
        } else {
            authenticated.store(true, Ordering::SeqCst);
            ack.insert("ok", Json::Bool(true));
            if let Some(rpc) = rpc_weak.upgrade() {
                ack.insert("caps", Json::Arr(rpc.capabilities.iter().map(|s| Json::Str(s.clone())).collect()));
                ack.insert("impl", Json::Str(rpc.impl_name.clone()));
                // v1.1.2：记录对端能力；对端不支持 PING 则本端停用心跳
                let peer = read_caps(content);
                if let Some(list) = &peer {
                    if !list.iter().any(|s| s == "ping") {
                        let impl_name = content.get("impl").and_then(|v| v.as_str()).unwrap_or("unknown").to_string();
                        rpc.disable_heartbeat(&format!("peer {} declares no 'ping' capability", impl_name));
                    }
                }
                *rpc.peer_capabilities.lock().unwrap() = peer;
            }
            ok = true;
        }
    } else {
        ack.insert("error", Json::Str("Invalid hello payload".into()));
    }
    let body = json::stringify(&ack).into_bytes();
    if let Err(e) = ms.send(&build_message(TYPE_HELLO_ACK, CONTENT_JSON, id, &body)) {
        eprintln!("[ipc] send hello ack failed: {}", e);
    }
    if !ok {
        // v1.1.2：先让 ack 刷到对端再断链（立即 close 会丢掉未送达的 ack）
        if let Some(rpc) = rpc_weak.upgrade() {
            thread::spawn(move || {
                thread::sleep(Duration::from_millis(300));
                rpc.close();
            });
        }
    }
}

/// 解析 HELLO / HELLO_ACK 中的 caps 字段；缺失返回 None（旧实现）。
fn read_caps(content: &Json) -> Option<Vec<String>> {
    let arr = content.get("caps")?.as_arr()?.clone();
    let mut out = Vec::new();
    for item in arr {
        if let Some(s) = item.as_str() {
            out.push(s.to_string());
        }
    }
    Some(out)
}

/// 客户端 HELLO_ACK 解析：error → EAUTH，版本不符 → EVERSION。
fn parse_hello_ack(content: &Json) -> Result<Json, RpcError> {
    if !matches!(content, Json::Obj(_)) {
        return Err(RpcError::Coded { message: "Invalid hello ack payload".into(), code: "EAUTH".into() });
    }
    if let Some(ev) = content.get("error") {
        if ev.is_truthy() {
            let msg = ev.as_str().map(|s| s.to_string()).unwrap_or_else(|| json::stringify(ev));
            return Err(RpcError::Coded { message: msg, code: "EAUTH".into() });
        }
    }
    let version = content.get("version").and_then(|v| v.as_f64()).unwrap_or(0.0);
    if version != PROTOCOL_VERSION {
        return Err(RpcError::Coded { message: "Unsupported protocol version".into(), code: "EVERSION".into() });
    }
    Ok(content.clone())
}

fn send_error_response(ms: &MessageStream, id: &str, msg: &str) {
    let mut e = Json::obj();
    e.insert("error", Json::Str(msg.to_string()));
    let body = json::stringify(&e).into_bytes();
    if let Err(err) = ms.send(&build_message(TYPE_RESPONSE, CONTENT_JSON, id, &body)) {
        eprintln!("[ipc] send error response failed: {}", err);
    }
}

fn handle_request(
    id: &str,
    content_type: u8,
    content: Json,
    raw: &[u8],
    handlers: &Arc<Mutex<HashMap<String, Arc<Handler>>>>,
    ms: &MessageStream,
) {
    let (method, args) = if content_type == CONTENT_BINARY {
        match decode_binary_request(raw) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("[ipc] bad binary request: {}", e);
                send_error_response(ms, id, &format!("Invalid request payload: {}", e));
                return;
            }
        }
    } else {
        if !matches!(content, Json::Obj(_)) {
            send_error_response(ms, id, "Invalid request payload: not an object");
            return;
        }
        let method = content.get("method").and_then(|m| m.as_str()).unwrap_or("").to_string();
        let args = expand_params(content.get("params").cloned().unwrap_or(Json::Null));
        (method, args)
    };
    if method.is_empty() {
        send_error_response(ms, id, "Invalid request payload: method missing");
        return;
    }

    // 取出处理器引用后立即释放锁，避免长时间持锁阻塞其他请求
    let handler = handlers.lock().unwrap().get(&method).cloned();
    let result = match handler {
        Some(h) => h(args),
        None => Err(format!("Method not found: {}", method)),
    };

    let (resp_type, resp_body) = match result {
        Err(msg) => {
            let mut e = Json::obj();
            e.insert("error", Json::Str(msg));
            (CONTENT_JSON, json::stringify(&e).into_bytes())
        }
        Ok(v) => serialize_response(&v),
    };
    if let Err(e) = ms.send(&build_message(TYPE_RESPONSE, resp_type, id, &resp_body)) {
        eprintln!("[ipc] send response failed: {}", e);
    }
}

// ---- 序列化辅助 ----

fn deserialize(content_type: u8, payload: &[u8]) -> Result<Json, String> {
    match content_type {
        CONTENT_TEXT => Ok(Json::Str(String::from_utf8_lossy(payload).into_owned())),
        CONTENT_JSON => json::parse(&String::from_utf8_lossy(payload)),
        CONTENT_BINARY => Ok(Json::Bytes(payload.to_vec())),
        _ => Err(format!("unknown content type: {}", content_type)),
    }
}

fn serialize_response(v: &Json) -> (u8, Vec<u8>) {
    match v {
        Json::Null => (CONTENT_JSON, b"null".to_vec()),
        Json::Str(s) => (CONTENT_TEXT, s.as_bytes().to_vec()),
        Json::Bytes(b) => (CONTENT_BINARY, b.clone()),
        other => (CONTENT_JSON, json::stringify(other).into_bytes()),
    }
}

fn expand_params(params: Json) -> Vec<Json> {
    match params {
        Json::Null => Vec::new(),
        Json::Arr(a) => a,
        other => vec![other],
    }
}

fn build_message(msg_type: u8, content_type: u8, id: &str, payload: &[u8]) -> Vec<u8> {
    let mut msg = vec![0u8; 18 + payload.len()];
    msg[0] = msg_type;
    msg[1] = content_type;
    msg[2..18].copy_from_slice(&uuid_to_bytes(id));
    msg[18..].copy_from_slice(payload);
    msg
}

/// [methodLen u32BE][method][paramsLen u32BE][paramsJSON][binary]
fn encode_binary_request(method: &str, data: &[u8], args: &[Json]) -> Vec<u8> {
    let method_bytes = method.as_bytes();
    let params_json = json::stringify(&Json::Arr(args.to_vec()));
    let params_bytes = params_json.as_bytes();
    let mut out = Vec::with_capacity(8 + method_bytes.len() + params_bytes.len() + data.len());
    out.extend_from_slice(&(method_bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(method_bytes);
    out.extend_from_slice(&(params_bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(params_bytes);
    out.extend_from_slice(data);
    out
}

fn decode_binary_request(raw: &[u8]) -> Result<(String, Vec<Json>), String> {
    if raw.len() < 4 {
        return Err("binary request too short".into());
    }
    let method_len = u32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]) as usize;
    if raw.len() < 4 + method_len + 4 {
        return Err("binary request truncated".into());
    }
    let method = String::from_utf8_lossy(&raw[4..4 + method_len]).into_owned();
    let off = 4 + method_len;
    let params_len = u32::from_be_bytes([raw[off], raw[off + 1], raw[off + 2], raw[off + 3]]) as usize;
    if raw.len() < off + 4 + params_len {
        return Err("binary request params truncated".into());
    }
    let params = json::parse(&String::from_utf8_lossy(&raw[off + 4..off + 4 + params_len]))?;
    let mut args = vec![Json::Bytes(raw[off + 4 + params_len..].to_vec())];
    if let Json::Arr(a) = params {
        args.extend(a);
    }
    Ok((method, args))
}

// ---- UUID（RFC 4122 大端字节序；无 rand 依赖，用 xorshift PRNG 保证唯一性）----

static UUID_STATE: AtomicU64 = AtomicU64::new(0);
static UUID_INIT: Once = Once::new();

fn next_rand() -> u64 {
    UUID_INIT.call_once(|| {
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(0);
        let seed = nanos ^ (&UUID_STATE as *const _ as u64) ^ (std::process::id() as u64) << 32;
        UUID_STATE.store(seed | 1, Ordering::SeqCst);
    });
    let mut s = UUID_STATE.load(Ordering::SeqCst);
    s ^= s << 13;
    s ^= s >> 7;
    s ^= s << 17;
    UUID_STATE.store(s, Ordering::SeqCst);
    s
}

fn new_uuid() -> String {
    let mut b = [0u8; 16];
    b[0..8].copy_from_slice(&next_rand().to_be_bytes());
    b[8..16].copy_from_slice(&next_rand().to_be_bytes());
    b[6] = (b[6] & 0x0f) | 0x40; // version 4
    b[8] = (b[8] & 0x3f) | 0x80; // variant
    bytes_to_uuid(&b)
}

fn uuid_to_bytes(id: &str) -> [u8; 16] {
    let hex: String = id.chars().filter(|c| *c != '-').collect();
    let mut b = [0u8; 16];
    for i in 0..16 {
        b[i] = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).unwrap_or(0);
    }
    b
}

fn bytes_to_uuid(b: &[u8]) -> String {
    let hex: String = b.iter().map(|x| format!("{:02x}", x)).collect();
    format!("{}-{}-{}-{}-{}", &hex[0..8], &hex[8..12], &hex[12..16], &hex[16..20], &hex[20..32])
}