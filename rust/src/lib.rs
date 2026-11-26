//! ipcprotocol：与 MessageStream.js / RpcProtocol.js 线格式兼容的跨语言 IPC/RPC 协议。
//!
//! - [`MessageStream`]：帧层（长度头 + CRC32 + 错误重同步）
//! - [`RpcProtocol`]：消息层（请求/响应/事件，JSON/TEXT/BINARY 载荷）
//!
//! 直接使用 MessageStream 时，先设置回调再调用 `start()`；
//! RpcProtocol::new 会自动完成这一步。

pub mod json;
pub mod message_stream;
pub mod rpc_protocol;

pub use json::Json;
pub use message_stream::MessageStream;
pub use rpc_protocol::{RpcError, RpcProtocol};