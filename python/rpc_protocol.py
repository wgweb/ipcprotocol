"""
RpcProtocol 的 Python 移植版（与 RpcProtocol.js 线格式完全兼容）。

消息格式:
    [type u8][contentType u8][UUID 16B][payload...]
    type:        0=REQUEST, 1=RESPONSE, 2=EVENT
    contentType: 0=TEXT(utf8), 1=JSON(utf8), 2=BINARY(原始字节)

JSON 请求体: {"method": ..., "params": ..., "to": ...(可选)}
错误响应体: {"error": ...}

二进制请求体（contentType=BINARY 的 REQUEST）:
    [methodLen u32BE][method utf8][paramsLen u32BE][params JSON utf8][binary...]
    服务端 handler 收到的参数为 [binary, *params]
"""

import asyncio
import inspect
import json
import os
import struct
import time
import uuid

from message_stream import MessageStream

TYPES = {'REQUEST': 0, 'RESPONSE': 1, 'EVENT': 2, 'PING': 3, 'PONG': 4, 'HELLO': 5, 'HELLO_ACK': 6}
CONTENT_TYPES = {'TEXT': 0, 'JSON': 1, 'BINARY': 2}

# v1.1.2 能力协商（PROTOCOL.md §8.4）
CAPABILITIES = ['ping', 'binary', 'event']
IMPL_NAME = 'python'

# 默认 RPC 超时（秒），对齐 JS 修复版 DEFAULT_TIMEOUT=15000ms；0 表示关闭
DEFAULT_TIMEOUT = 15.0
PROTOCOL_VERSION = 1


class RpcError(Exception):
    """对端返回的错误响应，error 属性携带原始 error 值；code 为错误码（如 ETIMEDOUT/EREMOTE）。"""

    def __init__(self, error, code=None):
        self.error = error
        self.code = code
        super().__init__(str(error))


def serialize(msg_type: int, content_type: int, msg_id: str, data) -> bytes:
    header = bytes([msg_type, content_type]) + uuid.UUID(msg_id).bytes
    if content_type == CONTENT_TYPES['TEXT']:
        payload = data.encode('utf8')
    elif content_type == CONTENT_TYPES['JSON']:
        payload = json.dumps(data, ensure_ascii=False, separators=(',', ':')).encode('utf8')
    elif content_type == CONTENT_TYPES['BINARY']:
        payload = bytes(data)
    else:
        raise ValueError(f'Unsupported content type: {content_type}')
    return header + payload


def deserialize(buffer: bytes):
    if len(buffer) < 18:
        raise ValueError('Failed to deserialize message: buffer too short')
    msg_type = buffer[0]
    content_type = buffer[1]
    msg_id = str(uuid.UUID(bytes=buffer[2:18]))
    payload = buffer[18:]
    if content_type == CONTENT_TYPES['TEXT']:
        content = payload.decode('utf8')
    elif content_type == CONTENT_TYPES['JSON']:
        content = json.loads(payload.decode('utf8'))
    elif content_type == CONTENT_TYPES['BINARY']:
        content = payload
    else:
        raise ValueError(f'Failed to deserialize message: unknown content type {content_type}')
    return {'type': msg_type, 'contentType': content_type, 'id': msg_id, 'content': content}


def encode_binary_request(method: str, params, binary_data: bytes) -> bytes:
    method_buf = method.encode('utf8')
    params_buf = json.dumps(params, ensure_ascii=False, separators=(',', ':')).encode('utf8')
    return (struct.pack('>I', len(method_buf)) + method_buf
            + struct.pack('>I', len(params_buf)) + params_buf
            + bytes(binary_data))


def decode_binary_request(buffer: bytes):
    offset = 0
    if len(buffer) < 4:
        raise ValueError('binary request too short')
    method_len = struct.unpack_from('>I', buffer, offset)[0]
    offset += 4
    if len(buffer) < offset + method_len + 4:
        raise ValueError('binary request truncated (method)')
    method = buffer[offset:offset + method_len].decode('utf8')
    offset += method_len
    params_len = struct.unpack_from('>I', buffer, offset)[0]
    offset += 4
    if len(buffer) < offset + params_len:
        raise ValueError('binary request truncated (params)')
    params = json.loads(buffer[offset:offset + params_len].decode('utf8'))
    offset += params_len
    return {'method': method, 'params': params, 'binaryData': buffer[offset:]}


class RpcProtocol:
    """请求/响应/事件 RPC 协议，语义对齐 RpcProtocol.js。

    事件（通过 on() 注册回调）:
        'event' -> callback({'contentType': int, 'content': Any})
        'error' -> callback(err)
    """

    TYPES = TYPES
    CONTENT_TYPES = CONTENT_TYPES

    def __init__(self, message_stream: MessageStream, default_timeout: float = DEFAULT_TIMEOUT,
                 heartbeat_interval: float = 5.0, heartbeat_timeout: float = 15.0,
                 auth_token: str = None, required_token: str = None,
                 capabilities=None, impl_name: str = IMPL_NAME,
                 heartbeat_require_capability: bool = False):
        if message_stream is None:
            raise ValueError('messageStream is undefined')
        self.message_stream = message_stream
        self.default_timeout = default_timeout
        self.handlers = {}
        self.pending_requests = {}
        self._listeners = {}
        self._destroyed = False

        # v1.1：心跳与可选 HELLO 握手（PROTOCOL.md §8.1/§8.2，单位秒）
        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_timeout = heartbeat_timeout
        # v1.1.2：默认鉴权 —— 未显式传 token 时从环境变量继承（Node 主进程注入）
        self.auth_token = auth_token or os.environ.get('IPC_TOKEN') or None
        self.required_token = required_token
        self._authenticated = required_token is None
        self._auth_sent = False          # 客户端本地状态：是否已完成过一次 HELLO
        self._auth_lock = asyncio.Lock()

        # v1.1.2：能力协商
        self.capabilities = list(capabilities) if capabilities else list(CAPABILITIES)
        self.impl_name = impl_name
        self.peer_capabilities = None
        self.server_capabilities = None
        self.heartbeat_disabled_reason = None
        self.heartbeat_require_capability = heartbeat_require_capability

        self._last_received = time.monotonic()
        self._heartbeat_task = None
        if heartbeat_interval > 0:
            self._heartbeat_task = asyncio.get_event_loop().create_task(self._heartbeat_loop())

        message_stream.on('message', self._handle_message)
        message_stream.on('error', self._on_stream_error)

    # ========== 事件 ==========
    def on(self, event, callback):
        self._listeners.setdefault(event, []).append(callback)

    def _emit(self, event, *args):
        for cb in list(self._listeners.get(event, [])):
            try:
                cb(*args)
            except Exception as err:
                print(f'[RpcProtocol] listener error: {err}')

    def on_event(self, listener):
        self.on('event', listener)

    def _on_stream_error(self, error):
        self._handle_stream_error(error)
        self._emit('error', error)

    def _handle_stream_error(self, error):
        message = str(error) or 'Connection error'
        code = getattr(error, 'code', None)  # 错误码透传（ERESYNC/ERESYNC_TIMEOUT/EHEARTBEAT 等）
        for pending in self.pending_requests.values():
            if not pending.done():
                pending.set_exception(RpcError(f'Request failed: {message}', code=code))
        self.pending_requests.clear()

    def _disable_heartbeat(self, reason: str):
        if self.heartbeat_disabled_reason:
            return
        self.heartbeat_disabled_reason = reason
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            self._heartbeat_task = None
        self._emit('heartbeatDisabled', reason)

    # ========== 心跳（§8.1） ==========
    async def _heartbeat_loop(self):
        try:
            while not self._destroyed:
                await asyncio.sleep(self.heartbeat_interval)
                if self._destroyed or self.heartbeat_disabled_reason:
                    return
                if self.heartbeat_require_capability and 'ping' not in (self.peer_capabilities or []):
                    continue
                silent = time.monotonic() - self._last_received
                if self.heartbeat_timeout > 0 and silent >= self.heartbeat_timeout:
                    err = RpcError(f'heartbeat timeout: no data for {int(silent * 1000)}ms',
                                   code='EHEARTBEAT')
                    for pending in self.pending_requests.values():
                        if not pending.done():
                            pending.set_exception(err)
                    self.pending_requests.clear()
                    self._emit('error', err)
                    await self.destroy()
                    return
                if silent >= self.heartbeat_interval:
                    try:
                        await self.send_raw(TYPES['PING'], CONTENT_TYPES['TEXT'], str(uuid.uuid4()), '')
                    except Exception:
                        pass
        except asyncio.CancelledError:
            pass

    # ========== HELLO 握手（§8.2） ==========
    async def _handle_hello(self, msg_id: str, content):
        version = content.get('version') if isinstance(content, dict) else None
        fail = False
        if version != PROTOCOL_VERSION:
            ack = {'error': f'unsupported protocol version: {version}'}
            fail = True
        elif self.required_token and (not isinstance(content, dict) or content.get('token') != self.required_token):
            ack = {'error': 'unauthorized'}
            fail = True
        else:
            self._authenticated = True
            caps = content.get('caps') if isinstance(content.get('caps'), list) else None
            self.peer_capabilities = caps
            if caps is not None and 'ping' not in caps:
                self._disable_heartbeat(f"peer {content.get('impl', 'unknown')} declares no 'ping' capability")
            ack = {'version': PROTOCOL_VERSION, 'ok': True,
                   'caps': self.capabilities, 'impl': self.impl_name}
        try:
            await self.send_raw(TYPES['HELLO_ACK'], CONTENT_TYPES['JSON'], msg_id, ack)
        except Exception:
            pass
        if fail:
            await self.destroy()

    def _handle_hello_ack(self, msg_id: str, content):
        pending = self.pending_requests.pop(msg_id, None)
        if pending is None or pending.done():
            return
        if isinstance(content, dict) and content.get('error'):
            pending.set_exception(RpcError(content['error'], code='EAUTH'))
        elif not isinstance(content, dict) or content.get('version') != PROTOCOL_VERSION:
            v = content.get('version') if isinstance(content, dict) else None
            pending.set_exception(RpcError(f'unsupported protocol version: {v}', code='EVERSION'))
        else:
            caps = content.get('caps') if isinstance(content.get('caps'), list) else None
            self.server_capabilities = caps
            if caps is not None and 'ping' not in caps:
                self._disable_heartbeat(f"server {content.get('impl', 'unknown')} declares no 'ping' capability")
            pending.set_result(content)

    async def hello(self, timeout=None):
        """显式握手：启用鉴权时调用方必须先 await hello() 再发业务请求；同时完成能力协商。"""
        msg_id = str(uuid.uuid4())
        future = asyncio.get_event_loop().create_future()
        self.pending_requests[msg_id] = future
        payload = {'version': PROTOCOL_VERSION, 'caps': self.capabilities, 'impl': self.impl_name}
        if self.auth_token:
            payload['token'] = self.auth_token
        try:
            await self.send_raw(TYPES['HELLO'], CONTENT_TYPES['JSON'], msg_id, payload)
        except Exception as err:
            self.pending_requests.pop(msg_id, None)
            if not future.done():
                future.set_exception(err)
        result = await self._wait_pending(msg_id, future, 'hello', timeout)
        self._auth_sent = True
        return result

    # ========== 接收处理 ==========
    def _handle_message(self, buffer: bytes):
        self._last_received = time.monotonic()  # 任何入站消息都刷新心跳活动时刻
        try:
            msg = deserialize(buffer)
            if msg['type'] not in TYPES.values():
                raise ValueError(f"Invalid message type: {msg['type']}")
            if msg['contentType'] not in CONTENT_TYPES.values():
                raise ValueError(f"Invalid content type: {msg['contentType']}")

            if msg['type'] == TYPES['REQUEST']:
                if self.required_token and not self._authenticated:
                    asyncio.get_event_loop().create_task(self._reject_unauthorized(msg['id']))
                    return
                asyncio.get_event_loop().create_task(self._handle_request(msg['id'], msg['content']))
            elif msg['type'] == TYPES['RESPONSE']:
                self._handle_response(msg['id'], msg['content'])
            elif msg['type'] == TYPES['EVENT']:
                if self.required_token and not self._authenticated:
                    return  # 未鉴权事件丢弃
                self._emit('event', {'contentType': msg['contentType'], 'content': msg['content']})
            elif msg['type'] == TYPES['PING']:
                asyncio.get_event_loop().create_task(self._send_pong(msg['id']))
            elif msg['type'] == TYPES['PONG']:
                pass  # 仅刷新 _last_received，不交付应用层
            elif msg['type'] == TYPES['HELLO']:
                asyncio.get_event_loop().create_task(self._handle_hello(msg['id'], msg['content']))
            elif msg['type'] == TYPES['HELLO_ACK']:
                self._handle_hello_ack(msg['id'], msg['content'])
        except Exception as err:
            print(f'Failed to handle message: {err}')
            # 畸形 REQUEST 尽量回错误响应，避免调用方永久等待（对齐 JS 修复版）
            if len(buffer) >= 18 and buffer[0] == TYPES['REQUEST']:
                try:
                    msg_id = str(uuid.UUID(bytes=buffer[2:18]))
                    asyncio.get_event_loop().create_task(
                        self.send_response(msg_id, {'error': 'Invalid request payload'}))
                except Exception:
                    pass

    async def _send_pong(self, msg_id: str):
        try:
            await self.send_raw(TYPES['PONG'], CONTENT_TYPES['TEXT'], msg_id, '')
        except Exception:
            pass

    async def _reject_unauthorized(self, msg_id: str):
        # 未鉴权：回错误但不断链（允许随后 hello 重试）；连续 5 次才强制断开
        self._unauthorized_hits = getattr(self, '_unauthorized_hits', 0) + 1
        try:
            await self.send_response(msg_id, {'error': 'Unauthorized'})
        except Exception:
            pass
        if self._unauthorized_hits >= 5:
            await self.destroy()

    async def _handle_request(self, msg_id: str, request):
        try:
            if isinstance(request, (bytes, bytearray)):
                decoded = decode_binary_request(bytes(request))
                method = decoded['method']
                args = [decoded['binaryData'], *decoded['params']]
            elif isinstance(request, dict):
                method = request.get('method')
                params = request.get('params')
                if isinstance(params, list):
                    args = list(params)
                elif params is None:
                    args = []
                else:
                    args = [params]
            else:
                raise ValueError('request payload is not an object')
            if not isinstance(method, str) or not method:
                raise ValueError('request method missing')
        except Exception as err:
            await self.send_response(msg_id, {'error': f'Invalid request payload: {err}'})
            return

        handler = self.handlers.get(method)
        if handler is None:
            await self.send_response(msg_id, {'error': f'Method not found: {method}'})
            return

        try:
            result = handler(*args)
            if inspect.isawaitable(result):
                result = await result

            content_type = CONTENT_TYPES['JSON']
            if result is None:
                result = None  # undefined -> null
            elif isinstance(result, str):
                content_type = CONTENT_TYPES['TEXT']
            elif isinstance(result, (bytes, bytearray)):
                result = bytes(result)
                content_type = CONTENT_TYPES['BINARY']
            await self.send_response(msg_id, result, content_type)
        except Exception as err:
            await self.send_response(msg_id, {'error': str(err)})

    def _handle_response(self, msg_id: str, response):
        pending = self.pending_requests.pop(msg_id, None)
        if pending is not None and not pending.done():
            if isinstance(response, dict) and response.get('error'):
                pending.set_exception(RpcError(response['error'], code='EREMOTE'))
            else:
                pending.set_result(response)

    # ========== 发送 ==========
    async def send_raw(self, msg_type: int, content_type: int, msg_id: str, data):
        if self.message_stream is None:
            raise RuntimeError('messageStream is not initialized')
        message = serialize(msg_type, content_type, msg_id, data)
        await self.message_stream.send(message)

    async def _wait_pending(self, msg_id: str, future, method: str, timeout):
        """等待响应；超时移除 pending 并抛 RpcError(code='ETIMEDOUT')，对齐 JS 修复版。"""
        effective = self.default_timeout if timeout is None else timeout
        try:
            if effective and effective > 0:
                return await asyncio.wait_for(future, effective)
            return await future
        except asyncio.TimeoutError:
            self.pending_requests.pop(msg_id, None)
            raise RpcError(f'RPC timeout after {int(effective * 1000)}ms: {method}',
                           code='ETIMEDOUT') from None

    async def _ensure_auth(self):
        """首次发业务请求前自动握手（拿环境变量里的 token），并发安全只握手一次。"""
        if not self.auth_token or self._auth_sent:
            return
        async with self._auth_lock:
            if self._auth_sent:
                return
            await self.hello()
            self._auth_sent = True

    async def send_request(self, method: str, params=None, to=None, timeout=None):
        """发送 JSON 请求并等待响应。params 为列表时展开为多参数。
        timeout 为秒；None 用 default_timeout，0 关闭超时。"""
        await self._ensure_auth()
        msg_id = str(uuid.uuid4())
        future = asyncio.get_event_loop().create_future()
        self.pending_requests[msg_id] = future
        body = {'method': method, 'params': params}
        if to is not None:
            body['to'] = to
        try:
            await self.send_raw(TYPES['REQUEST'], CONTENT_TYPES['JSON'], msg_id, body)
        except Exception as err:
            self.pending_requests.pop(msg_id, None)
            if not future.done():
                future.set_exception(err)
        return await self._wait_pending(msg_id, future, method, timeout)

    async def send_request_binary(self, method: str, buffer: bytes, *args, timeout=None):
        """发送二进制请求并等待响应。服务端 handler 收到 [buffer, *args]。"""
        await self._ensure_auth()
        msg_id = str(uuid.uuid4())
        future = asyncio.get_event_loop().create_future()
        self.pending_requests[msg_id] = future
        binary_request = encode_binary_request(method, list(args), buffer)
        try:
            await self.send_raw(TYPES['REQUEST'], CONTENT_TYPES['BINARY'], msg_id, binary_request)
        except Exception as err:
            self.pending_requests.pop(msg_id, None)
            if not future.done():
                future.set_exception(err)
        return await self._wait_pending(msg_id, future, method, timeout)

    async def send_response(self, msg_id: str, response, content_type: int = CONTENT_TYPES['JSON']):
        await self.send_raw(TYPES['RESPONSE'], content_type, msg_id, response)

    # ========== 辅助方法 ==========
    async def send_text(self, text: str):
        await self.send_raw(TYPES['EVENT'], CONTENT_TYPES['TEXT'], str(uuid.uuid4()), text)

    async def send_binary(self, data: bytes):
        await self.send_raw(TYPES['EVENT'], CONTENT_TYPES['BINARY'], str(uuid.uuid4()), data)

    async def send_json(self, data):
        await self.send_raw(TYPES['EVENT'], CONTENT_TYPES['JSON'], str(uuid.uuid4()), data)

    def register_method(self, name: str, handler):
        """注册方法处理器，可为同步函数或 async 函数。"""
        self.handlers[name] = handler

    async def destroy(self):
        if self._destroyed:
            return
        self._destroyed = True
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
            self._heartbeat_task = None
        for pending in self.pending_requests.values():
            if not pending.done():
                pending.set_exception(RpcError('RPC Protocol destroyed'))
        self.pending_requests.clear()
        if self.message_stream is not None:
            await self.message_stream.destroy()
            self.message_stream = None
        self.handlers.clear()
        self._listeners.clear()
        print('[RpcProtocol] Destroyed')
