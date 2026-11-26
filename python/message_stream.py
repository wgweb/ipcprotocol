"""
MessageStream 的 Python 移植版（与 MessageStream.js 线格式完全兼容）。

帧格式（正常模式，enable_crc=True 时）:
    [长度 u32LE][长度头CRC32 u32LE][数据...][数据CRC32 u32LE]
关闭 CRC 时:
    [长度 u32LE][数据...]

错误包（作为普通帧发送，5 字节）:
    [0xEE 0xEE 0xEE 0xEE][错误类型 1B]
    错误类型: 1=LENGTH_CRC_FAILED, 2=MESSAGE_CRC_FAILED, 3=BUFFER_OVERFLOW

重同步协议消息（不带 CRC 的裸帧）:
    [0xFF 0xFF 0xFF 0xFF][长度 u32LE][数据(如 'RESET_CONFIRM')]

CRC32 与 JS 版一致: 多项式 0xEDB88320（反射），初值 0xFFFFFFFF，结果异或 0xFFFFFFFF，
等价于 zlib.crc32。
"""

import asyncio
import struct
import zlib

PROTOCOL_HEADER = b'\xFF\xFF\xFF\xFF'
ERROR_HEADER = b'\xEE\xEE\xEE\xEE'

ERROR_TYPES = {
    'LENGTH_CRC_FAILED': 1,
    'MESSAGE_CRC_FAILED': 2,
    'BUFFER_OVERFLOW': 3,
}
ERROR_TYPE_CODES = {v: k for k, v in ERROR_TYPES.items()}


class StreamError(Exception):
    """带错误码的流错误（对齐 JS 修复版 ipcError 的 code 字段）。"""

    def __init__(self, message, code=None):
        self.code = code
        super().__init__(message)


class MessageStream:
    """基于 asyncio StreamReader/StreamWriter 的消息流，语义对齐 MessageStream.js。

    事件（通过 on() 注册回调）:
        'message'      -> callback(data: bytes)
        'error'        -> callback(err: Exception | str)
        'syncComplete' -> callback()
    """

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
                 enable_crc: bool = True, max_buffer_size: int = 256 * 1024 * 1024,
                 sync_timeout: float = 3.0):
        if reader is None or writer is None:
            raise ValueError('reader/writer is undefined')
        self._reader = reader
        self._writer = writer
        self.enable_crc = enable_crc
        self.max_buffer_size = max_buffer_size
        self.sync_timeout = sync_timeout  # 重同步握手超时（秒）；<=0 关闭

        self._buffer = bytearray()
        self._head = 0  # 逻辑读游标：消费只前移游标，避免 O(n^2) 头部删除
        self._expected_length = None
        self._protocol_mode = False
        self._send_lock = asyncio.Lock()
        self._listeners = {}
        self._closed = False
        self._sync_event = asyncio.Event()  # 发送门控：非协议模式时置位
        self._sync_event.set()
        self._sync_task = None
        self._read_task = asyncio.get_event_loop().create_task(self._read_loop())

    # ========== 事件 ==========
    def on(self, event, callback):
        self._listeners.setdefault(event, []).append(callback)

    def _emit(self, event, *args):
        for cb in list(self._listeners.get(event, [])):
            try:
                cb(*args)
            except Exception as err:  # 与 JS EventEmitter 不同，回调异常不应中断读循环
                print(f'[MessageStream] listener error: {err}')

    # ========== 发送 ==========
    async def send(self, data: bytes):
        """发送一条消息（带长度头与可选 CRC），按调用顺序串行化。
        协议模式期间门控等待重同步完成（对齐 JS _syncGate）。"""
        if len(data) > self.max_buffer_size:
            raise ValueError('Data size exceeds maximum allowed limit')
        while self._protocol_mode and not self._closed:
            await self._sync_event.wait()
        async with self._send_lock:
            if self._closed:
                raise ConnectionError('Stream is not connected')
            frame = self._build_frame(data)
            self._writer.write(frame)
            await self._writer.drain()

    def _build_frame(self, data: bytes) -> bytes:
        length_header = struct.pack('<I', len(data))
        if self.enable_crc:
            return (length_header
                    + struct.pack('<I', zlib.crc32(length_header))
                    + data
                    + struct.pack('<I', zlib.crc32(data)))
        return length_header + data

    async def send_error(self, error_type: str):
        """发送错误包（按普通帧格式封装）；不受重同步门控。"""
        code = ERROR_TYPES.get(error_type, 0)
        try:
            async with self._send_lock:
                if self._closed:
                    raise ConnectionError('Stream is not connected')
                self._writer.write(self._build_frame(ERROR_HEADER + bytes([code])))
                await self._writer.drain()
        except Exception as err:
            print(f'Failed to send error packet: {err}')

    async def send_protocol_message(self, message: str):
        """发送协议消息: [FFFFFFFF][长度LE][数据]，不带 CRC。"""
        data = message.encode('utf8')
        frame = PROTOCOL_HEADER + struct.pack('<I', len(data)) + data
        try:
            async with self._send_lock:
                if self._closed:
                    raise ConnectionError('Stream is not connected')
                self._writer.write(frame)
                await self._writer.drain()
        except Exception as err:
            print(f"Failed to send protocol message '{message}': {err}")

    # ========== 缓冲辅助（头游标 + 摊还 compact） ==========
    def _buf_len(self):
        return len(self._buffer) - self._head

    def _read_u32(self, offset: int) -> int:
        return struct.unpack_from('<I', self._buffer, self._head + offset)[0]

    def _take(self, n: int, offset: int = 0) -> bytes:
        start = self._head + offset
        return bytes(self._buffer[start:start + n])

    def _consume(self, n: int):
        self._head += n
        if self._head >= 1024 * 1024 or self._head * 2 >= len(self._buffer):
            del self._buffer[:self._head]
            self._head = 0

    def _clear_buffer(self):
        self._buffer.clear()
        self._head = 0

    def _find_protocol_header(self) -> int:
        window = min(self._buf_len(), 64 * 1024)
        if window < 4:
            return -1
        return self._buffer.find(PROTOCOL_HEADER, self._head, self._head + window)

    # ========== 接收 ==========
    async def _read_loop(self):
        try:
            while not self._closed:
                data = await self._reader.read(65536)
                if not data:
                    break
                self._on_data(data)
        except (ConnectionError, OSError) as err:
            if not self._closed:
                self._emit('error', StreamError(str(err), 'ESOCKET'))
        finally:
            if not self._closed:
                self._emit('error', StreamError('Connection closed', 'ECLOSED'))

    def _on_data(self, data: bytes):
        self._buffer.extend(data)

        if self._buf_len() > self.max_buffer_size:
            self._emit('error', StreamError(f'Buffer size: {self._buf_len()} exceeds maximum allowed limit', 'BUFFER_OVERFLOW'))
            asyncio.get_event_loop().create_task(self.send_error('BUFFER_OVERFLOW'))
            self.reset()
            return

        while self._buf_len() > 0:
            # 协议模式：扫描协议头，而不是整段丢弃（避免误丢 RESET_CONFIRM）
            if self._protocol_mode:
                idx = self._find_protocol_header()
                if idx >= 0:
                    self._consume(idx - self._head)
                    if self._buf_len() < 8:
                        break  # 等待长度字段
                    length = self._read_u32(4)
                    if length > 50:
                        self._consume(8)  # 伪头：跳过继续扫描
                        continue
                    if self._buf_len() < 8 + length:
                        break  # 等待协议消息收全
                    message_data = self._take(length, 8)
                    self._consume(8 + length)
                    self._handle_protocol_message(message_data)
                    continue
                # 窗口内无协议头：丢弃已扫描部分，保留尾部 3 字节（可能半截头）
                keep = min(3, self._buf_len())
                self._consume(self._buf_len() - keep)
                break

            # 正常模式
            if self._expected_length is None:
                header_size = 8 if self.enable_crc else 4
                if self._buf_len() < header_size:
                    break

                length = self._read_u32(0)
                if self.enable_crc:
                    expected_crc = self._read_u32(4)
                    calculated = zlib.crc32(self._take(4))
                    if calculated != expected_crc:
                        print(f'Length header CRC check failed: expected {expected_crc}, got {calculated}')
                        asyncio.get_event_loop().create_task(self.send_error('LENGTH_CRC_FAILED'))
                        self._emit('error', StreamError('Length header CRC check failed', 'LENGTH_CRC_FAILED'))
                        self.reset()
                        return

                # 长度头强制校验：0 或超限视为坏帧（CRC 合法也可能是伪造/损坏的长度）
                if length == 0 or length > self.max_buffer_size:
                    code = 'BUFFER_OVERFLOW' if length > self.max_buffer_size else 'LENGTH_CRC_FAILED'
                    asyncio.get_event_loop().create_task(self.send_error(code))
                    self._emit('error', StreamError(f'invalid frame length {length}', 'BAD_LENGTH'))
                    self.reset()
                    return

                self._expected_length = length
                self._consume(header_size)

            total = self._expected_length + (4 if self.enable_crc else 0)
            if self._buf_len() >= total:
                message_data = self._take(self._expected_length)
                if self.enable_crc:
                    message_crc = self._read_u32(self._expected_length)
                    calculated = zlib.crc32(message_data)
                    if calculated != message_crc:
                        print(f'Message CRC check failed: expected {message_crc}, got {calculated}')
                        asyncio.get_event_loop().create_task(self.send_error('MESSAGE_CRC_FAILED'))
                        self._emit('error', StreamError('Message CRC check failed', 'MESSAGE_CRC_FAILED'))
                        self.reset()
                        return

                self._consume(total)
                self._expected_length = None

                # 错误包在交给上层之前拦截处理
                if self._is_error_packet(message_data):
                    code = message_data[4]
                    name = ERROR_TYPE_CODES.get(code, 'UNKNOWN_ERROR')
                    print('Received error packet:', name)
                    self._emit('error', StreamError(f'peer reported {name}', 'ERESYNC'))
                    self.reset()
                    asyncio.get_event_loop().create_task(self.send_protocol_message('RESET_CONFIRM'))
                    self._exit_protocol_mode()
                    continue

                self._emit('message', message_data)
            else:
                break

    def _handle_protocol_message(self, message_data: bytes) -> bool:
        text = message_data.decode('utf8', errors='replace').strip()
        if text == 'RESET_CONFIRM':
            print('Received reset confirmation')
            self._exit_protocol_mode()
            self._clear_buffer()
            self._expected_length = None
            self._emit('syncComplete')
            return True
        return False

    def _exit_protocol_mode(self):
        self._protocol_mode = False
        self._cancel_sync_watch()
        self._sync_event.set()

    def _cancel_sync_watch(self):
        if self._sync_task is not None:
            self._sync_task.cancel()
            self._sync_task = None

    async def _sync_watch(self):
        """重同步握手超时：上报 ERESYNC_TIMEOUT 并断链（对齐 JS syncTimeout）。"""
        try:
            await asyncio.sleep(self.sync_timeout)
        except asyncio.CancelledError:
            return
        if not self._protocol_mode or self._closed:
            return
        self._sync_task = None  # 避免 _exit_protocol_mode 取消当前任务自身
        self._exit_protocol_mode()
        self._emit('error', StreamError('resync handshake timeout', 'ERESYNC_TIMEOUT'))
        await self.destroy()

    @staticmethod
    def _is_error_packet(data: bytes) -> bool:
        return len(data) == 5 and data[:4] == ERROR_HEADER

    def reset(self):
        """清空接收状态并进入协议模式，等待对端 RESET_CONFIRM。"""
        print('[MessageStream] Resetting stream state')
        self._clear_buffer()
        self._expected_length = None
        self._protocol_mode = True
        self._sync_event.clear()
        self._cancel_sync_watch()
        if self.sync_timeout > 0:
            self._sync_task = asyncio.get_event_loop().create_task(self._sync_watch())

    def get_status(self):
        return {
            'bufferLength': self._buf_len(),
            'expectedLength': self._expected_length,
            'enableCRC': self.enable_crc,
            'protocolMode': self._protocol_mode,
        }

    async def destroy(self):
        if self._closed:
            return
        self._closed = True
        self._cancel_sync_watch()
        self._sync_event.set()  # 唤醒门控中的发送方，使其以 ECLOSED 失败
        self._read_task.cancel()
        try:
            self._writer.close()
            await self._writer.wait_closed()
        except Exception:
            pass
        self._clear_buffer()
        self._listeners.clear()
        print('[MessageStream] Destroyed')


async def connect_tcp(host: str, port: int, **kwargs) -> MessageStream:
    """便捷方法：建立 TCP 连接并包装为 MessageStream。"""
    reader, writer = await asyncio.open_connection(host, port)
    return MessageStream(reader, writer, **kwargs)


async def serve_tcp(handler, host: str = '127.0.0.1', port: int = 0, **kwargs):
    """便捷方法：启动 TCP 服务器，每个连接回调 handler(MessageStream)。

    返回 asyncio.Server，可通过 server.sockets[0].getsockname() 获取实际端口。
    """
    async def on_conn(reader, writer):
        stream = MessageStream(reader, writer, **kwargs)
        result = handler(stream)
        if asyncio.iscoroutine(result):
            await result

    return await asyncio.start_server(on_conn, host, port)