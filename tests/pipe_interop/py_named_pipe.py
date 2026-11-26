# -*- coding: utf-8 -*-
"""把 Windows 命名管道包装成 asyncio (StreamReader, StreamWriter-like) 接口，
供 python/message_stream.py 直接复用 —— 协议层不动，只换传输。

用法:
    reader, writer = await open_pipe(r'\\\\.\\pipe\\ipc_interop_test')
    ms = MessageStream(reader, writer)
"""
import asyncio
import ctypes
from ctypes import wintypes, byref, create_string_buffer, c_ulong

kernel32 = ctypes.WinDLL('kernel32', use_last_error=True)

GENERIC_READ = 0x80000000
GENERIC_WRITE = 0x40000000
OPEN_EXISTING = 3
FILE_FLAG_OVERLAPPED = 0x40000000
ERROR_IO_PENDING = 997
ERROR_OPERATION_ABORTED = 995
INVALID_HANDLE_VALUE = wintypes.HANDLE(-1).value

# 关键：64 位下必须声明 restype，否则 HANDLE 会被截断成 32 位 int，后续 ReadFile 报 ERROR_INVALID_HANDLE(6)
kernel32.CreateFileW.restype = wintypes.HANDLE
kernel32.CreateFileW.argtypes = [
    wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD, ctypes.c_void_p,
    wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE,
]
kernel32.CreateEventW.restype = wintypes.HANDLE
kernel32.CreateEventW.argtypes = [ctypes.c_void_p, wintypes.BOOL, wintypes.BOOL, wintypes.LPCWSTR]
kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
kernel32.CancelIoEx.argtypes = [wintypes.HANDLE, ctypes.c_void_p]


class OVERLAPPED(ctypes.Structure):
    _fields_ = [
        ('Internal', ctypes.c_ulonglong),
        ('InternalHigh', ctypes.c_ulonglong),
        ('Offset', wintypes.DWORD),
        ('OffsetHigh', wintypes.DWORD),
        ('hEvent', wintypes.HANDLE),
    ]


def _check(handle):
    if handle == INVALID_HANDLE_VALUE or not handle:
        raise OSError(f'CreateFileW failed, err={kernel32.GetLastError()}')


class PipeWriter:
    """最小 StreamWriter 替身：write() 异步排队，drain() 等待写完。"""

    def __init__(self, handle, loop):
        self._handle = handle
        self._loop = loop
        self._pending = []
        self._closed = False

    def write(self, data: bytes) -> None:
        if self._closed:
            return
        self._pending.append(self._loop.run_in_executor(None, self._write_blocking, data))

    def _write_blocking(self, data: bytes) -> None:
        ov = OVERLAPPED()
        ov.hEvent = kernel32.CreateEventW(None, True, False, None)
        buf = create_string_buffer(bytes(data), len(data))
        written = c_ulong(0)
        ok = kernel32.WriteFile(self._handle, buf, len(data), byref(written), byref(ov))
        if not ok:
            err = kernel32.GetLastError()
            if err != ERROR_IO_PENDING:
                kernel32.CloseHandle(ov.hEvent)
                raise OSError(f'WriteFile failed, err={err}')
            if not kernel32.GetOverlappedResult(self._handle, byref(ov), byref(written), True):
                err = kernel32.GetLastError()
                kernel32.CloseHandle(ov.hEvent)
                raise OSError(f'GetOverlappedResult failed, err={err}')
        kernel32.CloseHandle(ov.hEvent)

    async def drain(self) -> None:
        if self._pending:
            pending, self._pending = self._pending, []
            await asyncio.gather(*pending)

    async def wait_closed(self) -> None:
        await self.drain()

    @property
    def transport(self):
        return None


class PipeConn:
    def __init__(self, handle, reader, writer, loop):
        self.handle = handle
        self.reader = reader
        self.writer = writer
        self._loop = loop
        self._closed = False

    def _reader_blocking(self, reader: asyncio.StreamReader, loop: asyncio.AbstractEventLoop):
        try:
            while not self._closed:
                ov = OVERLAPPED()
                ov.hEvent = kernel32.CreateEventW(None, True, False, None)
                buf = create_string_buffer(65536)
                nread = c_ulong(0)
                ok = kernel32.ReadFile(self.handle, buf, 65536, byref(nread), byref(ov))
                if not ok:
                    err = kernel32.GetLastError()
                    if err == ERROR_IO_PENDING:
                        if not kernel32.GetOverlappedResult(self.handle, byref(ov), byref(nread), True):
                            err = kernel32.GetLastError()
                            kernel32.CloseHandle(ov.hEvent)
                            if err == ERROR_OPERATION_ABORTED:
                                break
                            loop.call_soon_threadsafe(reader.set_exception, OSError(f'read failed err={err}'))
                            return
                    elif err == ERROR_OPERATION_ABORTED:
                        break
                    else:
                        kernel32.CloseHandle(ov.hEvent)
                        loop.call_soon_threadsafe(reader.set_exception, OSError(f'ReadFile failed err={err}'))
                        return
                kernel32.CloseHandle(ov.hEvent)
                n = nread.value
                if n == 0:
                    break
                loop.call_soon_threadsafe(reader.feed_data, bytes(buf[:n]))
        finally:
            loop.call_soon_threadsafe(reader.feed_eof)

    def close(self):
        if self._closed:
            return
        self._closed = True
        try:
            kernel32.CancelIoEx(self.handle, None)
        except Exception:
            pass
        try:
            kernel32.CloseHandle(self.handle)
        except Exception:
            pass


async def open_pipe(path: str):
    """连接命名管道，返回 (reader, writer)；reader 是标准 asyncio.StreamReader。"""
    loop = asyncio.get_running_loop()
    handle = kernel32.CreateFileW(
        path,
        GENERIC_READ | GENERIC_WRITE,
        0, None, OPEN_EXISTING, FILE_FLAG_OVERLAPPED, None,
    )
    _check(handle)
    reader = asyncio.StreamReader(limit=2 ** 20)
    writer = PipeWriter(handle, loop)
    conn = PipeConn(handle, reader, writer, loop)
    loop.run_in_executor(None, conn._reader_blocking, reader, loop)
    return reader, writer, conn
