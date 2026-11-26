"""v1.1.2 回归：Python 客户端默认鉴权 + 能力协商（连 Node 命名管道服务端，token 来自环境变量）

用法（Node 服务端已启动）:
    set IPC_TOKEN=pytok-123 & set PIPE_NAME=v112_py & python tests/v112/py_auth_client.py
"""
import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'python'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'pipe_interop'))

from message_stream import MessageStream      # noqa: E402
from rpc_protocol import RpcProtocol, RpcError  # noqa: E402
from py_named_pipe import open_pipe           # noqa: E402

PASS = 0
FAIL = 0


def ok(cond, name, extra=''):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f'  PASS {name}')
    else:
        FAIL += 1
        print(f'  FAIL {name} {extra}')


async def connect(pipe_name):
    path = pipe_name if pipe_name.startswith(r'\\.\pipe') else rf'\\.\pipe\{pipe_name}'
    reader, writer, conn = await open_pipe(path)
    ms = MessageStream(reader, writer)
    rpc = RpcProtocol(ms, heartbeat_interval=0)
    return rpc, writer, conn


async def main():
    pipe = os.environ['PIPE_NAME']
    mode = os.environ.get('MODE', 'good')
    token = os.environ.get('IPC_TOKEN')

    print(f'\n[Python 鉴权/{mode}] pipe={pipe} token={token!r}')
    rpc, writer, conn = await connect(pipe)
    try:
        if mode == 'good':
            # 不显式 hello：send_request 内部自动握手（token 取自 IPC_TOKEN）
            r = await rpc.send_request('add', [10, 32], timeout=8)
            ok(r == 42, '自动 hello 后业务调用', str(r))
            ok(rpc.server_capabilities is not None and 'ping' in rpc.server_capabilities,
               '拿到服务端能力', str(rpc.server_capabilities))
            info = await rpc.send_request('peerCaps', None, timeout=8)
            ok(isinstance(info, dict) and 'ping' in (info.get('peer') or []),
               '服务端看到 Python 客户端能力', str(info))
        else:
            try:
                await rpc.send_request('add', [1, 2], timeout=8)
                ok(False, '错误 token 应被拒')
            except RpcError as err:
                ok(err.code == 'EAUTH', '错误 token → EAUTH', f'code={err.code} msg={err}')
    finally:
        try:
            await rpc.destroy()
        except Exception:
            pass
        try:
            writer.close()
        except Exception:
            pass

    print(f'\n结果: {PASS} passed, {FAIL} failed')
    return 1 if FAIL else 0


sys.exit(asyncio.run(main()))
