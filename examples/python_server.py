"""Python 示例服务端（可被 Node/C#/Java 客户端连接）。

用法: python examples/python_server.py [port]
启动后向 stdout 输出 {"port": ...}。默认端口 0 表示自动分配。
"""
import asyncio
import json
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'python'))

from message_stream import serve_tcp
from rpc_protocol import RpcProtocol


def on_connection(stream):
    rpc = RpcProtocol(stream)
    rpc.register_method('add', lambda a, b: a + b)
    rpc.register_method('echo', lambda obj: obj)
    rpc.register_method('greet', lambda name: f'hello {name}')
    rpc.register_method('blob', lambda: b'\x00\x01\x02\xff')
    rpc.register_method('nothing', lambda: None)

    def fail():
        raise RuntimeError('boom')
    rpc.register_method('fail', fail)
    rpc.register_method('upload', lambda data, tag: {
        'size': len(data), 'tag': tag, 'head': list(data[:4])})

    async def push_events():
        await rpc.send_json({'push': 1})
        await rpc.send_text('srv-text')
        await rpc.send_binary(b'\x01\x02')
        return 'sent'
    rpc.register_method('pushEvents', push_events)


async def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    server = await serve_tcp(on_connection, port=port)
    actual = server.sockets[0].getsockname()[1]
    print(json.dumps({'port': actual}), flush=True)
    async with server:
        await server.serve_forever()


asyncio.run(main())