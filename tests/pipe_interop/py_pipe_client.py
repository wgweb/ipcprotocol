# -*- coding: utf-8 -*-
"""Python 子进程 <-> Node 主进程，Windows 命名管道互操作测试。"""
import asyncio
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'python')))
sys.path.insert(0, os.path.dirname(__file__))

from message_stream import MessageStream
from rpc_protocol import RpcProtocol, RpcError
from py_named_pipe import open_pipe

PASSED = 0


def check(name, cond):
    global PASSED
    assert cond, f'FAILED: {name}'
    PASSED += 1
    print(f'  ok: {name}')


async def main():
    pipe = '\\\\.\\pipe\\' + (sys.argv[1] if len(sys.argv) > 1 else 'ipc_interop_test')
    reader, writer, conn = await open_pipe(pipe)
    ms = MessageStream(reader, writer)
    rpc = RpcProtocol(ms)
    events = []
    rpc.on_event(lambda ev: events.append((ev['contentType'], ev['content'])))

    check('connect pipe', True)

    check('json add', await rpc.send_request('add', [3, 4]) == 7)
    check('text response', await rpc.send_request('text') == 'hello-text-✓')
    payload = {'name': '中文', 'arr': [1, 2, 3], 'nested': {'k': None}}
    check('unicode/null roundtrip', await rpc.send_request('json', [payload]) == {**payload, 'server': 'node', 'ok': True})
    check('explicit null', await rpc.send_request('nul') is None)

    try:
        await rpc.send_request('boom')
        check('error response', False)
    except RpcError as e:
        check('error response', 'boom' in str(e))

    try:
        await rpc.send_request('no_such_method')
        check('method not found', False)
    except RpcError as e:
        check('method not found', 'not found' in str(e).lower())

    data = bytes(range(256)) * 300          # 76800 字节
    echoed = await rpc.send_request_binary('binEcho', data)
    check('binary 75KB echo', isinstance(echoed, (bytes, bytearray)) and bytes(echoed) == data)

    info = await rpc.send_request_binary('len', b'\x01\x02\x03\x04' + b'\x00' * 12)
    check('binary -> json result', info['n'] == 16 and info['head'] == '01020304')

    check('slow task 800ms', await rpc.send_request('slow', [800], timeout=5000) == 'slept-800')

    results = await asyncio.gather(*[rpc.send_request('add', [i, i]) for i in range(50)])
    check('50 concurrent', results == [i * 2 for i in range(50)])

    n = await rpc.send_request('push', [3])
    await asyncio.sleep(0.3)
    check(f'events received ({len(events)})', len(events) == 9)

    # 空闲 12s：Node 端心跳 5s/15s，Python 必须回 PONG 才能存活
    await asyncio.sleep(12)
    check('idle 12s survives heartbeat', await rpc.send_request('add', [1, 1]) == 2)

    conn.close()
    print(f'\nALL {PASSED} PYTHON-PIPE TESTS PASSED')


asyncio.run(main())
