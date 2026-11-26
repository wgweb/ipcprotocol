"""互操作测试：Python 客户端 <-> Node 原版服务端（RpcProtocol.js / MessageStream.js）。"""
import asyncio
import json
import os
import struct
import subprocess
import sys
import zlib

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'python'))

from message_stream import MessageStream
from rpc_protocol import RpcProtocol, RpcError

ROOT = os.path.join(os.path.dirname(__file__), '..')
PASS = []


def check(name, cond):
    assert cond, f'FAILED: {name}'
    PASS.append(name)
    print(f'  ok: {name}')


async def main():
    proc = subprocess.Popen(
        ['node', os.path.join(ROOT, 'examples', 'node_server.js')],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, cwd=ROOT)
    try:
        line = proc.stdout.readline()
        port = json.loads(line)['port']
        print(f'Node server on port {port}')

        reader, writer = await asyncio.open_connection('127.0.0.1', port)
        stream = MessageStream(reader, writer)
        client = RpcProtocol(stream)
        events = []
        client.on_event(lambda e: events.append(e))

        check('json add', await client.send_request('add', [3, 4]) == 7)
        obj = {'name': '测试', 'list': [1, 2.5, True, None]}
        check('json echo unicode', await client.send_request('echo', [obj]) == obj)
        check('text response', await client.send_request('greet', ['世界']) == 'hello 世界')
        check('binary response', await client.send_request('blob') == b'\x00\x01\x02\xff')
        check('null response', await client.send_request('nothing') is None)

        try:
            await client.send_request('fail')
            check('error response', False)
        except RpcError as e:
            check('error response', e.error == 'boom')

        try:
            await client.send_request('nope')
            check('method not found', False)
        except RpcError as e:
            check('method not found', 'Method not found: nope' in str(e.error))

        payload = bytes(range(256)) * 300
        res = await client.send_request_binary('upload', payload, 'tag-1')
        check('binary request', res == {'size': len(payload), 'tag': 'tag-1', 'head': [0, 1, 2, 3]})

        check('pushEvents', await client.send_request('pushEvents') == 'sent')
        await asyncio.sleep(0.3)
        check('events received', len(events) == 3
              and events[0] == {'contentType': 1, 'content': {'push': 1}}
              and events[1] == {'contentType': 0, 'content': 'srv-text'}
              and events[2] == {'contentType': 2, 'content': b'\x01\x02'})

        results = await asyncio.gather(*[client.send_request('add', [i, i]) for i in range(50)])
        check('concurrent requests', results == [i * 2 for i in range(50)])

        # CRC 错误恢复：Python 客户端注入坏帧 -> Node 服务端检测并回错误包
        # -> Python 客户端回 RESET_CONFIRM -> Node 端 syncComplete 后通信继续
        bad = struct.pack('<I', 5) + struct.pack('<I', 0xDEADBEEF) + b'hello' + struct.pack('<I', zlib.crc32(b'hello'))
        writer.write(bad)
        await writer.drain()
        await asyncio.sleep(0.5)
        check('resync then rpc works', await client.send_request('add', [10, 20]) == 30)

        await client.destroy()
        print(f'\nALL {len(PASS)} PYTHON<->NODE INTEROP TESTS PASSED')
    finally:
        proc.kill()


asyncio.run(main())