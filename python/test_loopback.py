"""Python 版回环测试：TCP 服务端 + 客户端在同一进程内验证全部协议路径。"""
import asyncio
import struct
import zlib

from message_stream import MessageStream, serve_tcp
from rpc_protocol import RpcProtocol, RpcError

PASS = []


def check(name, cond):
    assert cond, f'FAILED: {name}'
    PASS.append(name)
    print(f'  ok: {name}')


async def main():
    server_streams = []

    def on_server(stream):
        server_streams.append(stream)
        rpc = RpcProtocol(stream)
        rpc.register_method('add', lambda a, b: a + b)
        rpc.register_method('echo', lambda obj: obj)
        rpc.register_method('greet', lambda name: f'hello {name}')          # str -> TEXT
        rpc.register_method('blob', lambda: b'\x00\x01\x02\xff')            # bytes -> BINARY
        rpc.register_method('nothing', lambda: None)                        # None -> JSON null
        rpc.register_method('fail', lambda: (_ for _ in ()).throw(RuntimeError('boom')))
        rpc.register_method('upload', lambda data, tag: {'size': len(data), 'tag': tag,
                                                         'head': list(data[:4])})  # 二进制请求
        rpc.register_method('push', lambda: asyncio.ensure_future(rpc.send_json({'push': 1})) or None)

        async def slow():
            await asyncio.sleep(0.5)
            return 'late'
        rpc.register_method('slow', slow)

    server = await serve_tcp(on_server, port=0)
    port = server.sockets[0].getsockname()[1]

    reader, writer = await asyncio.open_connection('127.0.0.1', port)
    client_stream = MessageStream(reader, writer)
    client = RpcProtocol(client_stream)

    events = []
    client.on_event(lambda e: events.append(e))

    # 1. JSON 请求/响应
    check('json add', await client.send_request('add', [3, 4]) == 7)
    obj = {'name': '测试', 'list': [1, 2.5, True, None]}
    check('json echo unicode', await client.send_request('echo', [obj]) == obj)

    # 2. TEXT 响应
    check('text response', await client.send_request('greet', ['世界']) == 'hello 世界')

    # 3. BINARY 响应
    check('binary response', await client.send_request('blob') == b'\x00\x01\x02\xff')

    # 4. null 响应
    check('null response', await client.send_request('nothing') is None)

    # 5. 错误响应
    try:
        await client.send_request('fail')
        check('error response', False)
    except RpcError as e:
        check('error response', e.error == 'boom')

    # 6. 方法不存在
    try:
        await client.send_request('nope')
        check('method not found', False)
    except RpcError as e:
        check('method not found', 'Method not found: nope' in str(e.error))

    # 7. 二进制请求
    payload = bytes(range(256)) * 300  # ~75KB，超过默认 64KB 分片
    res = await client.send_request_binary('upload', payload, 'tag-1')
    check('binary request', res == {'size': len(payload), 'tag': 'tag-1', 'head': [0, 1, 2, 3]})

    # 8. 事件（TEXT/JSON/BINARY）：服务端 -> 客户端
    srv = server_streams[0]
    from rpc_protocol import serialize, TYPES, CONTENT_TYPES
    import uuid as _uuid
    await srv.send(serialize(TYPES['EVENT'], CONTENT_TYPES['JSON'], str(_uuid.uuid4()), {'push': 1}))
    await srv.send(serialize(TYPES['EVENT'], CONTENT_TYPES['TEXT'], str(_uuid.uuid4()), 'srv-text'))
    await srv.send(serialize(TYPES['EVENT'], CONTENT_TYPES['BINARY'], str(_uuid.uuid4()), b'\x01\x02'))
    await asyncio.sleep(0.2)
    check('events received', len(events) == 3
          and events[0] == {'contentType': 1, 'content': {'push': 1}}
          and events[1] == {'contentType': 0, 'content': 'srv-text'}
          and events[2] == {'contentType': 2, 'content': b'\x01\x02'})

    # 9. 并发请求
    results = await asyncio.gather(*[client.send_request('add', [i, i]) for i in range(50)])
    check('concurrent requests', results == [i * 2 for i in range(50)])

    # 10. CRC 错误恢复：客户端注入长度头 CRC 损坏的帧 -> 服务端检测到错误并回错误包
    #     -> 客户端回 RESET_CONFIRM -> 服务端 syncComplete，之后通信应正常
    srv_errors = []
    srv.on('error', lambda e: srv_errors.append(e))
    sync_done = asyncio.Event()
    srv.on('syncComplete', lambda: sync_done.set())

    bad = struct.pack('<I', 5) + struct.pack('<I', 0xDEADBEEF) + b'hello' + struct.pack('<I', zlib.crc32(b'hello'))
    writer.write(bad)
    await writer.drain()
    await asyncio.wait_for(sync_done.wait(), timeout=3)
    check('crc error detected', len(srv_errors) >= 1)
    check('resync then rpc works', await client.send_request('add', [10, 20]) == 30)

    # 11. RPC 超时：显式短超时触发 ETIMEDOUT，pending 清理后迟到响应不影响后续调用
    try:
        await client.send_request('slow', timeout=0.1)
        check('rpc timeout ETIMEDOUT', False)
    except RpcError as e:
        check('rpc timeout ETIMEDOUT', e.code == 'ETIMEDOUT' and 'slow' in str(e.error))
    check('after timeout rpc works', await client.send_request('add', [1, 2]) == 3)
    await asyncio.sleep(0.6)  # 等 slow 的迟到响应到达（应被静默丢弃）
    check('late response ignored', await client.send_request('add', [5, 6]) == 11)
    check('default timeout is 15s', client.default_timeout == 15.0)

    # 12. HELLO 握手：requiredToken 服务端 + 正确 token，握手后业务正常；错误 token → EAUTH
    auth_streams = []

    def on_auth_server(stream):
        auth_streams.append(stream)
        rpc = RpcProtocol(stream, required_token='secret')
        rpc.register_method('add', lambda a, b: a + b)

    auth_server = await serve_tcp(on_auth_server, port=0)
    aport = auth_server.sockets[0].getsockname()[1]
    ar, aw = await asyncio.open_connection('127.0.0.1', aport)
    auth_client = RpcProtocol(MessageStream(ar, aw), auth_token='secret')
    ack = await auth_client.hello()
    check('hello ok', ack.get('ok') is True and ack.get('version') == 1)
    check('after hello add', await auth_client.send_request('add', [5, 6]) == 11)
    await auth_client.destroy()

    ar2, aw2 = await asyncio.open_connection('127.0.0.1', aport)
    bad_client = RpcProtocol(MessageStream(ar2, aw2), auth_token='wrong')
    try:
        await bad_client.hello()
        check('hello wrong token EAUTH', False)
    except RpcError as e:
        check('hello wrong token EAUTH', e.code == 'EAUTH')
    await bad_client.destroy()

    # 13. 未握手直接请求 → Unauthorized 错误响应
    ar3, aw3 = await asyncio.open_connection('127.0.0.1', aport)
    noauth_client = RpcProtocol(MessageStream(ar3, aw3))
    try:
        await noauth_client.send_request('add', [1, 2])
        check('unauthenticated request rejected', False)
    except RpcError as e:
        check('unauthenticated request rejected', e.code == 'EREMOTE' and e.error == 'Unauthorized')
    await noauth_client.destroy()
    auth_server.close()

    # 14. 版本不匹配：裸 MessageStream 对端发 version=99 的 HELLO → ACK error
    ver_streams = []

    def on_ver_server(stream):
        ver_streams.append(stream)
        RpcProtocol(stream)

    ver_server = await serve_tcp(on_ver_server, port=0)
    vport = ver_server.sockets[0].getsockname()[1]
    vr, vw = await asyncio.open_connection('127.0.0.1', vport)
    raw_ms = MessageStream(vr, vw)
    ack_box = []
    ack_evt = asyncio.Event()

    def on_ack(buf):
        from rpc_protocol import deserialize
        m = deserialize(buf)
        if m['type'] == TYPES['HELLO_ACK']:
            ack_box.append(m['content'])
            ack_evt.set()

    raw_ms.on('message', on_ack)
    await raw_ms.send(serialize(TYPES['HELLO'], CONTENT_TYPES['JSON'], str(_uuid.uuid4()), {'version': 99}))
    await asyncio.wait_for(ack_evt.wait(), timeout=3)
    check('version mismatch ack error', 'unsupported protocol version: 99' in str(ack_box[0].get('error')))
    await raw_ms.destroy()
    ver_server.close()

    # 15. 心跳保活：短间隔心跳下长时间空闲，链路不被误杀
    hb_streams = []

    def on_hb_server(stream):
        hb_streams.append(stream)
        rpc = RpcProtocol(stream, heartbeat_interval=0.1, heartbeat_timeout=0.5)
        rpc.register_method('add', lambda a, b: a + b)

    hb_server = await serve_tcp(on_hb_server, port=0)
    hport = hb_server.sockets[0].getsockname()[1]
    hr, hw = await asyncio.open_connection('127.0.0.1', hport)
    hb_client = RpcProtocol(MessageStream(hr, hw), heartbeat_interval=0.1, heartbeat_timeout=0.5)
    await asyncio.sleep(0.8)  # 远超 timeout，靠 PING/PONG 维持
    check('heartbeat keepalive add', await hb_client.send_request('add', [3, 4]) == 7)
    await hb_client.destroy()
    hb_server.close()

    # 16. 心跳超时：对端静默（半开模拟）→ EHEARTBEAT + 在途请求结算 + 断链
    async def silent_peer(r, w):
        await asyncio.sleep(3600)  # 接受连接后完全静默且不关闭

    silent_server = await asyncio.start_server(silent_peer, '127.0.0.1', 0)
    sport = silent_server.sockets[0].getsockname()[1]
    sr, sw = await asyncio.open_connection('127.0.0.1', sport)
    hb2_client = RpcProtocol(MessageStream(sr, sw), heartbeat_interval=0.1, heartbeat_timeout=0.3)
    hb_errors = []
    hb2_client.on('error', lambda e: hb_errors.append(e))
    try:
        await hb2_client.send_request('add', [1, 2], timeout=0)  # 关闭 RPC 超时，只靠心跳
        check('heartbeat timeout EHEARTBEAT', False)
    except RpcError as e:
        check('heartbeat timeout EHEARTBEAT', e.code == 'EHEARTBEAT')
    check('heartbeat closes rpc', hb2_client._destroyed is True)
    silent_server.close()

    # 17. 重同步超时：对端注入坏 CRC 帧后静默 → ERESYNC_TIMEOUT + 断链
    async def bad_peer(r, w):
        w.write(bad)
        await w.drain()
        await asyncio.sleep(3600)  # 发完坏帧即静默，不关闭连接

    bad_server = await asyncio.start_server(bad_peer, '127.0.0.1', 0)
    bport = bad_server.sockets[0].getsockname()[1]
    br, bw = await asyncio.open_connection('127.0.0.1', bport)
    rs_ms = MessageStream(br, bw, sync_timeout=0.3)
    rs_client = RpcProtocol(rs_ms)
    rs_codes = []
    rs_client.on('error', lambda e: rs_codes.append(getattr(e, 'code', None)))
    try:
        await rs_client.send_request('add', [1, 2], timeout=0)
        check('resync timeout rejects pending', False)
    except RpcError as e:
        # 首个流错误（LENGTH_CRC_FAILED）即结算 pending；随后重同步超时断链
        check('resync timeout rejects pending', e.code in ('LENGTH_CRC_FAILED', 'ERESYNC_TIMEOUT'))
    await asyncio.sleep(0.5)  # 等满 sync_timeout=0.3s
    check('resync ERESYNC_TIMEOUT emitted', 'ERESYNC_TIMEOUT' in rs_codes)
    check('resync timeout closes stream', rs_ms._closed is True)
    bad_server.close()

    # 18. destroy 后资源清理
    await client.destroy()
    for s in server_streams:
        await s.destroy()
    server.close()
    print(f'\nALL {len(PASS)} PYTHON TESTS PASSED')


asyncio.run(main())