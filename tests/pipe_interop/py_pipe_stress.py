# -*- coding: utf-8 -*-
"""Windows 命名管道吞吐/大帧压测（Python 客户端 <-> Node 服务端）。"""
import asyncio
import os
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'python')))
sys.path.insert(0, os.path.dirname(__file__))

from message_stream import MessageStream
from rpc_protocol import RpcProtocol
from py_named_pipe import open_pipe


async def main():
    pipe = '\\\\.\\pipe\\' + (sys.argv[1] if len(sys.argv) > 1 else 'ipc_stress')
    reader, writer, conn = await open_pipe(pipe)
    rpc = RpcProtocol(MessageStream(reader, writer))

    # 1) 顺序吞吐
    t0 = time.perf_counter()
    n = 2000
    for i in range(n):
        await rpc.send_request('add', [i, 1])
    dt = time.perf_counter() - t0
    print(f'顺序 2000 次: {dt:.3f}s  {n/dt:,.0f} req/s')

    # 2) 并发
    for conc in (100, 500):
        t0 = time.perf_counter()
        await asyncio.gather(*[rpc.send_request('add', [i, 1]) for i in range(conc)])
        dt = time.perf_counter() - t0
        print(f'并发 {conc}: {dt*1000:.1f}ms  {conc/dt:,.0f} req/s')

    # 3) 大帧往返（走命名管道，验证 maxBuffer/分片/CRC）
    for mb in (1, 4, 8):
        data = os.urandom(mb * 1024 * 1024)
        t0 = time.perf_counter()
        echoed = await rpc.send_request_binary('binEcho', data, timeout=120)
        dt = time.perf_counter() - t0
        ok = bytes(echoed) == data
        print(f'二进制 {mb}MB 往返: {dt:.3f}s  {mb*2/dt:.1f} MB/s  一致={ok}')
        assert ok

    conn.close()
    print('STRESS DONE')


asyncio.run(main())
