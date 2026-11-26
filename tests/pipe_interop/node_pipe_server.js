// Node 端作为命名管道服务端（生产形态：Node 主进程托管 \\.\pipe\xxx）
// 用法: node node_pipe_server.js <pipeName>
import net from 'net';
import { MessageStream } from '../../MessageStream.js';
import { RpcProtocol } from '../../RpcProtocol.js';

const pipeName = process.argv[2] || 'ipc_interop_test';
const path = `\\\\.\\pipe\\${pipeName}`;

const server = net.createServer((socket) => {
    socket.setNoDelay?.(true);
    const ms = new MessageStream({ socket });
    const rpc = new RpcProtocol(ms, { heartbeatIntervalMs: 5000, heartbeatTimeoutMs: 15000 });

    rpc.on('error', (e) => console.log(`[server rpc] error ${e.code || ''} ${e.message || ''}`));
    ms.on('error', (e) => console.log(`[server ms] error ${e.code || ''}`));
    ms.on('close', () => console.log('[server] client closed'));

    rpc.registerMethod('add', (a, b) => a + b);
    rpc.registerMethod('text', () => 'hello-text-✓');
    rpc.registerMethod('json', (o) => ({ ...o, server: 'node', ok: true }));
    rpc.registerMethod('nul', () => null);
    rpc.registerMethod('binEcho', (buf) => buf);                 // 二进制请求 → 二进制响应
    rpc.registerMethod('len', (buf) => ({ n: buf.length, head: buf.subarray(0, 4).toString('hex') }));
    rpc.registerMethod('slow', async (ms) => { await new Promise(r => setTimeout(r, ms)); return `slept-${ms}`; });
    rpc.registerMethod('boom', () => { throw new Error('boom!'); });
    rpc.registerMethod('push', async (n) => {
        for (let i = 0; i < n; i++) { await rpc.sendText(`evt-${i}`); await rpc.sendJson({ i }); await rpc.sendBinary(Buffer.from([i])); }
        return n;
    });

    console.log(`[server] client connected, pid=${process.pid}`);
});

server.on('error', (e) => console.log('[server] listen error', e.message));
server.listen(path, () => console.log(`[server] LISTENING ${path}`));

setTimeout(() => { console.log('[server] timeout, exit'); process.exit(0); }, 180000);
