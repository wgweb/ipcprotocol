// v1.1.2 验证用「子进程」：由 nativeService 拉起，按 PIPE_NAME/IPC_TOKEN 反向连接
import net from 'net';
import { MessageStream } from '../../MessageStream.js';
import { RpcProtocol } from '../../RpcProtocol.js';

const pipePath = process.env.PIPE_NAME;
const token = process.env.IPC_TOKEN || null;
const socket = net.connect(pipePath);
const ms = new MessageStream({ socket });
const rpc = new RpcProtocol(ms, {
    authToken: token,
    heartbeatIntervalMs: 5000,
    heartbeatTimeoutMs: 15000,
});

rpc.on('error', (e) => console.log(`[child] rpc error ${e.code || ''} ${e.message || ''}`));
ms.on('error', (e) => console.log(`[child] ms error ${e.code || ''}`));

rpc.registerMethod('echo', (x) => x);
rpc.registerMethod('add', (a, b) => a + b);
rpc.registerMethod('info', () => ({ pid: process.pid, token: token ? 'yes' : 'no', serverCaps: rpc.serverCapabilities }));
rpc.registerMethod('die', (code = 3) => {
    // 先回响应，再自杀，模拟崩溃
    setTimeout(() => process.exit(Number(code) || 3), 150);
    return `dying-${code}`;
});

socket.on('connect', async () => {
    try { await rpc.hello(); } catch (e) { console.log('[child] hello failed', e.code, e.message); process.exit(9); }
    console.log(`[child] connected+hello pid=${process.pid}`);
});
