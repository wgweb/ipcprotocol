// v1.1.2 验证用 Node 管道服务端：可开关鉴权 / 可自定义能力声明
// 用法: PIPE_NAME=xxx IPC_TOKEN=tok CAPS=ping,binary,event node node_auth_server.js
import net from 'net';
import { MessageStream } from '../../MessageStream.js';
import { RpcProtocol } from '../../RpcProtocol.js';

const pipeName = process.env.PIPE_NAME || 'v112_auth_test';
const path = pipeName.startsWith('\\\\') ? pipeName : `\\\\.\\pipe\\${pipeName}`;
const requiredToken = process.env.IPC_TOKEN || null;
const caps = process.env.CAPS ? process.env.CAPS.split(',').filter(Boolean) : undefined;
const hbInterval = Number(process.env.HB_INTERVAL_MS || 5000);
const hbTimeout = Number(process.env.HB_TIMEOUT_MS || 15000);

const server = net.createServer((socket) => {
    socket.setNoDelay?.(true);
    const ms = new MessageStream({ socket });
    const rpc = new RpcProtocol(ms, {
        requiredToken,
        capabilities: caps,
        heartbeatIntervalMs: hbInterval,
        heartbeatTimeoutMs: hbTimeout,
    });

    rpc.on('error', (e) => console.log(`[server rpc] error ${e.code || ''} ${e.message || ''}`));
    rpc.on('heartbeatDisabled', (r) => console.log(`[server] heartbeat disabled: ${r}`));
    ms.on('error', (e) => console.log(`[server ms] error ${e.code || ''}`));
    ms.on('close', () => console.log('[server] client closed'));

    rpc.registerMethod('add', (a, b) => a + b);
    rpc.registerMethod('echo', (x) => x);
    rpc.registerMethod('peerCaps', () => ({
        peer: rpc.peerCapabilities,
        mine: rpc.capabilities,
        impl: rpc.implName,
    }));
    rpc.registerMethod('whoami', () => ({ pid: process.pid, authRequired: !!requiredToken }));

    console.log(`[server] client connected, token=${requiredToken ? 'required' : 'none'} caps=${(caps || ['default']).join(',')}`);
});

server.on('error', (e) => console.log('[server] listen error', e.message));
server.listen(path, () => console.log(`[server] LISTENING ${path}`));
setTimeout(() => { console.log('[server] timeout, exit'); process.exit(0); }, 90000);
