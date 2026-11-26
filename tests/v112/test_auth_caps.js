// v1.1.2 回归：默认鉴权 + 能力协商（Node 服务端 ↔ Node 客户端，命名管道）
// 运行: node tests/v112/test_auth_caps.js
import net from 'net';
import { spawn } from 'child_process';
import { MessageStream } from '../../MessageStream.js';
import { RpcProtocol } from '../../RpcProtocol.js';

let pass = 0, fail = 0;
const ok = (cond, name, extra = '') => {
    if (cond) { pass++; console.log(`  PASS ${name}`); }
    else { fail++; console.log(`  FAIL ${name} ${extra}`); }
};
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const call = (rpc, method, params = null, timeout = 4000) => rpc.sendRequest(method, params, undefined, timeout);

function startServer(pipeName, env = {}) {
    const child = spawn(process.execPath, ['tests/v112/node_auth_server.js'], {
        env: { ...process.env, PIPE_NAME: pipeName, ...env },
        stdio: ['ignore', 'pipe', 'pipe'],
    });
    child.stdout.on('data', d => process.env.VERBOSE && console.log('[srv]', d.toString().trimEnd()));
    child.stderr.on('data', d => console.log('[srv:err]', d.toString().trimEnd()));
    return child;
}

async function connect(pipeName) {
    const socket = net.connect(`\\\\.\\pipe\\${pipeName}`);
    await new Promise((resolve, reject) => {
        socket.once('connect', resolve);
        socket.once('error', reject);
    });
    socket.setNoDelay?.(true);
    const ms = new MessageStream({ socket });
    const rpc = new RpcProtocol(ms, { heartbeatIntervalMs: 0, heartbeatTimeoutMs: 0 });
    rpc.on('error', () => { });
    ms.on('error', () => { });
    return { socket, ms, rpc };
}

const expectErr = async (p, code, name) => {
    try { await p; ok(false, `${name} (未报错)`); }
    catch (e) { ok(e.code === code, `${name} → ${e.code}`, `got=${e.code} msg=${e.message}`); }
};

// ---------- 1) 鉴权 ----------
async function testAuth() {
    console.log('\n[1] 默认鉴权');
    const pipe = `v112_auth_${Date.now()}`;
    const srv = startServer(pipe, { IPC_TOKEN: 'secret-token-123' });
    await sleep(700);
    try {
        const c1 = await connect(pipe);
        // 1.1 完全不握手、不带 token → EREMOTE(unauthorized)
        await expectErr(call(c1.rpc, 'add', [1, 2]), 'EREMOTE', '未握手请求被拒');
        c1.socket.destroy();
        await sleep(200);

        // 1.2 错误 token → EAUTH
        const c2 = await connect(pipe);
        c2.rpc.authToken = 'wrong-token';
        await expectErr(c2.rpc.hello(3000), 'EAUTH', '错误 token 握手失败');
        c2.socket.destroy();
        await sleep(200);

        // 1.3 正确 token + hello → 正常；能力双向可见
        const c3 = await connect(pipe);
        c3.rpc.authToken = 'secret-token-123';
        const ack = await c3.rpc.hello(3000);
        ok(ack.ok === true, 'hello ok');
        ok(Array.isArray(c3.rpc.serverCapabilities) && c3.rpc.serverCapabilities.includes('ping'), '客户端拿到服务端能力', JSON.stringify(c3.rpc.serverCapabilities));
        const r = await call(c3.rpc, 'add', [20, 22]);
        ok(r === 42, '鉴权后业务调用', String(r));
        const pcs = await call(c3.rpc, 'peerCaps');
        ok(Array.isArray(pcs.peer) && pcs.peer.includes('ping'), '服务端拿到客户端能力', JSON.stringify(pcs.peer));

        // 1.4 已握手后再发请求正常（token 只校验一次）
        ok((await call(c3.rpc, 'add', [1, 1])) === 2, '二次调用正常');
        c3.socket.destroy();
    } finally { srv.kill(); }
}

// ---------- 2) 能力协商：对端不支持 ping 时心跳不误杀 ----------
async function testCapabilityNegotiation() {
    console.log('\n[2] 能力协商（HELLO 声明无 ping → 服务端停用心跳，不误杀空闲链路）');
    const pipe = `v112_caps_${Date.now()}`;
    // 服务端心跳 500ms / 超时 1000ms：若仍按旧逻辑，1s 无数据就会把链路打死
    const srv = startServer(pipe, { IPC_TOKEN: 'tok2', HB_INTERVAL_MS: '500', HB_TIMEOUT_MS: '1000' });
    await sleep(700);
    try {
        const { socket, rpc } = await connect(pipe);
        rpc.authToken = 'tok2';
        rpc.capabilities = ['binary', 'event'];       // 故意声明「不支持心跳」
        rpc.heartbeatIntervalMs = 0;                  // 本端也不发 PING
        await rpc.hello(3000);
        ok(rpc.serverCapabilities?.includes('ping') === true, '服务端能力已知');
        await sleep(1600);                            // 静默跨过服务端 1s 超时窗口
        const r = await call(rpc, 'add', [1, 2]);
        ok(r === 3, '空闲 1.6s 后链路仍存活', String(r));
        await sleep(1200);
        const info = await call(rpc, 'whoami');
        ok(info.pid > 0, '再空闲 1.2s 仍可用', JSON.stringify(info));
        socket.destroy();
    } finally { srv.kill(); }
}

// ---------- 3) 未声明能力（旧实现）→ 不降级，心跳照常 ----------
async function testLegacyNoCaps() {
    console.log('\n[3] 旧实现对端（HELLO 不声明 caps）→ 心跳保持默认');
    const pipe = `v112_legacy_${Date.now()}`;
    const srv = startServer(pipe, { IPC_TOKEN: 'tok3' });
    await sleep(700);
    try {
        const { socket, rpc } = await connect(pipe);
        rpc.authToken = 'tok3';
        rpc.heartbeatDisabledReason = null;
        await rpc.hello(3000);
        ok(rpc.heartbeatDisabledReason === null, '未见 caps 不触发降级', String(rpc.heartbeatDisabledReason));
        ok((await call(rpc, 'add', [5, 5])) === 10, '旧路径仍可用');
        socket.destroy();
    } finally { srv.kill(); }
}

await testAuth();
await testCapabilityNegotiation();
await testLegacyNoCaps();
console.log(`\n结果: ${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);

