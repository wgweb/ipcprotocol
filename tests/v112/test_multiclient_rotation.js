// v1.1.2 回归：多客户端会话 + token 热轮换（NativeService 作为管道服务端，子侧为客户端）
import net from 'net';
import { MessageStream } from '../../MessageStream.js';
import { RpcProtocol } from '../../RpcProtocol.js';
import { NativeService } from '../../nativeService.js';

let pass = 0, fail = 0;
const ok = (cond, name, extra = '') => {
    if (cond) { pass++; console.log(`  PASS ${name}`); }
    else { fail++; console.log(`  FAIL ${name} ${extra}`); }
};
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

async function connect(pipe, token, label) {
    const socket = net.connect(pipe);
    await new Promise((res, rej) => { socket.once('connect', res); socket.once('error', rej); });
    socket.setNoDelay?.(true);
    const ms = new MessageStream({ socket });
    const rpc = new RpcProtocol(ms, { authToken: token, heartbeatIntervalMs: 0, heartbeatTimeoutMs: 0 });
    rpc.on('error', () => { });
    ms.on('error', () => { });
    // 子侧注册方法：主进程通过这些方法回调
    rpc.registerMethod('add', (a, b) => a + b);
    rpc.registerMethod('whoami', () => ({ label, pid: process.pid }));
    await rpc.hello(3000);
    return { socket, rpc, label };
}

// 仅起管道服务端，不 spawn 子进程
const svc = new NativeService('v112multi', { cmd: process.execPath, args: ['-e', '1'], requireAuth: true, tokenGraceMs: 1500 });
svc.on('error', () => { });
// 服务端也注册方法，验证"客户端 → 服务端"方向
svc.registerMethod('add', (a, b) => a + b);

const clients = [];
try {
    await sleep(400);

    console.log('\n[6] 多客户端会话');
    for (let i = 0; i < 3; i++) {
        const c = await connect(svc.pipeName, svc.authToken, `c${i + 1}`);
        c.id = svc.getClients().slice(-1)[0].id;
        clients.push(c);
    }
    ok(svc.clientCount === 3, '3 个客户端同时在线', String(svc.clientCount));
    const list = svc.getClients();
    ok(list.length === 3 && list.filter(x => x.primary).length === 1, '清单 + 唯一主会话', JSON.stringify(list));
    ok(list[0].primary === true && list[0].id === clients[0].id, '首个连入者是主会话', JSON.stringify(list[0]));

    const who = await svc.call.whoami();
    ok(who.label === 'c1', '默认 call 路由到主会话', JSON.stringify(who));
    const sum = await svc.call.add(20, 22);
    ok(sum === 42, '主会话业务调用', String(sum));

    const w2 = await svc.callTo(clients[1].id).whoami();
    const w3 = await svc.callTo(clients[2].id).whoami();
    ok(w2.label === 'c2' && w3.label === 'c3', 'callTo 精确路由', `${w2.label}/${w3.label}`);
    ok((await svc.callTo(clients[2].id).add(1, 1)) === 2, 'callTo 业务调用', '');

    const received = [0, 0, 0];
    clients.forEach((c, i) => c.rpc.onEvent(() => { received[i]++; }));
    const sent = await svc.broadcastText('fanout');
    ok(sent === 3, 'broadcastText 覆盖全部客户端', String(sent));
    await sleep(300);
    ok(received.every(n => n >= 1), '所有客户端都收到广播', JSON.stringify(received));

    // 主会话断开 → 自动切换，剩余会话继续可用
    const firstId = svc.getPrimaryClientId();
    clients[0].socket.destroy();
    await sleep(400);
    ok(svc.clientCount === 2, '断开后剩 2 个客户端', String(svc.clientCount));
    ok(svc.getPrimaryClientId() !== firstId && !!svc.getPrimaryClientId(), '主会话已切换', `${firstId} → ${svc.getPrimaryClientId()}`);
    const whoAfter = await svc.call.whoami();
    ok(whoAfter.label === 'c2', '切换后默认路由指向新主会话', JSON.stringify(whoAfter));

    // ---------- token 热轮换 ----------
    console.log('\n[7] token 热轮换');
    const oldToken = svc.authToken;
    const newToken = svc.rotateToken({ graceMs: 1500 });
    ok(newToken !== oldToken && newToken.length >= 16, '生成新 token', newToken.length);
    ok(svc.getProcessStatus().acceptedTokens === 2, '轮换期同时接受新旧 token', String(svc.getProcessStatus().acceptedTokens));

    ok((await svc.call.add(5, 5)) === 10, '已握手会话不受轮换影响', '');

    const cOld = await connect(svc.pipeName, oldToken, 'old');
    ok(true, '宽限期内旧 token 可握手', '');
    clients.push(cOld);

    const cNew = await connect(svc.pipeName, newToken, 'new');
    ok(true, '新 token 可握手', '');
    clients.push(cNew);

    await sleep(1800);
    ok(svc.getProcessStatus().acceptedTokens === 1, '宽限期后旧 token 吊销', String(svc.getProcessStatus().acceptedTokens));
    let rejected = false;
    try { await connect(svc.pipeName, oldToken, 'revoked'); } catch (e) { rejected = e.code === 'EAUTH'; }
    ok(rejected, '吊销后的旧 token → EAUTH', '');

    ok((await cOld.rpc.sendRequest('add', [6, 7], undefined, 4000)) === 13, '旧 token 已建会话继续可用', '');
    ok((await cNew.rpc.sendRequest('add', [8, 9], undefined, 4000)) === 17, '新 token 会话正常', '');
} catch (err) {
    fail++;
    console.log('  FAIL 异常:', err.stack || err.message);
} finally {
    for (const c of clients) { try { c.socket.destroy(); } catch { } }
    svc.stopServer();
    await sleep(300);
}

console.log(`\n结果: ${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);

