// v1.1.2 回归：进程守护（自动重启 + 重连 + token 下发 + 优雅停止不重启）
import { NativeService } from '../../nativeService.js';

let pass = 0, fail = 0;
const ok = (cond, name, extra = '') => {
    if (cond) { pass++; console.log(`  PASS ${name}`); }
    else { fail++; console.log(`  FAIL ${name} ${extra}`); }
};
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const waitEvent = (emitter, name, timeout = 15000) => new Promise((resolve, reject) => {
    const t = setTimeout(() => { emitter.off(name, h); reject(new Error(`timeout waiting ${name}`)); }, timeout);
    const h = (p) => { clearTimeout(t); resolve(p); };
    emitter.once(name, h);
});

console.log('\n[4] 进程守护');
const svc = new NativeService('v112child', {
    cmd: process.execPath,
    args: ['tests/v112/child_node.js'],
    restartDelayMs: 300,
    maxRestartDelayMs: 2000,
    maxRestarts: 5,
});

const events = { crash: [], restarting: [], restarted: [] };
svc.on('crash', p => { events.crash.push(p); console.log('  · crash', JSON.stringify(p)); });
svc.on('restarting', p => { events.restarting.push(p); console.log('  · restarting', JSON.stringify(p)); });
svc.on('restarted', p => events.restarted.push(p));
svc.on('error', e => console.log('  · svc error:', e.message));
svc.on('give-up', p => console.log('  · give-up', JSON.stringify(p)));

try {
    svc.start();
    await svc.waitForConnection(30000);
    console.log('  · 首次连接成功');

    const st = svc.getProcessStatus();
    ok(st.authRequired === true, '默认开启鉴权', JSON.stringify(st));
    ok(!!svc.authToken, '自动生成 token', svc.authToken ? 'yes' : 'no');

    const sum = await svc.call.add(2, 3);
    ok(sum === 5, '子进程业务调用（带 token 自动握手）', String(sum));

    const info1 = await svc.call.info();
    ok(info1.token === 'yes', '子进程确实拿到 IPC_TOKEN', JSON.stringify(info1));
    ok(Array.isArray(info1.serverCaps) && info1.serverCaps.includes('ping'), '子进程看到服务端能力', JSON.stringify(info1.serverCaps));
    const pid1 = info1.pid;

    // 模拟崩溃：子进程在回包后 process.exit(3)
    const crashP = waitEvent(svc, 'crash');
    const dead = await svc.call.die(3);
    ok(dead === 'dying-3', 'die 调用已返回', String(dead));
    const crash = await crashP;
    ok(crash.code === 3, '捕获 crash 事件（退出码 3）', JSON.stringify(crash));
    ok(svc.isConnected === false, '崩溃瞬间链路已标记断开', String(svc.isConnected));

    // 自动重连：同一管道名，子进程重启后重新 hello
    const restartedP = waitEvent(svc, 'restarted');
    const re = await restartedP;
    ok(re.attempt === 1, '第一次重启', JSON.stringify(re));
    await svc.waitForConnection(20000);
    ok(events.restarting.length === 1 && events.restarting[0].delay === 300, '首次退避 300ms', JSON.stringify(events.restarting));

    const info2 = await svc.call.info();
    ok(info2.pid !== pid1, '重启后是新的子进程', `${pid1} → ${info2.pid}`);
    ok((await svc.call.add(7, 8)) === 15, '重启后业务恢复', '');

    // 再崩一次：验证退避翻倍（300 → 600）
    const crash2P = waitEvent(svc, 'crash');
    await svc.call.die(1);
    const c2 = await crash2P;
    ok(c2.code === 1, '第二次 crash', JSON.stringify(c2));
    await waitEvent(svc, 'restarted');
    await svc.waitForConnection(20000);
    ok(events.restarting[1]?.delay === 600, '第二次退避 600ms（指数增长）', JSON.stringify(events.restarting[1]));
    ok((await svc.call.add(1, 1)) === 2, '二次重启后仍可用', '');
    ok(svc.getProcessStatus().crashes === 2, '崩溃计数正确', JSON.stringify(svc.getProcessStatus()));

    // 优雅停止：不应触发重启
    const crashesBefore = events.crash.length;
    await svc.stop();
    await sleep(1200);
    ok(svc.process === null, 'stop 后进程已清理', '');
    ok(events.crash.length === crashesBefore, 'stop 不产生 crash/重启事件', `crashes=${events.crash.length}`);
} catch (err) {
    fail++;
    console.log('  FAIL 异常:', err.message);
} finally {
    try { await svc.stop(); } catch { }
    svc.stopServer?.();
}

// ---------- 5) 关掉 autoRestart：崩了就不重启，给出 give-up ----------
console.log('\n[5] autoRestart:false');
const svc2 = new NativeService('v112noresart', {
    cmd: process.execPath,
    args: ['tests/v112/child_node.js'],
    autoRestart: false,
});
let giveUp = null, crashed = null;
svc2.on('give-up', p => { giveUp = p; });
svc2.on('crash', p => { crashed = p; });
try {
    svc2.start();
    await svc2.waitForConnection(20000);
    ok((await svc2.call.add(3, 4)) === 7, '初始可用');
    svc2.kill?.();
    await svc2.call.die(2).catch(() => { });
    await sleep(1500);
    ok(crashed !== null, '捕获 crash', JSON.stringify(crashed));
    ok(giveUp === null, 'autoRestart:false 不进入重启流程', '');
    ok(svc2.process === null, '不重启，进程保持为空', '');
} catch (err) {
    fail++;
    console.log('  FAIL 异常:', err.message);
} finally {
    try { await svc2.stop(); } catch { }
    svc2.stopServer?.();
}

console.log(`\n结果: ${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
