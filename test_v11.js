// JS v1.1 回环测试：心跳 PING/PONG、HELLO 握手、重同步超时断链
// 运行：node test_v11.js
import net from 'net';
import assert from 'node:assert';
import { MessageStream, crc32 } from './MessageStream.js';
import { RpcProtocol, uuidv4 } from './RpcProtocol.js';

let passed = 0;
function check(name, cond) {
    assert.ok(cond, `FAIL ${name}`);
    passed++;
    console.log(`PASS ${name}`);
}
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function listen(server) {
    return new Promise(r => server.listen(0, '127.0.0.1', () => r(server.address().port)));
}
function connect(port) {
    return new Promise((resolve, reject) => {
        const sock = net.connect(port, '127.0.0.1', () => resolve(sock));
        sock.once('error', reject);
    });
}
function makeRpc(sock, opts = {}) {
    const rpc = new RpcProtocol(new MessageStream({ socket: sock }), opts);
    rpc.on('error', () => { });
    rpc.registerMethod('add', (a, b) => a + b);
    return rpc;
}
// 客户端 rpc + 服务端 rpc 对
async function rpcPair(clientOpts = {}, serverOpts = {}) {
    let serverRpc, srv;
    const ready = new Promise(r => {
        srv = net.createServer(sock => { serverRpc = makeRpc(sock, serverOpts); r(); });
    });
    const port = await listen(srv);
    const sock = await connect(port);
    const clientRpc = makeRpc(sock, clientOpts);
    await ready;
    return { srv, serverRpc, clientRpc };
}
function badCrcFrame() {
    const payload = Buffer.from('hello');
    const frame = Buffer.alloc(17);
    frame.writeUInt32LE(payload.length, 0);
    frame.writeUInt32LE(0xDEADBEEF, 4);   // 错误的长度头 CRC
    payload.copy(frame, 8);
    frame.writeUInt32LE(crc32(payload), 13);
    return frame;
}

// 1. 心跳保活：短间隔心跳下长时间空闲，链路不被误杀，PING/PONG 正常往返
{
    const hb = { heartbeatIntervalMs: 100, heartbeatTimeoutMs: 500 };
    const { srv, serverRpc, clientRpc } = await rpcPair(hb, hb);
    await sleep(800);   // 远超 timeout，靠 PING/PONG 维持
    check('heartbeat keepalive add', await clientRpc.sendRequest('add', [3, 4]) === 7);
    clientRpc.destroy(); serverRpc.destroy(); srv.close();
}

// 2. 心跳超时：对端静默（半开模拟）→ EHEARTBEAT + 在途请求结算 + 断链
{
    const srv = net.createServer(() => { });   // 接受连接后完全静默
    const port = await listen(srv);
    const sock = await connect(port);
    // v1.1.1 起默认有在途请求时不断链（避免长任务被误杀），本用例专测心跳断链，故显式关闭该豁免
    const rpc = new RpcProtocol(new MessageStream({ socket: sock }),
        { heartbeatIntervalMs: 100, heartbeatTimeoutMs: 300, heartbeatSkipWhilePending: false });
    let errCode = null;
    rpc.on('error', e => { errCode = e.code; });
    const p = rpc.sendRequest('add', [1, 2], undefined, 0);   // 关闭 RPC 超时，只靠心跳
    await assert.rejects(p, e => e.code === 'EHEARTBEAT');
    check('heartbeat timeout rejects pending', true);
    await sleep(50);
    check('heartbeat EHEARTBEAT emitted', errCode === 'EHEARTBEAT');
    check('heartbeat closes stream', rpc._destroyed === true);
    srv.close();
}

// 3. HELLO 成功：requiredToken 服务端 + 正确 token 客户端，握手后业务正常
{
    const { srv, serverRpc, clientRpc } = await rpcPair(
        { authToken: 'secret' }, { requiredToken: 'secret' });
    const ack = await clientRpc.hello();
    check('hello ok', ack.ok === true && ack.version === 1);
    check('after hello add', await clientRpc.sendRequest('add', [5, 6]) === 11);
    clientRpc.destroy(); serverRpc.destroy(); srv.close();
}

// 4. HELLO 错误 token → EAUTH，服务端断链
{
    const { srv, serverRpc, clientRpc } = await rpcPair(
        { authToken: 'wrong' }, { requiredToken: 'secret' });
    await assert.rejects(clientRpc.hello(), e => e.code === 'EAUTH');
    check('hello wrong token EAUTH', true);
    await sleep(600);   // v1.1.2：ack 先落地，300ms 后再断链
    check('auth fail closes server', serverRpc._destroyed === true);
    clientRpc.destroy(); srv.close();
}

// 5. 未握手直接请求 → Unauthorized 错误响应
{
    const { srv, serverRpc, clientRpc } = await rpcPair({}, { requiredToken: 'secret' });
    await assert.rejects(clientRpc.sendRequest('add', [1, 2]),
        e => e.code === 'EREMOTE' && e.data === 'Unauthorized');
    check('unauthenticated request rejected', true);
    clientRpc.destroy(); srv.close();
}

// 6. 版本不匹配：裸 MessageStream 对端发 version=99 的 HELLO → ACK error
{
    let ackContent = null;
    let srv;
    const gotAck = new Promise(r => {
        srv = net.createServer(sock => {
            const ms = new MessageStream({ socket: sock });
            ms.on('error', () => { });
            ms.on('message', buf => {
                const m = RpcProtocol.deserialize(buf);
                if (m.type === RpcProtocol.TYPES.HELLO_ACK) { ackContent = m.content; r(); }
            });
            ms.send(RpcProtocol.serialize(RpcProtocol.TYPES.HELLO, 1, uuidv4(), { version: 99 }));
        });
    });
    const port = await listen(srv);
    const sock = await connect(port);
    const serverRpc = makeRpc(sock);
    await gotAck;
    check('version mismatch ack error',
        String(ackContent?.error).includes('unsupported protocol version: 99'));
    serverRpc.destroy(); srv.close();
}

// 7. 重同步超时：对端注入坏 CRC 帧后静默 → ERESYNC_TIMEOUT + 在途请求结算 + 断链
{
    const srv = net.createServer(sock => { sock.write(badCrcFrame()); });   // 发完坏帧即静默
    const port = await listen(srv);
    const sock = await connect(port);
    const rpc = new RpcProtocol(new MessageStream({ socket: sock, syncTimeout: 300 }));
    const codes = [];
    rpc.on('error', e => { codes.push(e.code); });
    const p = rpc.sendRequest('add', [1, 2], undefined, 0);
    // JS 语义：首个流错误（LENGTH_CRC_FAILED）即结算 pending；随后重同步超时断链
    await assert.rejects(p, e => e.code === 'LENGTH_CRC_FAILED' || e.code === 'ERESYNC_TIMEOUT');
    check('resync timeout rejects pending', true);
    await sleep(400);   // 等满 syncTimeout=300ms
    check('resync ERESYNC_TIMEOUT emitted', codes.includes('ERESYNC_TIMEOUT'));
    check('resync timeout closes', rpc.messageStream?._closed === true);
    srv.close();
}

console.log(`\nALL ${passed} JS v1.1 TESTS PASSED`);
process.exit(0);
