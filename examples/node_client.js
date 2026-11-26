// Node 原版协议测试客户端：node examples/node_client.js <port>
// 连接任意语言实现的服务端，验证请求/响应/事件/二进制请求。
import net from 'net';
import { MessageStream } from '../MessageStream.js';
import { RpcProtocol } from '../RpcProtocol.js';

const port = Number(process.argv[2]);
let passed = 0;
function check(name, cond) {
    if (!cond) { console.error(`FAILED: ${name}`); process.exit(1); }
    passed++;
    console.log(`  ok: ${name}`);
}

const socket = net.connect(port, '127.0.0.1', async () => {
    const stream = new MessageStream({ socket });
    const rpc = new RpcProtocol(stream);
    rpc.on('error', (err) => console.error('[node_client] stream error:', err.message || err));
    const events = [];
    rpc.onEvent((e) => events.push(e));

    try {
        check('json add', await rpc.sendRequest('add', [3, 4]) === 7);
        const obj = { name: '测试', list: [1, 2.5, true, null] };
        const echo = await rpc.sendRequest('echo', [obj]);
        check('json echo unicode', echo.name === '测试' && echo.list[1] === 2.5);
        check('text response', await rpc.sendRequest('greet', ['世界']) === 'hello 世界');
        const blob = await rpc.sendRequest('blob');
        check('binary response', Buffer.isBuffer(blob) && blob.equals(Buffer.from([0, 1, 2, 0xFF])));
        check('null response', await rpc.sendRequest('nothing') === null);

        try { await rpc.sendRequest('fail'); check('error response', false); }
        catch (err) { check('error response', String(err.data ?? err.error ?? err) === 'boom'); }

        try { await rpc.sendRequest('nope'); check('method not found', false); }
        catch (err) { check('method not found', String(err.data ?? err.error ?? err).includes('Method not found: nope')); }

        const payload = Buffer.alloc(256 * 300);
        for (let i = 0; i < payload.length; i++) payload[i] = i & 0xFF;
        const up = await rpc.sendRequestBinary('upload', payload, 'tag-1');
        check('binary request', up.size === payload.length && up.tag === 'tag-1' && up.head[3] === 3);

        await rpc.sendRequest('pushEvents');
        await new Promise(r => setTimeout(r, 300));
        check('events received', events.length === 3
            && events[0].contentType === 1 && events[0].content.push === 1
            && events[1].contentType === 0 && events[1].content === 'srv-text'
            && events[2].contentType === 2 && Buffer.from(events[2].content).equals(Buffer.from([1, 2])));

        const results = await Promise.all(Array.from({ length: 50 }, (_, i) => rpc.sendRequest('add', [i, i])));
        check('concurrent requests', results.every((v, i) => v === i * 2));

        console.log(`\nALL ${passed} NODE-CLIENT TESTS PASSED`);
        rpc.destroy();
        process.exit(0);
    } catch (err) {
        console.error('FAILED:', err);
        process.exit(1);
    }
});
socket.on('error', (err) => { console.error('socket error:', err.message); process.exit(1); });