// Node 原版协议测试服务端：node examples/node_server.js [port]
// 启动后向 stdout 输出 {"port": ...}，供互操作测试脚本读取。
import net from 'net';
import { MessageStream } from '../MessageStream.js';
import { RpcProtocol } from '../RpcProtocol.js';

const port = Number(process.argv[2] || 0);

const server = net.createServer((socket) => {
    const stream = new MessageStream({ socket });
    const rpc = new RpcProtocol(stream);
    // 必须监听 error：Node EventEmitter 在无监听器时 emit('error') 会直接崩溃进程
    rpc.on('error', (err) => console.error('[node_server] stream error:', err.message || err));

    rpc.registerMethod('add', (a, b) => a + b);
    rpc.registerMethod('echo', (obj) => obj);
    rpc.registerMethod('greet', (name) => `hello ${name}`);            // string -> TEXT
    rpc.registerMethod('blob', () => Buffer.from([0x00, 0x01, 0x02, 0xFF])); // Buffer -> BINARY
    rpc.registerMethod('nothing', () => undefined);                    // undefined -> JSON null
    rpc.registerMethod('fail', () => { throw new Error('boom'); });
    rpc.registerMethod('upload', (data, tag) => ({                     // 二进制请求
        size: data.length,
        tag,
        head: [...data.subarray(0, 4)],
    }));
    rpc.registerMethod('pushEvents', async () => {
        await rpc.sendJson({ push: 1 });
        await rpc.sendText('srv-text');
        await rpc.sendBinary(Buffer.from([1, 2]));
        return 'sent';
    });
});

server.listen(port, '127.0.0.1', () => {
    console.log(JSON.stringify({ port: server.address().port }));
});