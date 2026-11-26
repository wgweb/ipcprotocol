// Windows 命名管道内存长稳（同进程内 server + client，便于读 RSS 斜率）
// 用法: node node_pipe_soak.js [秒数]
import net from 'net';
import { MessageStream } from '../../MessageStream.js';
import { RpcProtocol } from '../../RpcProtocol.js';

const SECONDS = Number(process.argv[2] || 120);
const pipeName = `ipc_soak_${process.pid}`;
const path = `\\\\.\\pipe\\${pipeName}`;

const server = net.createServer((socket) => {
    const rpc = new RpcProtocol(new MessageStream({ socket }));
    rpc.on('error', () => { });
    rpc.registerMethod('add', (a, b) => a + b);
    rpc.registerMethod('echo', (o) => o);
    rpc.registerMethod('binEcho', (b) => b);
});
await new Promise((r) => server.listen(path, r));

const sock = await new Promise((resolve, reject) => {
    const s = net.connect(path, () => resolve(s));
    s.once('error', reject);
});
const rpc = new RpcProtocol(new MessageStream({ socket: sock }));
rpc.on('error', () => { });

const t0 = Date.now();
let stop = false, calls = 0, bigCalls = 0;
const small = async () => {
    while (!stop) {
        try { await rpc.sendRequest('add', [1, 1]); calls++; } catch { }
    }
};
const big = async () => {
    while (!stop) {
        try { await rpc.sendRequestBinary('binEcho', Buffer.alloc(256 * 1024, 7)); bigCalls++; } catch { }
    }
};
const workers = Array.from({ length: 6 }, small);
workers.push(big());

const sample = () => {
    const m = process.memoryUsage();
    console.log(`t=${((Date.now() - t0) / 1000).toFixed(0)}s rss=${(m.rss / 1048576).toFixed(2)}MB heap=${(m.heapUsed / 1048576).toFixed(2)}MB extern=${(m.external / 1048576).toFixed(2)}MB calls=${calls} big=${bigCalls}`);
};
const timer = setInterval(sample, 15000);
sample();

await new Promise((r) => setTimeout(r, SECONDS * 1000));
stop = true;
await Promise.all(workers);
sample();
clearInterval(timer);
sock.destroy(); server.close();
process.exit(0);
