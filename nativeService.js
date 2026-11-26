// modules/nativeService.js — 修复版
import { spawn, exec } from 'child_process';
import { EventEmitter } from 'events';
import { MessageStream } from './ipc/MessageStream.js';
import { RpcProtocol } from './ipc/RpcProtocol.js';
import net from 'net';
import fs from 'fs';
import { randomUUID } from 'crypto';

export class NativeService extends EventEmitter {
    constructor(name, options) {
        super();
        const {
            cmd, args = [], env = {}, windowsHide = true,
            stdio = ['pipe', 'pipe', 'pipe'],
            // v1.1.2：默认鉴权 + 进程守护
            requireAuth = true,
            authToken = null,
            autoRestart = true,
            restartDelayMs = 500,
            maxRestartDelayMs = 30000,
            maxRestarts = Infinity,
            restartResetAfterMs = 30000,
            maxClients = 8,
            tokenGraceMs = 300000,          // 轮换后旧 token 的宽限期（5 分钟）
            heartbeatIntervalMs,
            heartbeatTimeoutMs,
            heartbeatRequireCapability = false,
        } = options;
        this.name = name;
        this.cmd = cmd;
        this.args = args;
        this.env = env;
        this.windowsHide = windowsHide;
        this.stdio = stdio;

        // v1.1.2：默认鉴权 —— 每实例生成独立 token，经环境变量交给子进程
        this.requireAuth = requireAuth;
        this.authToken = requireAuth ? (authToken || randomUUID().replace(/-/g, '')) : null;

        // v1.1.2：进程守护 —— 子进程异常退出后按指数退避重启
        this.autoRestart = autoRestart;
        this.restartDelayMs = restartDelayMs;
        this.maxRestartDelayMs = maxRestartDelayMs;
        this.maxRestarts = maxRestarts;
        this.restartResetAfterMs = restartResetAfterMs;
        this.heartbeatIntervalMs = heartbeatIntervalMs;
        this.heartbeatTimeoutMs = heartbeatTimeoutMs;
        this.heartbeatRequireCapability = heartbeatRequireCapability;
        this._restartTimer = null;
        this._stableTimer = null;
        this._restartAttempts = 0;
        this._intentionalStop = false;
        this._crashCount = 0;

        // v1.1.2：多客户端会话
        this.maxClients = maxClients;
        this.clients = new Map();          // clientId -> { id, socket, rpc, connectedAt, pid }
        this._clientSeq = 0;
        this._primaryId = null;            // 主会话：call/sendRequest 默认路由目标

        // v1.1.2：token 热轮换
        this.tokenGraceMs = tokenGraceMs;
        this.requiredTokens = new Set(this.authToken ? [this.authToken] : []);
        this._tokenGraceTimer = null;

        this.process = null;
        this.pipeServer = null;
        this.pipeName = options.pipeName || this.generatePipeName(this.name);
        this.rpcProtocol = null;
        this.handlers = {};

        this.isConnected = false;
        this._stopping = false;
        this._connectWaiters = [];

        // 兜底：避免上层没挂 error 监听时把进程打崩
        this.on('error', (err) => console.error(`[${this.name}] error:`, err?.code || '', err?.message || err));

        this.call = new Proxy({}, {
            get: (_target, method) => {
                return async (...args) => {
                    await this.waitForConnection();
                    if (!this.rpcProtocol) throw new RpcErrorLocal(`[${this.name}] RPC session lost`);
                    if (args.length > 0 && (Buffer.isBuffer(args[0]) || args[0] instanceof Uint8Array)) {
                        const buffer = Buffer.isBuffer(args[0]) ? args[0] : Buffer.from(args[0]);
                        return this.sendRequestBinary(method, buffer, ...args.slice(1));
                    }
                    return this.sendRequest(method, args);
                };
            }
        });

        this.startServer();
    }

    generatePipeName(baseName) {
        const pid = process.pid;
        const timestamp = Date.now();
        const randomId = Math.floor(Math.random() * 10000);
        if (process.platform === 'win32') {
            const sessionId = process.env.SESSIONNAME ? `_${process.env.SESSIONNAME}` : '';
            return `\\\\.\\pipe\\native_service_${baseName}${sessionId}_${pid}_${timestamp}_${randomId}`;
        }
        return `/tmp/native_service_${baseName}_${pid}_${timestamp}_${randomId}.sock`;
    }

    // ===== 连接等待 =====
    waitForConnection(timeout = 30000) {
        if (this.isConnected) return Promise.resolve();
        return new Promise((resolve, reject) => {
            const waiter = { resolve, reject, timer: null, onConnect: null, onError: null };
            const cleanup = () => {
                clearTimeout(waiter.timer);
                this.off('connect', waiter.onConnect);
                this.off('error', waiter.onError);
                this._connectWaiters = this._connectWaiters.filter(w => w !== waiter);
            };
            waiter.onConnect = () => { cleanup(); resolve(); };
            waiter.onError = (err) => { cleanup(); reject(err); };
            waiter.timer = setTimeout(() => {
                cleanup();
                reject(new RpcErrorLocal(`[${this.name}] connection timeout after ${timeout}ms`, 'ETIMEDOUT'));
            }, timeout);
            waiter.timer.unref?.();
            this.once('connect', waiter.onConnect);
            this.once('error', waiter.onError);
            this._connectWaiters.push(waiter);
        });
    }

    async waitForReady(timeout = 30000) {
        if (!this.process) this.start();
        await this.waitForConnection(timeout);
        return true;
    }

    registerMethod(name, handler) {
        if (!name || typeof name !== 'string') {
            console.error(`[${this.name}] Invalid method name: ${name}`);
            return;
        }
        if (typeof handler !== 'function') {
            console.error(`[${this.name}] Invalid handler for: ${name}`);
            return;
        }
        this.handlers[name] = handler;
        if (this.rpcProtocol) this.rpcProtocol.registerMethod(name, handler);
    }

    // ===== 发送（失败时给 Promise，而不是 undefined） =====
    sendRequest(method, params, to, timeoutMs) {
        if (!this.rpcProtocol) return Promise.reject(new RpcErrorLocal(`[${this.name}] RPC not initialized`, 'ENOTCONN'));
        if (!method || typeof method !== 'string') return Promise.reject(new RpcErrorLocal(`[${this.name}] invalid method`, 'EINVAL'));
        return this.rpcProtocol.sendRequest(method, params, to, timeoutMs);
    }

    /** 显式超时/二进制的调用入口，长任务用它包一层（svc.call.x 用默认 15s 超时） */
    async invoke(method, params = [], options = {}) {
        await this.waitForConnection();
        if (!this.rpcProtocol) throw new RpcErrorLocal(`[${this.name}] RPC session lost`);
        if (options.binary) {
            const buf = Buffer.isBuffer(params) ? params : Buffer.from(params);
            return this.rpcProtocol.sendRequestBinary(method, buf, [], options.timeout);
        }
        return this.rpcProtocol.sendRequest(method, params, undefined, options.timeout);
    }

    sendText(text) {
        if (!this.rpcProtocol) return Promise.reject(new RpcErrorLocal(`[${this.name}] RPC not initialized`, 'ENOTCONN'));
        if (typeof text !== 'string') return Promise.reject(new RpcErrorLocal(`[${this.name}] invalid text`, 'EINVAL'));
        return this.rpcProtocol.sendText(text);
    }

    sendJson(data) {
        if (!this.rpcProtocol) return Promise.reject(new RpcErrorLocal(`[${this.name}] RPC not initialized`, 'ENOTCONN'));
        if (typeof data !== 'object' || data === null) return Promise.reject(new RpcErrorLocal(`[${this.name}] invalid json`, 'EINVAL'));
        return this.rpcProtocol.sendJson(data);
    }

    sendBinary(data) {
        if (!this.rpcProtocol) return Promise.reject(new RpcErrorLocal(`[${this.name}] RPC not initialized`, 'ENOTCONN'));
        const buf = Buffer.isBuffer(data) ? data : (data instanceof Uint8Array ? Buffer.from(data) : null);
        if (!buf) return Promise.reject(new RpcErrorLocal(`[${this.name}] invalid binary data`, 'EINVAL'));
        return this.rpcProtocol.sendBinary(buf);
    }

    sendRequestBinary(method, buffer, ...args) {
        if (!this.rpcProtocol) return Promise.reject(new RpcErrorLocal(`[${this.name}] RPC not initialized`, 'ENOTCONN'));
        if (typeof method !== 'string') return Promise.reject(new RpcErrorLocal(`[${this.name}] invalid method`, 'EINVAL'));
        const buf = Buffer.isBuffer(buffer) ? buffer : (buffer instanceof Uint8Array ? Buffer.from(buffer) : null);
        if (!buf) return Promise.reject(new RpcErrorLocal(`[${this.name}] invalid buffer`, 'EINVAL'));
        return this.rpcProtocol.sendRequestBinary(method, buf, ...args);
    }

    // ===== 服务端 =====
    startServer() {
        if (this.pipeServer) return;

        if (process.platform !== 'win32' && fs.existsSync(this.pipeName)) {
            try { fs.unlinkSync(this.pipeName); } catch { }
        }

        const server = net.createServer((socket) => {
            // v1.1.2：多客户端会话（上限 maxClients），首个连入者为主会话
            if (this.clients.size >= this.maxClients) {
                console.warn(`[${this.name}] client limit (${this.maxClients}) reached, rejecting`);
                socket.destroy();
                return;
            }
            const clientId = `c${++this._clientSeq}`;

            socket.setNoDelay?.(true);
            const messageStream = new MessageStream({ socket });
            const rpc = new RpcProtocol(messageStream, {
                requiredTokens: [...this.requiredTokens],   // 默认鉴权；轮换期同时接受新旧 token
                heartbeatIntervalMs: this.heartbeatIntervalMs,
                heartbeatTimeoutMs: this.heartbeatTimeoutMs,
                heartbeatRequireCapability: this.heartbeatRequireCapability,
            });

            const entry = { id: clientId, socket, rpc, connectedAt: Date.now(), pid: null };
            this.clients.set(clientId, entry);
            if (!this._primaryId) {
                this._primaryId = clientId;
                this.rpcProtocol = rpc;
            }

            // 连接稳定运行 restartResetAfterMs 后才清零守护计数（否则连着崩也永远按 300ms 重启）
            if (this._stableTimer) clearTimeout(this._stableTimer);
            this._stableTimer = setTimeout(() => {
                this._stableTimer = null;
                this._restartAttempts = 0;
            }, this.restartResetAfterMs);
            this._stableTimer.unref?.();

            for (const [name, handler] of Object.entries(this.handlers)) rpc.registerMethod(name, handler);

            rpc.on('event', (message) => this.emit('event', message));
            rpc.on('error', (err) => this.emit('error', err));

            socket.on('close', () => this._removeClient(clientId, rpc));

            socket.on('error', (err) => {
                this.emit('error', err);
            });

            this.isConnected = true;
            this.emit('connect', socket);
            this.emit('clientConnected', { id: clientId, socket });

            rpc.sendText('hello from native service').catch(() => { });
        });

        server.listen(this.pipeName, () => {
            console.log(`[${this.name}] server listening on ${this.pipeName}`);
        });

        server.on('error', (err) => {
            console.error(`[${this.name}] pipe server error:`, err.message);
            this.emit('error', err);
        });

        this.pipeServer = server;
    }

    _removeClient(clientId, rpc) {
        const entry = this.clients.get(clientId);
        if (!entry || entry.rpc !== rpc) return;
        this.clients.delete(clientId);

        const wasPrimary = this._primaryId === clientId;
        if (wasPrimary) {
            this.rpcProtocol = null;
            this._primaryId = null;
            const next = this.clients.values().next().value;
            if (next) { this._primaryId = next.id; this.rpcProtocol = next.rpc; }
        }

        this.isConnected = this.clients.size > 0;
        try { rpc.destroy(); } catch { }
        this.emit('clientDisconnected', { id: clientId });
        if (wasPrimary && this._primaryId) this.emit('primaryChanged', { id: this._primaryId });
        if (this.clients.size === 0) this.emit('disconnect');
    }

    // ===== v1.1.2：多客户端会话 API =====

    /** 当前连接的客户端列表（不含 socket，避免上层误用） */
    getClients() {
        return [...this.clients.values()].map(c => ({
            id: c.id, pid: c.pid, connectedAt: c.connectedAt, primary: c.id === this._primaryId,
        }));
    }

    get clientCount() { return this.clients.size; }

    getPrimaryClientId() { return this._primaryId; }

    /** 路由到指定客户端；clientId 为 null 时用主会话 */
    sendRequestTo(clientId, method, params, to = undefined, timeoutMs = 15000) {
        const entry = clientId ? this.clients.get(clientId) : (this._primaryId ? this.clients.get(this._primaryId) : null);
        if (!entry) return Promise.reject(new RpcErrorLocal(`[${this.name}] client not connected: ${clientId}`, 'ENOTCONN'));
        return entry.rpc.sendRequest(method, params, to, timeoutMs);
    }

    /** 指定客户端的调用代理：service.callTo(clientId).foo(a, b) */
    callTo(clientId) {
        return new Proxy({}, {
            get: (_t, method) => async (...args) => {
                const entry = this.clients.get(clientId);
                if (!entry) throw new RpcErrorLocal(`[${this.name}] client not connected: ${clientId}`, 'ENOTCONN');
                if (args.length > 0 && (Buffer.isBuffer(args[0]) || args[0] instanceof Uint8Array)) {
                    const buf = Buffer.isBuffer(args[0]) ? args[0] : Buffer.from(args[0]);
                    return entry.rpc.sendRequestBinary(method, buf, ...args.slice(1));
                }
                return entry.rpc.sendRequest(method, args);
            },
        });
    }

    /** 向所有在线客户端广播一条文本事件 */
    broadcastText(text) {
        const results = [];
        for (const c of this.clients.values()) {
            results.push(c.rpc.sendText(text).then(() => true).catch(() => false));
        }
        return Promise.all(results).then(list => list.filter(Boolean).length);
    }

    // ===== v1.1.2：token 热轮换 =====

    /**
     * 生成新 token 并立即生效；旧 token 在 graceMs 内继续被接受（默认 5 分钟），之后吊销。
     * 已握手的会话不受影响（鉴权位是粘性的），只有新连接/重启的子进程需要新 token。
     */
    rotateToken({ graceMs = this.tokenGraceMs } = {}) {
        const oldToken = this.authToken;
        const newToken = randomUUID().replace(/-/g, '');
        this.authToken = newToken;
        this.requiredTokens.add(newToken);
        this._syncRequiredTokens();

        if (this._tokenGraceTimer) { clearTimeout(this._tokenGraceTimer); this._tokenGraceTimer = null; }
        if (oldToken) {
            if (graceMs > 0) {
                this._tokenGraceTimer = setTimeout(() => {
                    this._tokenGraceTimer = null;
                    this.revokeToken(oldToken);
                }, graceMs);
                this._tokenGraceTimer.unref?.();
            } else {
                this.revokeToken(oldToken);
            }
        }
        const payload = { token: newToken, oldTokenRevokedIn: graceMs };
        this.emit('tokenRotated', payload);
        return newToken;
    }

    /** 吊销指定 token（立刻生效） */
    revokeToken(token) {
        if (!token) return false;
        this.requiredTokens.delete(token);
        this._syncRequiredTokens();
        this.emit('tokenRevoked', { token });
        return true;
    }

    _syncRequiredTokens() {
        const tokens = [...this.requiredTokens];
        for (const c of this.clients.values()) c.rpc.setRequiredTokens(tokens);
    }

    cleanupRpcResources() {
        if (this._tokenGraceTimer) { clearTimeout(this._tokenGraceTimer); this._tokenGraceTimer = null; }
        for (const c of this.clients.values()) {
            try { c.rpc.destroy(); } catch { }
        }
        this.clients.clear();
        this._primaryId = null;
        this.isConnected = false;
        if (this.rpcProtocol) {
            try { this.rpcProtocol.destroy(); } catch { }
            this.rpcProtocol = null;
        }
    }

    stopServer() {
        if (!this.pipeServer) return;
        this.cleanupRpcResources();
        this.isConnected = false;
        const server = this.pipeServer;
        this.pipeServer = null;
        try {
            server.close(() => {
                if (process.platform !== 'win32' && fs.existsSync(this.pipeName)) {
                    try { fs.unlinkSync(this.pipeName); } catch { }
                }
            });
        } catch (err) {
            console.error(`[${this.name}] close server error:`, err.message);
        }
    }

    // ===== 子进程 =====
    start() {
        if (this.process) {
            console.warn(`[${this.name}] already running`);
            return false;
        }
        if (!this.pipeServer) this.startServer();

        this._intentionalStop = false;
        const childEnv = { ...process.env, ...this.env, PIPE_NAME: this.pipeName };
        if (this.authToken) childEnv.IPC_TOKEN = this.authToken;         // 子进程凭此 token 握手
        if (this.requireAuth) childEnv.IPC_REQUIRE_AUTH = '1';

        this.process = spawn(this.cmd, this.args, {
            env: childEnv,
            windowsHide: this.windowsHide,
            stdio: this.stdio,
        });

        if (this.process.stdout) this.process.stdout.on('data', d => console.log(`[${this.name} stdout]:`, d.toString().trimEnd()));
        if (this.process.stderr) this.process.stderr.on('data', d => console.error(`[${this.name} stderr]:`, d.toString().trimEnd()));

        this.process.on('error', (err) => this.emit('error', err));
        this.process.on('close', (code, signal) => {
            console.log(`[${this.name}] exited code=${code} signal=${signal}`);
            this.process = null;
            this.isConnected = false;
            if (this.rpcProtocol) {
                try { this.rpcProtocol.destroy(); } catch { }
                this.rpcProtocol = null;
            }
            const intentional = this._intentionalStop;
            if (intentional) { this._intentionalStop = false; return; }

            this._crashCount++;
            this.emit('crash', { code, signal, crashes: this._crashCount });
            this._scheduleRestart(code);
        });
        return true;
    }

    /** 守护：指数退避重启，超过 maxRestarts 后放弃并 emit 'give-up' */
    _scheduleRestart(exitCode) {
        if (!this.autoRestart || this._stopping) return;
        if (this._restartAttempts >= this.maxRestarts) {
            this.emit('give-up', { attempts: this._restartAttempts, exitCode });
            return;
        }
        this._restartAttempts++;
        const delay = Math.min(
            this.restartDelayMs * Math.pow(2, this._restartAttempts - 1),
            this.maxRestartDelayMs,
        );
        this.emit('restarting', { attempt: this._restartAttempts, delay, exitCode });
        this._restartTimer = setTimeout(() => {
            this._restartTimer = null;
            if (this._stopping) return;
            if (!this.pipeServer) this.startServer();
            if (this.start()) this.emit('restarted', { attempt: this._restartAttempts });
        }, delay);
        this._restartTimer.unref?.();
    }

    stop(timeout = 2000) {
        if (this._stopping) return false;
        if (!this.process) return false;
        this._stopping = true;
        this._intentionalStop = true;
        if (this._restartTimer) { clearTimeout(this._restartTimer); this._restartTimer = null; }
        this._restartAttempts = 0;

        const proc = this.process;
        const pid = proc.pid;
        this.cleanupRpcResources();
        this.isConnected = false;

        let finished = false;
        const finish = () => {
            if (finished) return;
            finished = true;
            this._stopping = false;
            this.process = null;
            this.stopServer();
            this.emit('stopped');
        };

        try { proc.kill('SIGTERM'); } catch { }

        const killTimer = setTimeout(() => {
            if (finished) return;
            try { proc.kill('SIGKILL'); } catch {
                if (process.platform === 'win32') exec(`taskkill /PID "${pid}" /T /F`, () => finish());
                else finish();
                return;
            }
            setTimeout(finish, 800);
        }, timeout);
        killTimer.unref?.();

        proc.once('exit', () => { clearTimeout(killTimer); finish(); });
        return true;
    }

    async restart(stopTimeout = 2000) {
        if (this.process) {
            const stopped = new Promise(r => this.once('stopped', r));
            this.stop(stopTimeout);
            await Promise.race([stopped, new Promise(r => setTimeout(r, stopTimeout + 1500))]);
        }
        if (!this.pipeServer) this.startServer();
        if (!this.start()) return false;
        try {
            await this.waitForConnection();
            return true;
        } catch (e) {
            console.error(`[${this.name}] restart connect failed:`, e.message);
            return false;
        }
    }

    getRpc() { return this.rpcProtocol; }

    getProcessStatus() {
        const status = {
            running: !!this.process,
            pid: this.process?.pid ?? null,
            isConnected: this.isConnected,
            crashes: this._crashCount,
            restarting: !!this._restartTimer,
            restartAttempts: this._restartAttempts,
            authRequired: this.requireAuth,
            clients: this.clients.size,
            primaryClient: this._primaryId,
            acceptedTokens: this.requiredTokens.size,
        };
        if (this.process) {
            status.killed = this.process.killed ?? false;
            if (this.process.exitCode !== null) status.exitCode = this.process.exitCode;
            if (this.process.signalCode !== null) status.signalCode = this.process.signalCode;
        }
        return status;
    }
}

class RpcErrorLocal extends Error {
    constructor(message, code = 'EIPC') {
        super(message);
        this.name = 'RpcError';
        this.code = code;
    }
}
