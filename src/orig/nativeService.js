// modules/nativeService.js
import { spawn, exec } from 'child_process';
import { EventEmitter } from 'events';
import { MessageStream } from './ipc/MessageStream.js';
import { RpcProtocol } from './ipc/RpcProtocol.js';
import net from 'net';

export class NativeService extends EventEmitter {
    constructor(name, options) {
        super();
        const { cmd, args = [], env = {}, windowsHide = true } = options;
        this.name = name;
        this.cmd = cmd;
        this.args = args;
        this.env = env;
        this.windowsHide = windowsHide;

        this.process = null;
        this.pipeServer = null;
        // 在构造函数中替换原来的pipeName赋值
        this.pipeName = options.pipeName || this.generatePipeName(this.name);
        this.rpcProtocol = null;
        this.handlers = {};

        // 添加连接状态
        this.isConnected = false;
        this.connectionPromise = null;
        this.connectionResolve = null;
        this.connectionReject = null;

        this.startServer(); // 启动命名管道服务端

        // 修改构造函数中的 this.call 代理对象定义
        this.call = new Proxy({}, {
            get: (_target, method) => {
                return async (...args) => {
                    // 等待连接就绪
                    await this.waitForConnection();

                    // 检查第一个参数是否为 Buffer 或 Uint8Array
                    if (args.length > 0) {
                        if (Buffer.isBuffer(args[0])) {
                            // 如果第一个参数是 Buffer，则使用 sendRequestBinary
                            const buffer = args[0];
                            const restArgs = args.slice(1);
                            return await this.sendRequestBinary(method, buffer, ...restArgs);
                        } else if (args[0] instanceof Uint8Array) {
                            // 如果第一个参数是 Uint8Array，转换为 Buffer
                            const buffer = Buffer.from(args[0]);
                            const restArgs = args.slice(1);
                            return await this.sendRequestBinary(method, buffer, ...restArgs);
                        }
                    }

                    // 否则使用普通的 sendRequest
                    return await this.sendRequest(method, args);
                };
            }
        });
    }

    generatePipeName(baseName) {
        const pid = process.pid;
        const timestamp = Date.now();
        const randomId = Math.floor(Math.random() * 10000);

        if (process.platform === 'win32') {
            // Windows 命名管道
            const sessionId = process.env.SESSIONNAME ? `_${process.env.SESSIONNAME}` : '';
            return `\\\\.\\pipe\\native_service_${baseName}${sessionId}_${pid}_${timestamp}_${randomId}`;
        } else {
            // Unix/Linux 使用临时目录下的 socket 文件
            return `/tmp/native_service_${baseName}_${pid}_${timestamp}_${randomId}.sock`;
        }
    }

    // 等待连接就绪
    waitForConnection(timeout = 30000) {
        if (this.isConnected) {
            return Promise.resolve();
        }

        return new Promise((resolve, reject) => {
            const timeoutId = setTimeout(() => {
                reject(new Error(`[${this.name}] Connection timeout after ${timeout}ms`));
            }, timeout);

            const onConnect = () => {
                clearTimeout(timeoutId);
                this.removeListener('connect', onConnect);
                this.removeListener('error', onError);
                resolve();
            };

            const onError = (error) => {
                clearTimeout(timeoutId);
                this.removeListener('connect', onConnect);
                this.removeListener('error', onError);
                reject(error);
            };

            this.once('connect', onConnect);
            this.once('error', onError);
        });
    }

    // 等待服务完全启动并连接
    async waitForReady(timeout = 30000) {
        // 启动服务
        if (!this.process) {
            this.start();
        }

        // 等待连接
        await this.waitForConnection(timeout);
        return true;
    }

    registerMethod(name, handler) {
        if (!name || typeof name !== 'string') {
            console.error(`[${this.name}] Invalid method name provided: ${name}`);
            return;
        }
        if (typeof handler !== 'function') {
            console.error(`[${this.name}] Invalid handler function provided for method: ${name}`);
            return;
        }

        // 无论如何都保存方法引用
        this.handlers[name] = handler;

        // 如果 RPC 协议已存在，立即注册
        if (this.rpcProtocol) {
            this.rpcProtocol.registerMethod(name, handler);
        }

        console.log(`[${this.name}] Method registered: ${name}`);
    }

    sendRequest(method, params, to) {
        if (!this.rpcProtocol) {
            console.error(`[${this.name}] RPC protocol is not initialized.`);
            return;
        }
        if (!method || typeof method !== 'string') {
            console.error(`[${this.name}] Invalid method name provided: ${method}`);
            return;
        }
        return this.rpcProtocol.sendRequest(method, params, to);
    }

    sendText(text) {
        if (!this.rpcProtocol) {
            console.error(`[${this.name}] RPC protocol is not initialized.`);
            return;
        }
        if (typeof text !== 'string') {
            console.error(`[${this.name}] Invalid text provided: ${text}`);
            return;
        }
        this.rpcProtocol.sendText(text);
    }

    sendJson(data) {
        if (!this.rpcProtocol) {
            console.error(`[${this.name}] RPC protocol is not initialized.`);
            return;
        }
        if (typeof data !== 'object' || data === null) {
            console.error(`[${this.name}] Invalid JSON data provided: ${data}`);
            return;
        }
        this.rpcProtocol.sendJson(data);
    }

    sendBinary(data) {
        if (!this.rpcProtocol) {
            console.error(`[${this.name}] RPC protocol is not initialized.`);
            return;
        }

        // 如果传入的是 Uint8Array，转换为 Buffer
        if (data instanceof Uint8Array && !Buffer.isBuffer(data)) {
            data = Buffer.from(data);
        }

        if (!(data instanceof Buffer)) {
            console.error(`[${this.name}] Invalid binary data provided: ${data}`);
            return;
        }
        this.rpcProtocol.sendBinary(data);
    }

    sendRequestBinary(method, buffer, ...args) {
        if (!this.rpcProtocol) {
            console.error(`[${this.name}] RPC protocol is not initialized.`);
            return;
        }
        if (typeof method !== 'string') {
            console.error(`[${this.name}] Invalid method provided: ${method}`);
            return;
        }

        // 如果传入的是 Uint8Array，转换为 Buffer
        if (buffer instanceof Uint8Array && !Buffer.isBuffer(buffer)) {
            buffer = Buffer.from(buffer);
        }

        if (!Buffer.isBuffer(buffer)) {
            console.error(`[${this.name}] Invalid buffer provided: ${buffer}`);
            return;
        }
        return this.rpcProtocol.sendRequestBinary(method, buffer, ...args);
    }

    startServer() {
        // 防止重复调用
        if (this.pipeServer) {
            console.warn(`[${this.name}] Pipe server is already running.`);
            return;
        }

        // 创建新的命名管道服务端
        const server = net.createServer((socket) => {
            console.log(`[${this.name}] Client connected`);

            // 创建 MessageStream 和 RpcProtocol
            const messageStream = new MessageStream({ socket });
            this.rpcProtocol = new RpcProtocol(messageStream); // 注意：只传入 messageStream

            // 注册所有已定义的处理器（不再清空 handlers）
            for (const [name, handler] of Object.entries(this.handlers)) {
                this.rpcProtocol.registerMethod(name, handler);
            }

            // 监听连接关闭事件
            socket.on('close', () => {
                console.log(`[${this.name}] Client connection closed`);
                this.isConnected = false;
                this.cleanupRpcResources();
                this.emit('disconnect');
            });

            socket.on('error', (err) => {
                console.error(`[${this.name}] Client connection error:`, err.message);
                this.isConnected = false;
                this.emit('error', err);
            });

            this.rpcProtocol.on('event', (message) => {
                this.emit('event', message);
            });
            this.rpcProtocol.on('error', error => {
                console.error(`[${this.name}] RPC error:`, error);
                this.emit('error', error);
            });

            // 设置连接状态
            this.isConnected = true;
            this.emit('connect', socket);

            this.sendText('hello from native service');
        });

        // 绑定到命名管道
        server.listen(this.pipeName, () => {
            console.log(`[${this.name}] Named pipe server is listening on ${this.pipeName}`);
        });

        // 错误监听
        server.on('error', (err) => {
            console.error(`[${this.name}] Pipe server error:`, err.message);
            this.emit('error', err);
        });

        // 保存 server 实例
        this.pipeServer = server;
    }

    /**
     * 销毁 RPC 相关资源
     */
    cleanupRpcResources() {
        if (this.rpcProtocol) {
            console.log(`[${this.name}] Cleaning up RPC resources...`);
            try {
                this.rpcProtocol.destroy();
            } catch (err) {
                console.error(`[${this.name}] Error destroying RPC protocol:`, err);
            }
            this.rpcProtocol = null;
            console.log(`[${this.name}] RPC resources cleaned up`);
        }
    }

    /**
     * 停止管道服务器
     */
    stopServer() {
        if (!this.pipeServer) {
            console.warn(`[${this.name}] No pipe server to stop.`);
            return;
        }

        // 先清理 RPC 资源
        this.cleanupRpcResources();
        this.isConnected = false;

        // 关闭管道服务器
        try {
            this.pipeServer.close(() => {
                console.log(`[${this.name}] Pipe server stopped.`);
            });
        } catch (err) {
            console.error(`[${this.name}] Error closing pipe server:`, err);
        }

        this.pipeServer = null;
    }

    start() {
        if (this.process) {
            console.warn(`[${this.name}] Already running.`);
            return false;
        }

        this.process = spawn(this.cmd, this.args, {
            env: {
                ...process.env,
                ...this.env,
                PIPE_NAME: this.pipeName
            },
            windowsHide: this.windowsHide,
            stdio: ['pipe', 'pipe', 'pipe'],
        });

        this.process.stdout.on('data', (data) => {
            console.log(`[${this.name} stdout]:`, data.toString());
        });

        this.process.stderr.on('data', (data) => {
            console.error(`[${this.name} stderr]:`, data.toString());
        });

        this.process.on('close', (code) => {
            console.log(`[${this.name}] Exited with code: ${code}`);
            this.process = null;
            this.isConnected = false;
        });

        return true;
    }

    /**
     * 停止服务
     */
    stop(timeout = 2000) {
        if (!this.process) {
            console.warn(`[${this.name}] No process to stop.`);
            return false;
        }

        const pid = this.process.pid;
        console.log(`[${this.name}] Stopping process ${pid}...`);

        // 先清理 RPC 资源
        this.cleanupRpcResources();
        this.isConnected = false;

        // 发送终止信号
        try {
            this.process.kill('SIGTERM');
        } catch (err) {
            console.error(`[${this.name}] Error sending SIGTERM to process ${pid}:`, err);
        }

        // 设置超时强制终止
        const killTimeout = setTimeout(() => {
            console.warn(`[${this.name}] Process ${pid} did not exit within timeout, forcing termination...`);

            // 尝试 SIGKILL
            try {
                this.process.kill('SIGKILL');
                console.log(`[${this.name}] SIGKILL sent to process ${pid}`);
            } catch (err) {
                console.error(`[${this.name}] Error sending SIGKILL to process ${pid}:`, err);

                // 最后的手段：使用 taskkill
                exec(`taskkill /PID "${pid}" /T /F`, (err, stdout, stderr) => {
                    if (err) {
                        console.error(`[${this.name}] Failed to terminate process ${pid}:`, err.message);
                        if (stderr) console.error(`[${this.name}] stderr:`, stderr);
                    } else {
                        console.log(`[${this.name}] Process ${pid} forcefully terminated`);
                        if (stdout) console.log(`[${this.name}] stdout:`, stdout);
                    }
                    this.finalizeStop();
                });
                return;
            }

            // 给 SIGKILL 一点时间生效
            setTimeout(() => {
                this.finalizeStop();
            }, 1000);

        }, timeout);

        // 监听正常退出
        this.process.once('exit', (code, signal) => {
            clearTimeout(killTimeout);
            console.log(`[${this.name}] Process ${pid} exited with code: ${code}, signal: ${signal}`);
            this.finalizeStop();
        });

        return true;
    }

    /**
     * 最终停止处理
     */
    finalizeStop() {
        // 确保清理所有资源
        this.cleanupRpcResources();
        this.stopServer();
        this.isConnected = false;

        // 清理进程引用
        this.process = null;

        this.emit('stopped');
        console.log(`[${this.name}] Service stopped completely`);
    }

    /**
     * 重启子进程服务
     * @param {number} stopTimeout 停止超时时间（毫秒）
     * @returns {Promise<boolean>} 重启是否成功
     */
    async restart(stopTimeout = 2000) {
        // 先停止现有服务
        if (this.process) {
            await new Promise((resolve) => {
                this.once('stopped', resolve);
                this.stop(stopTimeout);
            });
        }

        // 重新初始化必要组件（如果需要）
        if (!this.pipeServer) {
            this.startServer();
        }

        // 启动新进程
        const started = this.start();

        // 等待连接建立
        if (started) {
            try {
                await this.waitForConnection();
                return true;
            } catch (error) {
                console.error(`[${this.name}] Failed to connect after restart:`, error.message);
                return false;
            }
        }

        return false;
    }

    getRpc() {
        return this.rpcProtocol;
    }

    /**
     * 获取子进程状态
     * @returns {Object} 进程状态对象
     */
    getProcessStatus() {
        const status = {
            running: !!this.process,
            pid: this.process ? this.process.pid : null,
            isConnected: this.isConnected
        };

        if (this.process) {
            status.killed = this.process.killed || false;

            // 检查退出码
            if (this.process.exitCode !== null) {
                status.exitCode = this.process.exitCode;
            }

            // 检查信号终止
            if (this.process.signalCode !== null) {
                status.signalCode = this.process.signalCode;
            }
        }

        return status;
    }
}