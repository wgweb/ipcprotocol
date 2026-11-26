// modules/ipc/RpcProtocol.js
import { v4 as uuidv4 } from 'uuid';
import { EventEmitter } from 'events';

export { uuidv4 }
export class RpcProtocol extends EventEmitter {
    constructor(messageStream) {
        if (!messageStream) throw new Error('messageStream is undefined');
        super();
        this.messageStream = messageStream;
        this.handlers = {};
        this.pendingRequests = new Map();

        this.messageStream.on('message', (buffer) => {
            this.handleMessage(buffer);
        });
        this.messageStream.on('error', (error) => {
            // console.error('[RpcProtocol] Error:', error);
            // 当消息流出错时，通知所有等待的请求
            this.handleStreamError(error);
            this.emit('error', error);
        });
    }

    // 处理消息流错误
    handleStreamError(error) {
        // 拒绝所有待处理的请求
        if (this.pendingRequests) {
            const errorMessage = error.message || 'Connection error';
            for (const [id, pending] of this.pendingRequests) {
                try {
                    pending.reject(new Error(`Request failed: ${errorMessage}`));
                } catch (err) {
                    console.error('[RpcProtocol] Error rejecting pending request:', err);
                }
            }
            this.pendingRequests.clear();
        }

        // 可以选择性地通知事件监听器连接已断开
        // this.emit('disconnected', error);
    }

    // ========== 协议定义 ==========
    static TYPES = {
        REQUEST: 0,
        RESPONSE: 1,
        EVENT: 2,
    };

    static CONTENT_TYPES = {
        TEXT: 0,
        JSON: 1,
        BINARY: 2,
    };

    // ========== 工具方法 ==========
    static serialize(type, contentType, id, data) {
        const header = Buffer.alloc(1 + 1 + 16); // type(1) + contentType(1) + id(16)
        header.writeUInt8(type, 0);
        header.writeUInt8(contentType, 1);
        const idBuffer = Buffer.from(id.replace(/-/g, ''), 'hex');
        idBuffer.copy(header, 2);

        let payload;
        if (contentType === RpcProtocol.CONTENT_TYPES.TEXT) {
            payload = Buffer.from(data, 'utf8');
        } else if (contentType === RpcProtocol.CONTENT_TYPES.JSON) {
            payload = Buffer.from(JSON.stringify(data), 'utf8');
        } else if (contentType === RpcProtocol.CONTENT_TYPES.BINARY) {
            payload = data; // data is already a Buffer
        } else {
            throw new Error(`Unsupported content type: ${contentType}`);
        }

        return Buffer.concat([header, payload]);
    }

    static deserialize(buffer) {
        try {
            const type = buffer.readUInt8(0);
            const contentType = buffer.readUInt8(1);
            const idBuffer = buffer.slice(2, 18);
            const id = idBuffer.toString('hex').replace(/(.{8})(.{4})(.{4})(.{4})(.*)/, '$1-$2-$3-$4-$5');

            const payload = buffer.slice(18);
            let content;
            if (contentType === RpcProtocol.CONTENT_TYPES.TEXT) {
                content = payload.toString('utf8');
            } else if (contentType === RpcProtocol.CONTENT_TYPES.JSON) {
                content = JSON.parse(payload.toString('utf8')); // 可能抛出异常
            } else if (contentType === RpcProtocol.CONTENT_TYPES.BINARY) {
                content = payload;
            }

            return { type, contentType, id, content };
        } catch (error) {
            // 处理反序列化错误
            throw new Error(`Failed to deserialize message: ${error.message}`);
        }
    }

    /**
     * 将请求数据编码为二进制格式
     * 格式: [methodLength][method][paramsLength][params][binaryData]
     * @param {string} method - 方法名
     * @param {Array} params - 参数数组（除了二进制数据外的其他参数）
     * @param {Buffer|Uint8Array} binaryData - 二进制数据
     * @returns {Buffer} 编码后的数据
     */
    static encodeBinaryRequest(method, params, binaryData) {
        const methodBuffer = Buffer.from(method, 'utf8');
        const methodLength = methodBuffer.length;

        const paramsJson = JSON.stringify(params);
        const paramsBuffer = Buffer.from(paramsJson, 'utf8');
        const paramsLength = paramsBuffer.length;

        // 创建完整的二进制请求
        const totalLength = 4 + methodLength + 4 + paramsLength + binaryData.length;
        const result = Buffer.alloc(totalLength);

        let offset = 0;

        // 写入方法名长度和方法名
        result.writeUInt32BE(methodLength, offset);
        offset += 4;
        methodBuffer.copy(result, offset);
        offset += methodLength;

        // 写入参数长度和参数
        result.writeUInt32BE(paramsLength, offset);
        offset += 4;
        paramsBuffer.copy(result, offset);
        offset += paramsLength;

        // 写入二进制数据
        Buffer.from(binaryData).copy(result, offset);

        return result;
    }

    /**
     * 从二进制数据解码请求
     * @param {Buffer} buffer - 二进制数据
     * @returns {Object} 解码后的请求对象
     */
    static decodeBinaryRequest(buffer) {
        let offset = 0;

        // 读取方法名长度
        const methodLength = buffer.readUInt32BE(offset);
        offset += 4;

        // 读取方法名
        const method = buffer.toString('utf8', offset, offset + methodLength);
        offset += methodLength;

        // 读取参数长度
        const paramsLength = buffer.readUInt32BE(offset);
        offset += 4;

        // 读取参数
        const paramsJson = buffer.toString('utf8', offset, offset + paramsLength);
        const params = JSON.parse(paramsJson);
        offset += paramsLength;

        // 剩余部分为二进制数据
        const binaryData = buffer.slice(offset);

        return {
            method,
            params,
            binaryData
        };
    }

    // 修改 handleRequest 方法以支持二进制协议
    async handleRequest(id, request) {
        // 检查 request 是否为 Buffer，如果是则使用二进制协议解码
        let method, params, args;

        if (Buffer.isBuffer(request)) {
            // 使用二进制协议解码
            const decoded = RpcProtocol.decodeBinaryRequest(request);
            method = decoded.method;
            params = decoded.params;
            // 将二进制数据作为第一个参数添加到参数列表
            args = [decoded.binaryData, ...params];
        } else {
            // 使用现有的 JSON 协议
            method = request.method;
            params = request.params;

            if (Array.isArray(params)) {
                args = [...params];
            } else if (params === null || params === undefined) {
                args = [];
            } else {
                args = [params];
            }
        }

        const handler = this.handlers[method];
        if (!handler) {
            this.sendResponse(id, { error: `Method not found: ${method}` }, RpcProtocol.CONTENT_TYPES.JSON);
            return;
        }

        try {
            const result = await handler(...args);

            let contentType = RpcProtocol.CONTENT_TYPES.JSON;
            let responseData = result;

            if (result === undefined) {
                // 处理 undefined 返回值
                responseData = null; // 或者其他合适的默认值
                contentType = RpcProtocol.CONTENT_TYPES.JSON;
            } else if (typeof result === 'string') {
                contentType = RpcProtocol.CONTENT_TYPES.TEXT;
            } else if (Buffer.isBuffer(result)) {
                contentType = RpcProtocol.CONTENT_TYPES.BINARY;
            }
            // console.log('[IPC] Sending response:', id, result, contentType);
            this.sendResponse(id, responseData, contentType);
        } catch (err) {
            this.sendResponse(id, { error: err.message }, RpcProtocol.CONTENT_TYPES.JSON);
        }
    }

    // ========== 核心方法 ==========
    handleMessage(buffer) {
        try {
            const { type, id, content, contentType } = RpcProtocol.deserialize(buffer);

            // 验证协议字段
            if (!Object.values(RpcProtocol.TYPES).includes(type)) {
                throw new Error(`Invalid message type: ${type}`);
            }

            if (!Object.values(RpcProtocol.CONTENT_TYPES).includes(contentType)) {
                throw new Error(`Invalid content type: ${contentType}`);
            }

            // 处理消息...
            if (type === RpcProtocol.TYPES.REQUEST) {
                // console.log('[IPC] Received request:', content);
                this.handleRequest(id, content);
            } else if (type === RpcProtocol.TYPES.RESPONSE) {
                // console.log('[IPC] Received response:', content);
                this.handleResponse(id, content);
            } else if (type === RpcProtocol.TYPES.EVENT) {
                // console.log('[IPC] Received event:', content);
                this.emit('event', { contentType, content });
            }
        } catch (error) {
            console.error('Failed to handle message:', error.message);
            // 可以选择忽略错误消息或发送错误响应
        }
    }

    handleResponse(id, response) {
        const pending = this.pendingRequests.get(id);
        if (pending) {
            if (response && response.error) {
                // pending.reject(new Error(response.error));
                // 在开发环境中记录详细错误信息
                // if (process.env.NODE_ENV !== 'production') {
                //     console.error('[IPC] Received error response:', response);
                // }
                pending.reject(response.error); // 直接传递完整的响应结构
            } else {
                pending.resolve(response);
            }
            this.pendingRequests.delete(id);
        }
    }

    sendRaw(type, contentType, id, data) {
        if (!this.messageStream) {
            throw new Error('messageStream is not initialized');
        }
        const message = RpcProtocol.serialize(type, contentType, id, data);
        // console.log('[IPC] Sending message:', message);
        // 使用实例方法发送
        return this.messageStream.send(message);
    }

    sendRequest(method, params, to = undefined) {
        // console.log('[IPC] Sending request:', method, params);
        const id = uuidv4();
        return new Promise((resolve, reject) => {
            this.pendingRequests.set(id, { resolve, reject });
            this.sendRaw(RpcProtocol.TYPES.REQUEST, RpcProtocol.CONTENT_TYPES.JSON, id, {
                method,
                params,
                to,
            });
        });
    }

    sendResponse(id, response, contentType = RpcProtocol.CONTENT_TYPES.JSON) {
        this.sendRaw(RpcProtocol.TYPES.RESPONSE, contentType, id, response);
    }

    onEvent(listener) {
        this.on('event', listener);
    }

    registerMethod(name, handler) {
        this.handlers[name] = handler;
    }

    // ========== 辅助方法 ==========
    sendText(text) {
        this.sendRaw(RpcProtocol.TYPES.EVENT, RpcProtocol.CONTENT_TYPES.TEXT, uuidv4(), text);
    }

    sendBinary(data) {
        this.sendRaw(RpcProtocol.TYPES.EVENT, RpcProtocol.CONTENT_TYPES.BINARY, uuidv4(), data);
    }

    sendJson(data) {
        this.sendRaw(RpcProtocol.TYPES.EVENT, RpcProtocol.CONTENT_TYPES.JSON, uuidv4(), data);
    }

    /**
    * 发送二进制数据请求并等待响应
    * @param {string} method - 调用的方法名
    * @param {Buffer|Uint8Array} buffer - 要发送的二进制数据
    * @param {...any} args - 其他参数
    * @returns {Promise<any>} 服务端响应结果
    */
    sendRequestBinary(method, buffer, ...args) {
        const id = uuidv4();
        return new Promise((resolve, reject) => {
            this.pendingRequests.set(id, { resolve, reject });

            // 使用自定义的二进制协议编码
            const binaryRequest = RpcProtocol.encodeBinaryRequest(method, args, buffer);
            this.sendRaw(RpcProtocol.TYPES.REQUEST, RpcProtocol.CONTENT_TYPES.BINARY, id, binaryRequest);
        });
    }

    /**
     * 销毁 RpcProtocol，清理所有资源
     */
    destroy() {
        // 拒绝所有待处理的请求
        if (this.pendingRequests) {
            for (const [id, pending] of this.pendingRequests) {
                try {
                    pending.reject(new Error('RPC Protocol destroyed'));
                } catch (err) {
                    console.error('Error rejecting pending request:', err);
                }
            }
            this.pendingRequests.clear();
            this.pendingRequests = null;
        }

        // 清理消息流
        if (this.messageStream) {
            this.messageStream.destroy();
            this.messageStream = null;
        }

        // 清理处理器
        this.handlers = null;

        // 清理所有事件监听器
        this.removeAllListeners();

        console.log('[RpcProtocol] Destroyed');
    }
}