// MessageStream.js
import { EventEmitter } from 'events';

// CRC32 查找表
const CRC32_TABLE = new Uint32Array(256);
(() => {
    for (let i = 0; i < 256; i++) {
        let c = i;
        for (let j = 0; j < 8; j++) {
            c = (c & 1) ? 0xEDB88320 ^ (c >>> 1) : c >>> 1;
        }
        CRC32_TABLE[i] = c;
    }
})();

/**
 * 计算 Buffer 的 CRC32 校验值
 * @param {Buffer} buffer 
 * @returns {number} CRC32 校验值
 */
function crc32(buffer) {
    let crc = 0xFFFFFFFF;
    for (let i = 0; i < buffer.length; i++) {
        const byte = buffer[i];
        crc = CRC32_TABLE[(crc ^ byte) & 0xFF] ^ (crc >>> 8);
    }
    return (crc ^ 0xFFFFFFFF) >>> 0; // 确保返回无符号32位整数
}

export class MessageStream extends EventEmitter {
    constructor(options) {
        if (!options || !options.socket) throw new Error('socket is undefined');
        super();
        this.buffer = Buffer.alloc(0);
        this.expectedLength = null;

        // 模式控制
        this.protocolMode = false; // false=正常模式, true=协议模式
        this.protocolHeader = Buffer.from([0xFF, 0xFF, 0xFF, 0xFF]); // 协议模式标识头

        // 新增：最大缓冲区大小，默认 256MB
        this.maxBufferSize = options.maxBufferSize || 256 * 1024 * 1024; // 256MB
        this.socket = options.socket;

        this.socket.on('data', (data) => {
            // console.log('data', data);
            this.onData(data);
        });

        // 新增：是否启用 CRC 校验
        this.enableCRC = options.enableCRC !== false; // 默认启用

        // 新增：发送队列
        this.sendQueue = Promise.resolve();
        this.abortController = new AbortController(); // Node.js 15+ 支持

        // 定义错误消息标识符 (4字节)
        this.errorHeader = Buffer.from([0xEE, 0xEE, 0xEE, 0xEE]);
        // 定义错误类型映射
        this.errorTypes = {
            'LENGTH_CRC_FAILED': 1,
            'MESSAGE_CRC_FAILED': 2,
            'BUFFER_OVERFLOW': 3
        };
        // 反向映射
        this.errorTypeCodes = {
            1: 'LENGTH_CRC_FAILED',
            2: 'MESSAGE_CRC_FAILED',
            3: 'BUFFER_OVERFLOW'
        };
    }

    // 测试方法：模拟 CRC 错误
    async simulateCRCErrors() {
        console.log('[Test] Simulating CRC errors');
        // 发送一个错误消息来测试处理流程
        await this.sendError('TEST_ERROR');
        this.reset();
        // 等待一段时间观察行为
        await new Promise(resolve => setTimeout(resolve, 1000));
        console.log('[Test] CRC error simulation complete');
    }

    // 获取当前状态用于调试
    getStatus() {
        return {
            bufferLength: this.buffer?.length || 0,
            expectedLength: this.expectedLength,
            sendQueuePending: this.sendQueue !== Promise.resolve(),
            socketReadyState: this.socket?.readyState,
            enableCRC: this.enableCRC
        };
    }

    // 重置方法
    reset() {
        console.log('[MessageStream] Resetting stream state');
        this.buffer = Buffer.alloc(0);
        this.expectedLength = null;
        this.protocolMode = true; // 进入协议模式

        // 取消所有正在进行的操作
        this.abortController.abort();
        this.abortController = new AbortController();

        // 创建新的队列
        this.sendQueue = Promise.resolve();

        console.log('[MessageStream] Stream state reset complete');
    }

    // 处理协议消息
    handleProtocolMessage(messageData) {
        if (messageData.length > 50) return false;

        const messageString = messageData.toString('utf8').trim();

        switch (messageString) {
            case 'RESET_CONFIRM':
                console.log('Received reset confirmation');
                this.protocolMode = false; // 切换回正常模式
                // 安全起见，清空缓冲区并重置状态
                this.buffer = Buffer.alloc(0);
                this.expectedLength = null;
                this.emit('syncComplete');
                return true;
        }
        return false;
    }

    // 发送错误消息 - 使用二进制格式
    async sendError(errorType) {
        // 错误消息格式: [错误标识头(4字节)][错误类型(1字节)]
        const errorTypeCode = this.errorTypes[errorType] || 0;
        const errorTypeBuffer = Buffer.alloc(1);
        errorTypeBuffer.writeUInt8(errorTypeCode, 0);

        const errorData = Buffer.concat([this.errorHeader, errorTypeBuffer]);

        try {
            // 使用正常的消息发送方式发送错误信息
            await this._sendInternal(this.socket, errorData, 128);
        } catch (err) {
            console.error('Failed to send error packet:', err);
        }
    }

    // 发送协议消息（带协议头标识）
    async sendProtocolMessage(message) {
        const messageData = Buffer.from(message);
        const lengthHeader = Buffer.alloc(4);
        lengthHeader.writeUInt32LE(messageData.length, 0);

        // 协议消息格式: [协议标识头][长度头][数据]
        const protocolMessage = Buffer.concat([
            this.protocolHeader,
            lengthHeader,
            messageData
        ]);

        try {
            await this._sendDirect(protocolMessage, 128);
        } catch (err) {
            console.error(`Failed to send protocol message '${message}':`, err);
        }
    }

    // 添加一个直接发送方法，绕过所有检查
    async _sendDirect(data, chunkSize = 128) {
        if (!this.socket ||
            (this.socket.readyState !== 'open' && this.socket.writable !== true)) {
            throw new Error('Stream is not connected');
        }

        // 如果支持 cork/uncork，使用它来确保原子性
        this.socket.cork();
        try {
            return await this._sendDirectInternal(this.socket, data, chunkSize);
        } finally {
            this.socket.uncork();
        }
    }

    async _sendDirectInternal(stream, data, chunkSize = 128) {
        return new Promise((resolve, reject) => {
            let offset = 0;
            const totalLength = data.length;

            const sendNextChunk = () => {
                if (offset >= totalLength) {
                    resolve();
                    return;
                }

                const remaining = totalLength - offset;
                const size = Math.min(chunkSize, remaining);
                const chunk = data.subarray(offset, offset + size);

                if (stream.write(chunk)) {
                    offset += size;
                    setImmediate(sendNextChunk);
                } else {
                    stream.once('drain', () => {
                        offset += size;
                        sendNextChunk();
                    });
                }
            };

            sendNextChunk();
        });
    }

    /**
     * 实例方法：发送数据，包含 CRC 校验和队列管理
     * 发送数据，包含 CRC 校验
     * 格式: [长度头(4字节)][长度头CRC(可选4字节)][数据...][数据CRC(可选4字节)]
     * @param {Buffer} data 要发送的数据
     * @param {number} [chunkSize=65536] 分片大小，默认 64KB
     */
    send(data, chunkSize = 65536) {
        if (data.length > this.maxBufferSize) {
            return Promise.reject(new Error('Data size exceeds maximum allowed limit'));
        }

        const signal = this.abortController.signal;

        return new Promise((resolve, reject) => {
            // 监听取消信号
            if (signal.aborted) {
                reject(new Error('Send operation cancelled'));
                return;
            }

            const abortHandler = () => {
                signal.removeEventListener('abort', abortHandler);
                reject(new Error('Send operation cancelled'));
            };
            signal.addEventListener('abort', abortHandler);

            this.sendQueue = this.sendQueue.then(async () => {
                try {
                    // 再次检查是否已被取消
                    if (signal.aborted) {
                        throw new Error('Send operation cancelled');
                    }

                    if (!this.socket ||
                        (this.socket.readyState !== 'open' && this.socket.writable !== true)) {
                        throw new Error('Stream is not connected');
                    }
                    await this._sendInternal(this.socket, data, chunkSize);
                    resolve();
                } catch (error) {
                    reject(error);
                } finally {
                    signal.removeEventListener('abort', abortHandler);
                }
            }).catch(error => {
                signal.removeEventListener('abort', abortHandler);
                reject(error);
            });
        });
    }

    // 内部发送实现
    async _sendInternal(stream, data, chunkSize = 65536) {
        if (!stream || (stream.readyState !== 'open' && stream.writable !== true)) {
            throw new Error('Stream is not connected');
        }

        const totalLength = data.length;
        const enableCRC = this.enableCRC;

        // 使用 cork/uncork 确保消息原子性发送
        stream.cork();
        try {
            await this._sendData(stream, data, totalLength, chunkSize, enableCRC);
        } finally {
            stream.uncork();
        }
    }

    // 发送数据核心逻辑（使用 cork）
    async _sendData(stream, data, totalLength, chunkSize, enableCRC) {
        // 构造并发送长度头
        const lengthHeader = Buffer.alloc(4);
        lengthHeader.writeUInt32LE(totalLength, 0);
        stream.write(lengthHeader);

        // 如果启用 CRC，发送长度头的 CRC
        if (enableCRC) {
            const lengthCRC = crc32(lengthHeader);
            const lengthCRCBuffer = Buffer.alloc(4);
            lengthCRCBuffer.writeUInt32LE(lengthCRC, 0);
            stream.write(lengthCRCBuffer);
        }

        // 发送数据块
        let offset = 0;
        while (offset < totalLength) {
            const remaining = totalLength - offset;
            const size = Math.min(chunkSize, remaining);
            const chunk = data.subarray(offset, offset + size);
            stream.write(chunk);
            offset += size;
        }

        // 如果启用 CRC，发送数据的 CRC
        if (enableCRC) {
            const dataCRC = crc32(data);
            const dataCRCBuffer = Buffer.alloc(4);
            dataCRCBuffer.writeUInt32LE(dataCRC, 0);
            stream.write(dataCRCBuffer);
        }
    }

    // 修改 onData 方法
    onData(data) {
        this.buffer = Buffer.concat([this.buffer, data]);

        if (this.buffer.length > this.maxBufferSize) {
            this.emit('error', `Buffer size: ${this.buffer.length} exceeds maximum allowed limit`);
            this.sendError('BUFFER_OVERFLOW').catch(() => { });
            this.reset();
            return;
        }

        while (this.buffer.length > 0) {
            // 只有在协议模式下才检查协议消息
            if (this.protocolMode && this.buffer.length >= 8) {
                // 检查是否有协议标识头
                if (this.buffer.readUInt32LE(0) === this.protocolHeader.readUInt32LE(0)) {
                    // 有协议标识头，处理协议消息
                    const length = this.buffer.readUInt32LE(4);
                    const messageTotalSize = 8 + length;

                    if (this.buffer.length >= messageTotalSize) {
                        const messageData = Buffer.from(this.buffer.subarray(8, 8 + length));
                        this.buffer = Buffer.from(this.buffer.subarray(messageTotalSize));

                        if (this.handleProtocolMessage(messageData)) {
                            continue; // 继续处理剩余数据
                        }
                    } else {
                        break; // 等待更多数据
                    }
                } else {
                    // 在协议模式下但没有协议头，可能是残留数据，丢弃
                    console.log('[MessageStream] Dropping non-protocol data in protocol mode');
                    this.buffer = Buffer.alloc(0);
                    break;
                }
            }

            // 如果在协议模式下，不处理正常消息
            if (this.protocolMode) {
                // 等待协议同步完成
                break;
            }

            // 正常模式处理
            if (this.expectedLength === null) {
                const headerSize = this.enableCRC ? 8 : 4;

                if (this.buffer.length < headerSize) {
                    break;
                }

                const length = this.buffer.readUInt32LE(0);

                if (this.enableCRC) {
                    const expectedCRC = this.buffer.readUInt32LE(4);
                    const lengthBuffer = Buffer.alloc(4);
                    lengthBuffer.writeUInt32LE(length, 0);
                    const calculatedCRC = crc32(lengthBuffer);

                    if (calculatedCRC !== expectedCRC) {
                        console.error(`Length header CRC check failed: expected ${expectedCRC}, got ${calculatedCRC}`);
                        this.sendError('LENGTH_CRC_FAILED').catch(() => { });
                        this.emit('error', new Error('Length header CRC check failed'));
                        this.reset();
                        return;
                    }
                }

                this.expectedLength = length;
                this.buffer = Buffer.from(this.buffer.subarray(headerSize));
            }

            const messageTotalSize = this.enableCRC ? this.expectedLength + 4 : this.expectedLength;

            if (this.buffer.length >= messageTotalSize) {
                const messageData = Buffer.from(this.buffer.subarray(0, this.expectedLength));

                if (this.enableCRC) {
                    const messageCRC = this.buffer.readUInt32LE(this.expectedLength);
                    const calculatedCRC = crc32(messageData);

                    if (calculatedCRC !== messageCRC) {
                        console.error(`Message CRC check failed: expected ${messageCRC}, got ${calculatedCRC}`);
                        this.sendError('MESSAGE_CRC_FAILED').catch(() => { });
                        this.emit('error', new Error('Message CRC check failed'));
                        this.reset();
                        return;
                    }
                }

                this.buffer = Buffer.from(this.buffer.subarray(messageTotalSize));
                this.expectedLength = null;

                // 检查是否是错误消息（在发送给上层之前）
                if (this.isErrorPacket(messageData)) {
                    const errorTypeCode = messageData.readUInt8(4);
                    const errorTypeName = this.getErrorTypeName(errorTypeCode);

                    console.log('Received error packet:', errorTypeName);

                    // 收到错误消息时进入协议模式
                    this.reset();

                    // 发送确认消息，表示已准备好同步
                    this.sendProtocolMessage('RESET_CONFIRM').catch(err => {
                        console.error('Failed to send reset confirm:', err);
                    });

                    this.protocolMode = false;

                    // 不发送给上层应用，直接处理下一个消息
                    continue;
                }

                this.emit('message', messageData);
            } else {
                break;
            }
        }
    }

    // 检查是否是错误消息包
    isErrorPacket(data) {
        // 错误消息应该是5字节长，前4字节是错误标识符
        if (data.length === 5 && data.readUInt32LE(0) === 0xEEEEEEEE) {
            return true;
        }
        return false;
    }

    // 根据错误类型代码获取错误类型名称
    getErrorTypeName(errorTypeCode) {
        return this.errorTypeCodes[errorTypeCode] || 'UNKNOWN_ERROR';
    }

    /**
     * 销毁 MessageStream，清理所有资源
     */
    destroy() {
        // 清理缓冲区
        this.buffer = null;
        this.expectedLength = null;

        // 清理 socket 监听器
        if (this.socket) {
            this.socket.removeAllListeners('data');
            this.socket.end('EOF');
            this.socket.destroy();
            this.socket = null;
        }

        // 清理所有事件监听器
        this.removeAllListeners();

        // 清理发送队列
        this.sendQueue = null;

        console.log('[MessageStream] Destroyed');
    }
}

// 保持原有的 hexdump 函数不变
function hexdump(buffer, options = {}) {
    const {
        bytesPerLine = 16,
        showAscii = true,
        showOffset = true
    } = options;

    const bufferLength = buffer.length;
    let output = '';

    for (let i = 0; i < bufferLength; i += bytesPerLine) {
        let hexLine = '';
        let asciiLine = '';

        // 生成十六进制部分
        for (let j = 0; j < bytesPerLine; j++) {
            const index = i + j;
            if (index < bufferLength) {
                const byte = buffer[index];
                hexLine += byte.toString(16).padStart(2, '0') + ' ';
                asciiLine += (byte >= 32 && byte < 127) ? String.fromCharCode(byte) : '.';
            } else {
                hexLine += '   ';
                asciiLine += ' ';
            }
        }

        // 添加偏移量
        if (showOffset) {
            output += i.toString(16).padStart(8, '0') + '  ';
        }

        // 添加十六进制和 ASCII 部分
        output += hexLine;
        if (showAscii) {
            output += ' |' + asciiLine + '|';
        }
        output += '\n';
    }

    return output;
}