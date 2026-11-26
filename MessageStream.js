// MessageStream.js  — 修复版（线格式与 PROTOCOL.md 完全一致，仅内部实现重构）
// 帧格式(CRC 开启): [len u32LE][lenCrc u32LE][payload][payloadCrc u32LE]
// 帧格式(CRC 关闭): [len u32LE][payload]
// 协议模式消息:     [0xFFFFFFFF][len u32LE][utf8 data]
// 错误包(在正常帧内): [0xEEEEEEEE][code u8]
import { EventEmitter } from 'events';
import zlib from 'node:zlib';

const PROTOCOL_HEADER = Buffer.from([0xFF, 0xFF, 0xFF, 0xFF]);
const PROTOCOL_HEADER_U32 = 0xFFFFFFFF;
const ERROR_HEADER_U32 = 0xEEEEEEEE;

const CHUNK = 64 * 1024;
const MAX_PROTOCOL_MSG = 50;

export const ERROR_CODES = {
    LENGTH_CRC_FAILED: 1,
    MESSAGE_CRC_FAILED: 2,
    BUFFER_OVERFLOW: 3,
};
const ERROR_NAMES = { 1: 'LENGTH_CRC_FAILED', 2: 'MESSAGE_CRC_FAILED', 3: 'BUFFER_OVERFLOW' };

// CRC32（标准多项式，与 zlib / PROTOCOL.md 一致）
const CRC_TABLE = new Uint32Array(256);
for (let i = 0; i < 256; i++) {
    let c = i;
    for (let j = 0; j < 8; j++) c = (c & 1) ? (0xEDB88320 ^ (c >>> 1)) : (c >>> 1);
    CRC_TABLE[i] = c >>> 0;
}
function crc32Table(buf) {
    let crc = 0xFFFFFFFF;
    for (let i = 0; i < buf.length; i++) crc = CRC_TABLE[(crc ^ buf[i]) & 0xFF] ^ (crc >>> 8);
    return (crc ^ 0xFFFFFFFF) >>> 0;
}
const hasNativeCrc = typeof zlib.crc32 === 'function';
export function crc32(buf) {
    return (hasNativeCrc ? zlib.crc32(buf) : crc32Table(buf)) >>> 0;
}

function ipcError(message, code) {
    return Object.assign(new Error(message), { code });
}

export class MessageStream extends EventEmitter {
    constructor(options) {
        if (!options || !options.socket) throw new Error('socket is undefined');
        super();
        this.socket = options.socket;
        this.enableCRC = options.enableCRC !== false;
        this.maxBufferSize = options.maxBufferSize || 256 * 1024 * 1024;
        this.syncTimeout = options.syncTimeout ?? 3000;

        // 接收缓冲：Buffer 队列 + 头游标（不再整段 Buffer.concat，消除 O(n²)）
        this._q = [];
        this._qHead = 0;
        this._buffered = 0;

        this._expectedLength = null;
        this._protocolMode = false;
        this._closed = false;

        // 发送：单一串行队列 + 背压处理
        this._chain = Promise.resolve();
        this._syncDeferred = null;
        this._syncTimer = null;

        const onData = (d) => this._onData(d);
        const onEnd = () => this._failClosed('IPC stream ended');
        const onClose = () => this._failClosed('IPC stream closed', true);
        const onError = (err) => this._emitError(ipcError(`socket error: ${err?.message || err}`, 'ESOCKET'));

        this.socket.on('data', onData);
        this.socket.on('end', onEnd);
        this.socket.on('close', onClose);
        this.socket.on('error', onError);
        this._detach = () => {
            this.socket?.off('data', onData);
            this.socket?.off('end', onEnd);
            this.socket?.off('close', onClose);
            this.socket?.off('error', onError);
        };
    }

    // ===== 只读状态（兼容旧代码 + 测试） =====
    get protocolMode() { return this._protocolMode; }
    get expectedLength() { return this._expectedLength; }
    get buffered() { return this._buffered; }
    get buffer() { return { length: this._buffered }; }

    getStatus() {
        return {
            buffered: this._buffered,
            expectedLength: this._expectedLength,
            protocolMode: this._protocolMode,
            closed: this._closed,
            enableCRC: this.enableCRC,
            socketReadyState: this.socket?.readyState,
        };
    }

    _emitError(err) {
        if (this.listenerCount('error') > 0) this.emit('error', err);
        else console.error('[MessageStream] unhandled error:', err?.code || '', err?.message || err);
    }

    _failClosed(reason, closeEvent = false) {
        if (this._closed) return;
        this._closed = true;
        this._notifyClose(closeEvent);
        if (this._syncDeferred) { try { this._syncDeferred.reject(ipcError(reason, 'ECLOSED')); } catch { } this._syncDeferred = null; }
        this._clearSyncTimer();
        this._emitError(ipcError(reason, 'ECLOSED'));
    }

    /** 广播一次 close 事件，保证上层能结算在途请求 */
    _notifyClose(emitCloseEvent = true) {
        if (this._closeNotified) return;
        this._closeNotified = true;
        if (emitCloseEvent && this.listenerCount('close') > 0) {
            try { this.emit('close'); } catch { }
        }
    }

    // ===== 接收缓冲工具 =====
    _append(data) {
        this._q.push(data);
        this._buffered += data.length;
    }

    _copyRange(offset, len) {
        const out = Buffer.allocUnsafe(len);
        let qi = 0, pos = this._qHead, skip = offset;
        while (skip > 0) {
            const c = this._q[qi];
            const avail = c.length - pos;
            if (skip >= avail) { skip -= avail; qi++; pos = 0; } else { pos += skip; skip = 0; }
        }
        let filled = 0;
        while (filled < len) {
            const c = this._q[qi];
            const avail = c.length - pos;
            const take = Math.min(avail, len - filled);
            c.copy(out, filled, pos, pos + take);
            filled += take; pos += take;
            if (pos >= c.length) { qi++; pos = 0; }
        }
        return out;
    }

    _readU32(offset) { return this._copyRange(offset, 4).readUInt32LE(0); }

    _consume(n) {
        let left = n;
        while (left > 0) {
            const c = this._q[0];
            const avail = c.length - this._qHead;
            if (left >= avail) { left -= avail; this._q.shift(); this._qHead = 0; }
            else { this._qHead += left; left = 0; }
        }
        this._buffered -= n;
    }

    /** 取出 n 字节并消费；单块内零拷贝 */
    _take(n) {
        if (n === 0) return Buffer.alloc(0);
        const c = this._q[0];
        if (c.length - this._qHead >= n) {
            const out = c.subarray(this._qHead, this._qHead + n);
            this._qHead += n;
            this._buffered -= n;
            if (this._qHead === c.length) { this._q.shift(); this._qHead = 0; }
            return out;
        }
        const out = this._copyRange(0, n);
        this._consume(n);
        return out;
    }

    _clearBuffer() {
        this._q = [];
        this._qHead = 0;
        this._buffered = 0;
    }

    _findHeader() {
        const window = Math.min(this._buffered, 64 * 1024);
        if (window < 4) return -1;
        return this._copyRange(0, window).indexOf(PROTOCOL_HEADER);
    }

    // ===== 接收主循环 =====
    _onData(data) {
        if (this._closed) return;
        this._append(data);
        if (this._buffered > this.maxBufferSize + 16) {
            this._sendError(ERROR_CODES.BUFFER_OVERFLOW).catch(() => { });
            this._emitError(ipcError(`buffer ${this._buffered} exceeds limit`, 'BUFFER_OVERFLOW'));
            this._enterProtocolMode();
            return;
        }
        try {
            this._process();
        } catch (err) {
            this._emitError(ipcError(`process error: ${err.message}`, 'EPARSE'));
        }
    }

    _process() {
        while (true) {
            if (this._protocolMode) {
                if (this._buffered < 8) return;
                if (this._readU32(0) !== PROTOCOL_HEADER_U32) {
                    const idx = this._findHeader();
                    if (idx < 0) {
                        const drop = Math.max(0, this._buffered - 3);
                        if (drop > 0) this._consume(drop);
                        return;
                    }
                    this._consume(idx);
                    continue;
                }
                const len = this._readU32(4);
                if (len > MAX_PROTOCOL_MSG || len > this.maxBufferSize) { this._consume(8); continue; }
                if (this._buffered < 8 + len) return;
                this._consume(8);
                const text = this._take(len).toString('utf8').trim();
                this._handleProtocolMessage(text);
                continue;
            }

            if (this._expectedLength === null) {
                const hdr = this.enableCRC ? 8 : 4;
                if (this._buffered < hdr) return;
                const length = this._readU32(0);

                // ★ 关键修复：长度必须校验，否则一条假帧会让链路静默等 4GB
                if (length === 0 || length > this.maxBufferSize) {
                    this._sendError(length > this.maxBufferSize ? ERROR_CODES.BUFFER_OVERFLOW : ERROR_CODES.LENGTH_CRC_FAILED).catch(() => { });
                    this._emitError(ipcError(`invalid frame length ${length}`, 'BAD_LENGTH'));
                    this._enterProtocolMode();
                    return;
                }

                if (this.enableCRC) {
                    const expected = this._readU32(4);
                    const lenBuf = Buffer.allocUnsafe(4);
                    lenBuf.writeUInt32LE(length, 0);
                    if (crc32(lenBuf) !== expected) {
                        this._sendError(ERROR_CODES.LENGTH_CRC_FAILED).catch(() => { });
                        this._emitError(ipcError('length header CRC check failed', 'LENGTH_CRC_FAILED'));
                        this._enterProtocolMode();
                        return;
                    }
                }

                this._expectedLength = length;
                this._consume(hdr);
            }

            const total = this._expectedLength + (this.enableCRC ? 4 : 0);
            if (this._buffered < total) return;

            const payload = this._take(this._expectedLength);

            if (this.enableCRC) {
                const expected = this._readU32(0);
                this._consume(4);
                if (crc32(payload) !== expected) {
                    this._expectedLength = null;
                    this._sendError(ERROR_CODES.MESSAGE_CRC_FAILED).catch(() => { });
                    this._emitError(ipcError('message CRC check failed', 'MESSAGE_CRC_FAILED'));
                    this._enterProtocolMode();
                    return;
                }
            }
            this._expectedLength = null;

            if (this._isErrorPacket(payload)) {
                this._onPeerError(payload[4]);
                continue;
            }
            this.emit('message', payload);
        }
    }

    _isErrorPacket(buf) {
        return buf.length === 5 && buf.readUInt32LE(0) === ERROR_HEADER_U32;
    }

    // ===== 错误 / 重同步 =====
    _enterProtocolMode() {
        this._clearBuffer();
        this._expectedLength = null;
        this._protocolMode = true;

        this._clearSyncTimer();
        let resolveFn, rejectFn;
        const promise = new Promise((res, rej) => { resolveFn = res; rejectFn = rej; });
        promise.catch(() => { });  // 防止未处理 rejection
        this._syncDeferred = { promise, resolve: resolveFn, reject: rejectFn };

        // 防御：重同步握手失败不再无限等待，直接断链让上层重连
        this._syncTimer = setTimeout(() => {
            if (!this._protocolMode) return;
            this._emitError(ipcError('resync handshake timeout', 'ERESYNC_TIMEOUT'));
            this._protocolMode = false;
            this._resolveSync();
            try { this.socket?.destroy(); } catch { }
        }, this.syncTimeout);
        this._syncTimer.unref?.();
    }

    _clearSyncTimer() {
        if (this._syncTimer) { clearTimeout(this._syncTimer); this._syncTimer = null; }
    }

    _resolveSync() {
        this._clearSyncTimer();
        if (this._syncDeferred) { try { this._syncDeferred.resolve(); } catch { } this._syncDeferred = null; }
    }

    _handleProtocolMessage(text) {
        if (text === 'RESET_CONFIRM') {
            this._protocolMode = false;
            this._expectedLength = null;
            this._resolveSync();
            this.emit('syncComplete');
        }
    }

    /** 收到对端的损坏报告 */
    _onPeerError(code) {
        this._emitError(ipcError(`peer reported ${ERROR_NAMES[code] || code}`, 'ERESYNC'));
        this._enterProtocolMode();
        this._sendProtocolMessage('RESET_CONFIRM').catch(() => { });
        this._protocolMode = false;
        this._resolveSync();
    }

    // ===== 发送 =====
    send(data) {
        if (!Buffer.isBuffer(data)) data = Buffer.from(data);
        if (data.length > this.maxBufferSize) {
            return Promise.reject(ipcError('data exceeds maxBufferSize', 'ETOOBIG'));
        }
        return this._enqueue(() => this._writeFrame(data));
    }

    _enqueue(fn) {
        const p = this._chain.then(fn);
        this._chain = p.then(() => { }, () => { });   // 队列永不因单次失败而断链
        return p;
    }

    async _syncGate() {
        if (!this._protocolMode) return;
        if (!this._syncDeferred) throw ipcError('stream is resyncing', 'ERESYNC');
        await this._syncDeferred.promise;   // 超时会被 reject
    }

    async _writeFrame(data, bypassGate = false) {
        if (this._closed) throw ipcError('stream closed', 'ECLOSED');
        if (!bypassGate) await this._syncGate();

        const head = Buffer.allocUnsafe(this.enableCRC ? 8 : 4);
        head.writeUInt32LE(data.length, 0);
        if (this.enableCRC) head.writeUInt32LE(crc32(head.subarray(0, 4)), 4);

        await this._writeBuf(head);
        await this._writeBuf(data);

        if (this.enableCRC) {
            const tail = Buffer.allocUnsafe(4);
            tail.writeUInt32LE(crc32(data), 0);
            await this._writeBuf(tail);
        }
    }

    async _writeBuf(buf) {
        if (this._closed || !this.socket || this.socket.destroyed) throw ipcError('stream not writable', 'ECLOSED');
        if (buf.length <= CHUNK) {
            if (!this.socket.write(buf)) await this._drain();
            return;
        }
        for (let off = 0; off < buf.length; off += CHUNK) {
            const chunk = buf.subarray(off, Math.min(off + CHUNK, buf.length));
            if (!this.socket.write(chunk)) await this._drain();
        }
    }

    _drain() {
        if (!this.socket || this.socket.destroyed) return Promise.reject(ipcError('stream closed', 'ECLOSED'));
        return new Promise((resolve, reject) => {
            const done = () => { cleanup(); resolve(); };
            const fail = (e) => { cleanup(); reject(e instanceof Error ? e : ipcError('write failed', 'ECLOSED')); };
            const onClose = () => fail(ipcError('stream closed during write', 'ECLOSED'));
            const cleanup = () => {
                this.socket.off('drain', done);
                this.socket.off('close', onClose);
                this.socket.off('error', fail);
            };
            this.socket.once('drain', done);
            this.socket.once('close', onClose);
            this.socket.once('error', fail);
        });
    }

    async _sendError(code) {
        const body = Buffer.from([0xEE, 0xEE, 0xEE, 0xEE, code]);
        return this._enqueue(() => this._writeFrame(body, true));
    }

    async _sendProtocolMessage(message) {
        const payload = Buffer.from(message, 'utf8');
        const len = Buffer.allocUnsafe(4);
        len.writeUInt32LE(payload.length, 0);
        const raw = Buffer.concat([PROTOCOL_HEADER, len, payload]);
        return this._enqueue(async () => {
            if (this._closed || this.socket?.destroyed) throw ipcError('stream closed', 'ECLOSED');
            if (!this.socket.write(raw)) await this._drain();
        });
    }

    // 兼容旧 API
    async sendProtocolMessage(m) { return this._sendProtocolMessage(m); }
    async sendError(name) { return this._sendError(ERROR_CODES[name] || 0); }

    reset() { this._enterProtocolMode(); }

    destroy() {
        if (this._closed) return;
        this._closed = true;
        this._notifyClose(true);
        if (this._syncDeferred) { try { this._syncDeferred.reject(ipcError('destroyed', 'ECLOSED')); } catch { } this._syncDeferred = null; }
        this._clearSyncTimer();
        this._clearBuffer();
        this._expectedLength = null;
        try { this._detach?.(); } catch { }
        try { this.socket?.destroy(); } catch { }   // 不再写入 'EOF' 垃圾字节
        this.socket = null;
        this.removeAllListeners();
    }
}
