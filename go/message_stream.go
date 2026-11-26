// Package ipc 实现与 MessageStream.js / RpcProtocol.js 线格式兼容的 IPC 协议。
package ipc

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"sync"
	"time"
)

// 错误包类型码（与 JS 版 ERROR_TYPES 一致）。
const (
	ErrLengthCRCFailed  = 1
	ErrMessageCRCFailed = 2
	ErrBufferOverflow   = 3
)

const defaultMaxBufferSize = 256 * 1024 * 1024

// StreamError 带错误码的流错误（对齐 JS 修复版 ipcError 的 code 字段）。
type StreamError struct {
	Message string
	Code    string
}

func (e *StreamError) Error() string { return e.Message }

// MessageStream 帧层：长度头分帧 + 可选 CRC32 + 错误包与重同步。
// 回调（OnMessage/OnError/OnSyncComplete）必须在 Start 之前设置。
type MessageStream struct {
	r io.Reader
	w io.Writer

	EnableCRC     bool
	MaxBufferSize int
	// SyncTimeout 重同步握手超时（PROTOCOL.md §4.5）；<=0 关闭。需在 Start 前设置。
	SyncTimeout time.Duration

	OnMessage      func(data []byte)
	OnError        func(err error)
	OnSyncComplete func()

	sendMu         sync.Mutex
	startOnce      sync.Once
	buf            []byte
	expectedLen    int // -1 表示等待帧头
	stateMu        sync.Mutex
	syncCond       *sync.Cond
	protocolMode   bool
	syncGeneration int // 重同步计时世代号，退出协议模式时递增作废旧计时
	closed         bool
}

// NewMessageStream 创建消息流（默认启用 CRC）。不会自动开始读取，
// 设置回调后调用 Start（RpcProtocol 会自动处理）。
func NewMessageStream(r io.Reader, w io.Writer) *MessageStream {
	ms := &MessageStream{
		r:             r,
		w:             w,
		EnableCRC:     true,
		MaxBufferSize: defaultMaxBufferSize,
		SyncTimeout:   3 * time.Second,
		expectedLen:   -1,
	}
	ms.syncCond = sync.NewCond(&ms.stateMu)
	return ms
}

// Start 启动读循环（幂等）。
func (ms *MessageStream) Start() {
	ms.startOnce.Do(func() { go ms.readLoop() })
}

// Close 关闭消息流；若底层连接实现 io.Closer 则一并关闭。
func (ms *MessageStream) Close() {
	ms.stateMu.Lock()
	if ms.closed {
		ms.stateMu.Unlock()
		return
	}
	ms.closed = true
	ms.syncGeneration++
	ms.syncCond.Broadcast() // 唤醒门控中的发送方，使其以关闭失败
	ms.stateMu.Unlock()
	if c, ok := ms.w.(io.Closer); ok {
		_ = c.Close()
	} else if c, ok := ms.r.(io.Closer); ok {
		_ = c.Close()
	}
}

func (ms *MessageStream) isClosed() bool {
	ms.stateMu.Lock()
	defer ms.stateMu.Unlock()
	return ms.closed
}

func (ms *MessageStream) inProtocolMode() bool {
	ms.stateMu.Lock()
	defer ms.stateMu.Unlock()
	return ms.protocolMode
}

// Send 发送一个完整帧（并发安全，帧不交错）。
// 协议模式期间门控等待重同步完成（对齐 JS _syncGate）。
func (ms *MessageStream) Send(data []byte) error {
	if len(data) > ms.MaxBufferSize {
		return fmt.Errorf("data size exceeds maximum buffer size (%d > %d)", len(data), ms.MaxBufferSize)
	}
	ms.stateMu.Lock()
	for ms.protocolMode && !ms.closed {
		ms.syncCond.Wait()
	}
	closed := ms.closed
	ms.stateMu.Unlock()
	if closed {
		return errors.New("stream closed")
	}
	return ms.sendFrame(data)
}

// sendFrame 直接写帧，不受重同步门控（错误包/协议消息旁路用）。
func (ms *MessageStream) sendFrame(data []byte) error {
	headerSize := 4
	if ms.EnableCRC {
		headerSize = 8
	}
	frame := make([]byte, headerSize+len(data)+boolInt(ms.EnableCRC)*4)
	binary.LittleEndian.PutUint32(frame[0:4], uint32(len(data)))
	if ms.EnableCRC {
		binary.LittleEndian.PutUint32(frame[4:8], crc32.ChecksumIEEE(frame[0:4]))
	}
	copy(frame[headerSize:], data)
	if ms.EnableCRC {
		binary.LittleEndian.PutUint32(frame[headerSize+len(data):], crc32.ChecksumIEEE(data))
	}
	ms.sendMu.Lock()
	defer ms.sendMu.Unlock()
	_, err := ms.w.Write(frame)
	return err
}

// SendError 发送错误包（按正常帧封装的 EEEEEEEE+code）；不受重同步门控。
func (ms *MessageStream) SendError(code byte) error {
	return ms.sendFrame([]byte{0xEE, 0xEE, 0xEE, 0xEE, code})
}

// SendProtocolMessage 发送裸协议帧（FFFFFFFF+长度+数据，无 CRC）。
func (ms *MessageStream) SendProtocolMessage(data []byte) error {
	frame := make([]byte, 8+len(data))
	binary.LittleEndian.PutUint32(frame[0:4], 0xFFFFFFFF)
	binary.LittleEndian.PutUint32(frame[4:8], uint32(len(data)))
	copy(frame[8:], data)
	ms.sendMu.Lock()
	defer ms.sendMu.Unlock()
	_, err := ms.w.Write(frame)
	return err
}

func (ms *MessageStream) readLoop() {
	tmp := make([]byte, 65536)
	for {
		n, err := ms.r.Read(tmp)
		if n > 0 {
			ms.buf = append(ms.buf, tmp[:n]...)
			ms.processBuffer()
		}
		if err != nil {
			if !ms.isClosed() {
				if errors.Is(err, io.EOF) {
					ms.emitError(&StreamError{Message: "Connection closed", Code: "ECLOSED"})
				} else {
					ms.emitError(&StreamError{Message: err.Error(), Code: "ESOCKET"})
				}
			}
			return
		}
	}
}

func (ms *MessageStream) reset() {
	ms.buf = nil
	ms.expectedLen = -1
	ms.stateMu.Lock()
	ms.protocolMode = true
	ms.syncGeneration++
	gen := ms.syncGeneration
	ms.stateMu.Unlock()
	if ms.SyncTimeout > 0 {
		go ms.syncWatch(gen)
	}
}

// exitProtocolMode 退出协议模式：作废旧计时并唤醒门控中的发送方。
func (ms *MessageStream) exitProtocolMode() {
	ms.stateMu.Lock()
	ms.protocolMode = false
	ms.syncGeneration++
	ms.syncCond.Broadcast()
	ms.stateMu.Unlock()
}

// syncWatch 重同步计时：超时仍未收到 RESET_CONFIRM 则以 ERESYNC_TIMEOUT 断链。
func (ms *MessageStream) syncWatch(gen int) {
	time.Sleep(ms.SyncTimeout)
	ms.stateMu.Lock()
	if gen != ms.syncGeneration || !ms.protocolMode || ms.closed {
		ms.stateMu.Unlock()
		return
	}
	ms.protocolMode = false
	ms.syncGeneration++
	ms.syncCond.Broadcast()
	ms.stateMu.Unlock()
	ms.emitError(&StreamError{Message: "resync handshake timeout", Code: "ERESYNC_TIMEOUT"})
	ms.Close()
}

func (ms *MessageStream) processBuffer() {
	for {
		if len(ms.buf) > ms.MaxBufferSize {
			ms.emitError(&StreamError{Message: "buffer overflow", Code: "BUFFER_OVERFLOW"})
			_ = ms.SendError(ErrBufferOverflow)
			ms.reset()
			return
		}
		if ms.inProtocolMode() {
			// 扫描协议头，而不是整段丢弃（避免误丢 RESET_CONFIRM）
			window := len(ms.buf)
			if window > 64*1024 {
				window = 64 * 1024
			}
			idx := -1
			if window >= 4 {
				idx = bytes.Index(ms.buf[:window], []byte{0xFF, 0xFF, 0xFF, 0xFF})
			}
			if idx >= 0 {
				ms.buf = ms.buf[idx:]
				if len(ms.buf) < 8 {
					return // 等待长度字段
				}
				length := binary.LittleEndian.Uint32(ms.buf[4:8])
				if length > 50 {
					ms.buf = ms.buf[8:] // 伪头：跳过继续扫描
					continue
				}
				if len(ms.buf) < 8+int(length) {
					return // 协议消息未收全
				}
				text := string(ms.buf[8 : 8+int(length)])
				ms.buf = ms.buf[8+int(length):]
				if trimSpace(text) == "RESET_CONFIRM" {
					ms.exitProtocolMode()
					ms.buf = nil
					if ms.OnSyncComplete != nil {
						ms.OnSyncComplete()
					}
				}
				continue
			}
			// 窗口内无协议头：丢弃已扫描部分，保留尾部 3 字节（可能半截头）
			keep := len(ms.buf)
			if keep > 3 {
				keep = 3
			}
			ms.buf = ms.buf[len(ms.buf)-keep:]
			return
		}

		headerSize := 4
		if ms.EnableCRC {
			headerSize = 8
		}
		if ms.expectedLen == -1 {
			if len(ms.buf) < headerSize {
				return
			}
			length := binary.LittleEndian.Uint32(ms.buf[0:4])
			if ms.EnableCRC {
				expected := binary.LittleEndian.Uint32(ms.buf[4:8])
				if crc32.ChecksumIEEE(ms.buf[0:4]) != expected {
					_ = ms.SendError(ErrLengthCRCFailed)
					ms.emitError(&StreamError{Message: "length header CRC check failed", Code: "LENGTH_CRC_FAILED"})
					ms.reset()
					return
				}
			}
			// 长度头强制校验：0 或超限视为坏帧（CRC 合法也可能是伪造/损坏的长度）
			if length == 0 || uint64(length) > uint64(ms.MaxBufferSize) {
				code := byte(ErrLengthCRCFailed)
				if uint64(length) > uint64(ms.MaxBufferSize) {
					code = ErrBufferOverflow
				}
				_ = ms.SendError(code)
				ms.emitError(&StreamError{Message: fmt.Sprintf("invalid frame length %d", length), Code: "BAD_LENGTH"})
				ms.reset()
				return
			}
			ms.expectedLen = int(length)
			ms.buf = ms.buf[headerSize:]
		}

		totalSize := ms.expectedLen
		if ms.EnableCRC {
			totalSize += 4
		}
		if len(ms.buf) < totalSize {
			return
		}
		data := make([]byte, ms.expectedLen)
		copy(data, ms.buf[:ms.expectedLen])
		if ms.EnableCRC {
			expected := binary.LittleEndian.Uint32(ms.buf[ms.expectedLen : ms.expectedLen+4])
			if crc32.ChecksumIEEE(data) != expected {
				_ = ms.SendError(ErrMessageCRCFailed)
				ms.emitError(&StreamError{Message: "message CRC check failed", Code: "MESSAGE_CRC_FAILED"})
				ms.reset()
				return
			}
		}
		ms.buf = ms.buf[totalSize:]
		ms.expectedLen = -1
		ms.extractMessage(data) // 错误包可能 reset 清空缓冲区，故先消费再分发
		if ms.inProtocolMode() {
			return
		}
	}
}

func (ms *MessageStream) extractMessage(data []byte) {
	if len(data) == 5 && data[0] == 0xEE && data[1] == 0xEE && data[2] == 0xEE && data[3] == 0xEE {
		// 收到错误包：上报 ERESYNC、清状态、回 RESET_CONFIRM、立即回正常模式
		ms.emitError(&StreamError{Message: "peer reported error packet", Code: "ERESYNC"})
		ms.reset()
		_ = ms.SendProtocolMessage([]byte("RESET_CONFIRM"))
		ms.exitProtocolMode()
		return
	}
	if ms.OnMessage != nil {
		ms.OnMessage(data)
	}
}

func (ms *MessageStream) emitError(err error) {
	if ms.OnError != nil {
		ms.OnError(err)
	}
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func trimSpace(s string) string {
	start, end := 0, len(s)
	for start < end && isSpace(s[start]) {
		start++
	}
	for end > start && isSpace(s[end-1]) {
		end--
	}
	return s[start:end]
}

func isSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\v' || c == '\f'
}
