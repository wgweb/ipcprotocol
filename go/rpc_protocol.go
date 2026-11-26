package ipc

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// 消息类型与载荷类型（与 JS 版一致，v1.1 新增 3-6）。
const (
	TypeRequest  = 0
	TypeResponse = 1
	TypeEvent    = 2
	TypePing     = 3
	TypePong     = 4
	TypeHello    = 5
	TypeHelloAck = 6

	ContentText   = 0
	ContentJSON   = 1
	ContentBinary = 2
)

// ProtocolVersion 线协议版本（HELLO 协商用）。
const ProtocolVersion = 1

// Handler 服务端方法处理器；args 为展开后的参数，返回值决定响应类型。
type Handler func(args []any) (any, error)

// RpcError 对端返回的错误响应，Value 携带原始 error 值（与 JS reject(response.error) 对齐）；
// Code 为错误码（如 ETIMEDOUT/EREMOTE）。
type RpcError struct {
	Value any
	Code  string
}

func (e *RpcError) Error() string { return fmt.Sprintf("rpc error: %v", e.Value) }

// RpcProtocol 消息层：请求/响应/事件。构造时自动挂回调并启动读循环。
type RpcProtocol struct {
	ms *MessageStream

	handlersMu sync.Mutex
	handlers   map[string]Handler

	pendingMu sync.Mutex
	pending   map[string]chan rpcResult

	OnEvent func(contentType int, content any)
	OnError func(err error)

	// DefaultTimeout 默认 RPC 超时，对齐 JS 修复版 15s；0 表示关闭。可在构造后修改。
	DefaultTimeout time.Duration

	// ---- v1.1: 心跳与 HELLO 握手（PROTOCOL.md §8.1/§8.2） ----
	HeartbeatInterval time.Duration // <=0 关闭心跳
	HeartbeatTimeout  time.Duration // 静默超时，EHEARTBEAT 断链；<=0 关闭
	AuthToken         string        // 客户端 HELLO 携带的令牌（为空时回退环境变量 IPC_TOKEN）
	RequiredToken     string        // 服务端要求的令牌；非空时未鉴权请求被拒绝
	lastReceived      atomic.Int64  // 最后入站消息时刻（UnixMilli）
	authenticated     atomic.Bool
	hbStop            chan struct{}

	// ---- v1.1.2: 能力协商（PROTOCOL.md §8.4） ----
	Capabilities       []string // 本端声明的能力
	ImplName           string   // 实现名，随 HELLO/HELLO_ACK 发送
	PeerCapabilities   []string // 对端在 HELLO 中声明的能力（nil = 旧实现）
	ServerCapabilities []string // 客户端收到 HELLO_ACK 里服务端声明的能力
	heartbeatDisabled  atomic.Bool
	heartbeatReason    atomic.Value // string
	hbOnce             sync.Once

	// ---- v1.1.3: 首个业务请求前自动握手（P2） ----
	authSent atomic.Bool
	authMu   sync.Mutex

	closeOnce sync.Once
}

// DefaultCapabilities v1.1.2 默认能力集合。
var DefaultCapabilities = []string{"ping", "binary", "event"}

type rpcResult struct {
	value any
	err   error
}

// NewRpcProtocol 包装消息流（默认心跳 5s/15s，不鉴权）；先注册回调再启动读循环，避免早到消息丢失。
func NewRpcProtocol(ms *MessageStream) *RpcProtocol {
	return NewRpcProtocolOptions(ms, 5*time.Second, 15*time.Second, "", "")
}

// NewRpcProtocolOptions 带心跳与鉴权配置的构造。
func NewRpcProtocolOptions(ms *MessageStream, heartbeatInterval, heartbeatTimeout time.Duration, authToken, requiredToken string) *RpcProtocol {
	if authToken == "" {
		authToken = os.Getenv("IPC_TOKEN")
	}
	rpc := &RpcProtocol{
		ms:                ms,
		handlers:          make(map[string]Handler),
		pending:           make(map[string]chan rpcResult),
		DefaultTimeout:    15 * time.Second,
		HeartbeatInterval: heartbeatInterval,
		HeartbeatTimeout:  heartbeatTimeout,
		AuthToken:         authToken,
		RequiredToken:     requiredToken,
		Capabilities:      append([]string(nil), DefaultCapabilities...),
		ImplName:          "go",
	}
	rpc.heartbeatReason.Store("")
	rpc.authenticated.Store(requiredToken == "" && authToken == "")
	rpc.lastReceived.Store(time.Now().UnixMilli())
	ms.OnMessage = rpc.handleMessage
	ms.OnError = rpc.handleStreamError
	ms.Start()
	if heartbeatInterval > 0 {
		rpc.hbStop = make(chan struct{})
		go rpc.heartbeatLoop()
	}
	return rpc
}

// heartbeatLoop 心跳节拍：静默超过 timeout 以 EHEARTBEAT 结算全部 pending 并断链；否则按间隔发 PING。
func (rpc *RpcProtocol) heartbeatLoop() {
	ticker := time.NewTicker(rpc.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-rpc.hbStop:
			return
		case <-ticker.C:
			if rpc.heartbeatDisabled.Load() {
				return
			}
			silent := time.Since(time.UnixMilli(rpc.lastReceived.Load()))
			if rpc.HeartbeatTimeout > 0 && silent >= rpc.HeartbeatTimeout {
				err := &RpcError{
					Value: fmt.Sprintf("heartbeat timeout: no data for %dms", silent.Milliseconds()),
					Code:  "EHEARTBEAT",
				}
				rpc.rejectAll(err)
				if rpc.OnError != nil {
					rpc.OnError(err)
				}
				rpc.Close()
				return
			}
			if silent >= rpc.HeartbeatInterval {
				_ = rpc.ms.Send(buildMessage(TypePing, ContentText, newUUID(), nil))
			}
		}
	}
}

// RegisterMethod 注册服务端方法。
func (rpc *RpcProtocol) RegisterMethod(name string, h Handler) {
	rpc.handlersMu.Lock()
	defer rpc.handlersMu.Unlock()
	rpc.handlers[name] = h
}

// SendRequest 发送 JSON 请求并阻塞等待响应。
// 返回值为 JSON 解析结果（map[string]any / []any / float64 / string / bool / nil）、
// TEXT 字符串或 BINARY []byte；对端错误响应返回 *RpcError。
func (rpc *RpcProtocol) SendRequest(method string, params any) (any, error) {
	return rpc.SendRequestTo(method, params, "")
}

// SendRequestTo 带可选 to 路由字段的请求（默认超时）。
func (rpc *RpcProtocol) SendRequestTo(method string, params any, to string) (any, error) {
	return rpc.SendRequestToTimeout(method, params, to, rpc.DefaultTimeout)
}

// SendRequestTimeout 带显式超时的请求；0 关闭超时。
func (rpc *RpcProtocol) SendRequestTimeout(method string, params any, timeout time.Duration) (any, error) {
	return rpc.SendRequestToTimeout(method, params, "", timeout)
}

// SendRequestToTimeout 带 to 路由与显式超时的请求；超时返回 *RpcError{Code: "ETIMEDOUT"}，对齐 JS 修复版。
func (rpc *RpcProtocol) SendRequestToTimeout(method string, params any, to string, timeout time.Duration) (any, error) {
	if err := rpc.ensureAuth(); err != nil {
		return nil, err
	}
	id := newUUID()
	ch := make(chan rpcResult, 1)
	rpc.pendingMu.Lock()
	rpc.pending[id] = ch
	rpc.pendingMu.Unlock()

	payload := map[string]any{"method": method, "params": normalizeJSON(params)}
	if to != "" {
		payload["to"] = to
	}
	body, err := json.Marshal(payload)
	if err != nil {
		rpc.removePending(id)
		return nil, err
	}
	if err := rpc.ms.Send(buildMessage(TypeRequest, ContentJSON, id, body)); err != nil {
		rpc.removePending(id)
		return nil, err
	}
	return rpc.waitResult(id, ch, method, timeout)
}

// SendRequestBinary 发送二进制请求（默认超时）：handler 实参为 [data, ...args]。
func (rpc *RpcProtocol) SendRequestBinary(method string, data []byte, args ...any) (any, error) {
	return rpc.SendRequestBinaryTimeout(rpc.DefaultTimeout, method, data, args...)
}

// SendRequestBinaryTimeout 带显式超时的二进制请求；0 关闭超时。
func (rpc *RpcProtocol) SendRequestBinaryTimeout(timeout time.Duration, method string, data []byte, args ...any) (any, error) {
	if err := rpc.ensureAuth(); err != nil {
		return nil, err
	}
	id := newUUID()
	ch := make(chan rpcResult, 1)
	rpc.pendingMu.Lock()
	rpc.pending[id] = ch
	rpc.pendingMu.Unlock()

	body := encodeBinaryRequest(method, data, args)
	if err := rpc.ms.Send(buildMessage(TypeRequest, ContentBinary, id, body)); err != nil {
		rpc.removePending(id)
		return nil, err
	}
	return rpc.waitResult(id, ch, method, timeout)
}

// waitResult 等待响应；超时先移除 pending 再返回 ETIMEDOUT，避免迟到响应写入已废弃的 channel。
func (rpc *RpcProtocol) waitResult(id string, ch chan rpcResult, method string, timeout time.Duration) (any, error) {
	if timeout <= 0 {
		res := <-ch
		return res.value, res.err
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case res := <-ch:
		return res.value, res.err
	case <-timer.C:
		rpc.removePending(id)
		return nil, &RpcError{
			Value: fmt.Sprintf("RPC timeout after %dms: %s", timeout.Milliseconds(), method),
			Code:  "ETIMEDOUT",
		}
	}
}

// SendText / SendJSON / SendBinary 发送单向事件。
func (rpc *RpcProtocol) SendText(text string) error {
	return rpc.ms.Send(buildMessage(TypeEvent, ContentText, newUUID(), []byte(text)))
}

func (rpc *RpcProtocol) SendJSON(value any) error {
	body, err := json.Marshal(normalizeJSON(value))
	if err != nil {
		return err
	}
	return rpc.ms.Send(buildMessage(TypeEvent, ContentJSON, newUUID(), body))
}

func (rpc *RpcProtocol) SendBinary(data []byte) error {
	return rpc.ms.Send(buildMessage(TypeEvent, ContentBinary, newUUID(), data))
}

// Close 销毁：拒绝所有 pending 并关闭流。
func (rpc *RpcProtocol) Close() {
	rpc.closeOnce.Do(func() {
		rpc.hbOnce.Do(func() {
			if rpc.hbStop != nil {
				close(rpc.hbStop)
			}
		})
		rpc.rejectAll(errors.New("RPC Protocol destroyed"))
		rpc.ms.Close()
	})
}

func (rpc *RpcProtocol) removePending(id string) {
	rpc.pendingMu.Lock()
	delete(rpc.pending, id)
	rpc.pendingMu.Unlock()
}

func (rpc *RpcProtocol) takePending(id string) chan rpcResult {
	rpc.pendingMu.Lock()
	defer rpc.pendingMu.Unlock()
	ch := rpc.pending[id]
	delete(rpc.pending, id)
	return ch
}

func (rpc *RpcProtocol) rejectAll(err error) {
	rpc.pendingMu.Lock()
	defer rpc.pendingMu.Unlock()
	for id, ch := range rpc.pending {
		ch <- rpcResult{err: err}
		delete(rpc.pending, id)
	}
}

func (rpc *RpcProtocol) handleStreamError(err error) {
	var se *StreamError
	if errors.As(err, &se) {
		rpc.rejectAll(&RpcError{Value: "Request failed: " + se.Message, Code: se.Code})
	} else {
		rpc.rejectAll(fmt.Errorf("Request failed: %w", err))
	}
	if rpc.OnError != nil {
		rpc.OnError(err)
	}
}

func (rpc *RpcProtocol) handleMessage(data []byte) {
	if len(data) < 18 {
		log.Printf("[ipc] message too short: %d", len(data))
		return
	}
	msgType := int(data[0])
	contentType := int(data[1])
	if msgType < 0 || msgType > 6 {
		log.Printf("[ipc] invalid message type: %d", msgType)
		return
	}
	id := bytesToUUID(data[2:18])
	payload := data[18:]
	rpc.lastReceived.Store(time.Now().UnixMilli()) // 任何入站消息都刷新心跳基准

	content, err := deserialize(contentType, payload)
	if err != nil {
		log.Printf("[ipc] deserialize failed: %v", err)
		// 畸形 REQUEST 尽量回错误响应，避免调用方永久等待（对齐 JS 修复版）
		if msgType == TypeRequest {
			rpc.sendErrorResponse(id, "Invalid request payload")
		}
		return
	}

	switch msgType {
	case TypeRequest:
		if rpc.RequiredToken != "" && !rpc.authenticated.Load() {
			rpc.sendErrorResponse(id, "Unauthorized")
			rpc.Close()
			return
		}
		go rpc.handleRequest(id, contentType, content, payload)
	case TypeResponse:
		if ch := rpc.takePending(id); ch != nil {
			if m, ok := content.(map[string]any); ok {
				if ev, exists := m["error"]; exists && isTruthy(ev) {
					ch <- rpcResult{err: &RpcError{Value: ev, Code: "EREMOTE"}}
					return
				}
			}
			ch <- rpcResult{value: content}
		}
	case TypeEvent:
		if rpc.RequiredToken != "" && !rpc.authenticated.Load() {
			return // 未鉴权事件直接丢弃
		}
		if rpc.OnEvent != nil {
			rpc.OnEvent(contentType, content)
		}
	case TypePing:
		_ = rpc.ms.Send(buildMessage(TypePong, ContentText, id, nil))
	case TypePong:
		// 仅刷新 lastReceived（上面已做）
	case TypeHello:
		rpc.handleHello(id, content)
	case TypeHelloAck:
		rpc.handleHelloAck(id, content)
	}
}

// ---- v1.1: HELLO 握手 ----

func (rpc *RpcProtocol) handleHello(id string, content any) {
	ack := map[string]any{"version": float64(ProtocolVersion)}
	ok := false
	if m, isMap := content.(map[string]any); isMap {
		version, _ := m["version"].(float64)
		if version != ProtocolVersion {
			ack["error"] = "Unsupported protocol version"
		} else if rpc.RequiredToken != "" && m["token"] != rpc.RequiredToken {
			ack["error"] = "Unauthorized"
		} else {
			rpc.authenticated.Store(true)
			// v1.1.2：能力协商
			rpc.PeerCapabilities = readCaps(m)
			if rpc.PeerCapabilities != nil && !containsStr(rpc.PeerCapabilities, "ping") {
				impl, _ := m["impl"].(string)
				if impl == "" {
					impl = "unknown"
				}
				rpc.disableHeartbeat("peer " + impl + " declares no 'ping' capability")
			}
			ack["ok"] = true
			ack["caps"] = rpc.Capabilities
			ack["impl"] = rpc.ImplName
			ok = true
		}
	} else {
		ack["error"] = "Invalid hello payload"
	}
	body, _ := json.Marshal(ack)
	_ = rpc.ms.Send(buildMessage(TypeHelloAck, ContentJSON, id, body))
	if !ok {
		// v1.1.2：先让 ack 刷到对端再断链（立即 Close 会丢掉未送达的 ack）
		go func() { time.Sleep(300 * time.Millisecond); rpc.Close() }()
	}
}

func (rpc *RpcProtocol) handleHelloAck(id string, content any) {
	ch := rpc.takePending(id)
	if ch == nil {
		return
	}
	m, ok := content.(map[string]any)
	if !ok {
		ch <- rpcResult{err: &RpcError{Value: "Invalid hello ack payload", Code: "EAUTH"}}
		return
	}
	if ev, exists := m["error"]; exists && isTruthy(ev) {
		ch <- rpcResult{err: &RpcError{Value: ev, Code: "EAUTH"}}
		return
	}
	if version, _ := m["version"].(float64); version != ProtocolVersion {
		ch <- rpcResult{err: &RpcError{Value: "Unsupported protocol version", Code: "EVERSION"}}
		return
	}
	// v1.1.2：记录服务端能力；服务端不支持 PING 则本端停用心跳
	rpc.ServerCapabilities = readCaps(m)
	if rpc.ServerCapabilities != nil && !containsStr(rpc.ServerCapabilities, "ping") {
		rpc.disableHeartbeat("server declares no 'ping' capability")
	}
	rpc.authenticated.Store(true)
	ch <- rpcResult{value: content}
}

// ---- v1.1.3: 自动握手（P2） ----

// ensureAuth 在首个业务请求前自动 HELLO。仅客户端（RequiredToken=="" 且持有 AuthToken）生效；
// 并发调用由 authMu 串行化，只握手一次。
func (rpc *RpcProtocol) ensureAuth() error {
	if rpc.RequiredToken != "" || rpc.AuthToken == "" {
		return nil
	}
	if rpc.authenticated.Load() || rpc.authSent.Load() {
		return nil
	}
	rpc.authMu.Lock()
	defer rpc.authMu.Unlock()
	if rpc.authenticated.Load() || rpc.authSent.Load() {
		return nil
	}
	if _, err := rpc.Hello(); err != nil {
		return err
	}
	rpc.authSent.Store(true)
	return nil
}

// AuthSent 是否已完成（或被跳过）自动握手。
func (rpc *RpcProtocol) AuthSent() bool {
	return rpc.authSent.Load() || rpc.authenticated.Load()
}

// ---- v1.1.2: 能力协商辅助 ----

// HeartbeatDisabledReason 返回因能力协商停用心跳的原因（"" 表示心跳正常）。
func (rpc *RpcProtocol) HeartbeatDisabledReason() string {
	if s, _ := rpc.heartbeatReason.Load().(string); s != "" {
		return s
	}
	return ""
}

func (rpc *RpcProtocol) disableHeartbeat(reason string) {
	if rpc.heartbeatDisabled.Swap(true) {
		return
	}
	rpc.heartbeatReason.Store(reason)
	rpc.hbOnce.Do(func() {
		if rpc.hbStop != nil {
			close(rpc.hbStop)
		}
	})
}

func readCaps(m map[string]any) []string {
	raw, ok := m["caps"].([]any)
	if !ok {
		if list, ok := m["caps"].([]string); ok {
			return list
		}
		return nil
	}
	out := make([]string, 0, len(raw))
	for _, it := range raw {
		if s, ok := it.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

func containsStr(list []string, want string) bool {
	for _, s := range list {
		if s == want {
			return true
		}
	}
	return false
}

// Hello 发起 HELLO 握手（默认超时）；ACK error → EAUTH，版本不符 → EVERSION。
func (rpc *RpcProtocol) Hello() (any, error) {
	return rpc.HelloTimeout(rpc.DefaultTimeout)
}

// HelloTimeout 带显式超时的 HELLO 握手；0 关闭超时。
func (rpc *RpcProtocol) HelloTimeout(timeout time.Duration) (any, error) {
	id := newUUID()
	ch := make(chan rpcResult, 1)
	rpc.pendingMu.Lock()
	rpc.pending[id] = ch
	rpc.pendingMu.Unlock()

	payload := map[string]any{"version": float64(ProtocolVersion),
		"caps": rpc.Capabilities, "impl": rpc.ImplName}
	if rpc.AuthToken != "" {
		payload["token"] = rpc.AuthToken
	}
	body, _ := json.Marshal(payload)
	if err := rpc.ms.Send(buildMessage(TypeHello, ContentJSON, id, body)); err != nil {
		rpc.removePending(id)
		return nil, err
	}
	return rpc.waitResult(id, ch, "hello", timeout)
}

func (rpc *RpcProtocol) sendErrorResponse(id, msg string) {
	body, _ := json.Marshal(map[string]any{"error": msg})
	if err := rpc.ms.Send(buildMessage(TypeResponse, ContentJSON, id, body)); err != nil {
		log.Printf("[ipc] send error response failed: %v", err)
	}
}

func (rpc *RpcProtocol) handleRequest(id string, contentType int, content any, raw []byte) {
	var method string
	var args []any

	if contentType == ContentBinary {
		m, binArgs, err := decodeBinaryRequest(raw)
		if err != nil {
			log.Printf("[ipc] bad binary request: %v", err)
			rpc.sendErrorResponse(id, "Invalid request payload: "+err.Error())
			return
		}
		method, args = m, binArgs
	} else {
		m, ok := content.(map[string]any)
		if !ok {
			log.Printf("[ipc] invalid request content")
			rpc.sendErrorResponse(id, "Invalid request payload: not an object")
			return
		}
		method, _ = m["method"].(string)
		args = expandParams(m["params"])
	}
	if method == "" {
		rpc.sendErrorResponse(id, "Invalid request payload: method missing")
		return
	}

	rpc.handlersMu.Lock()
	h := rpc.handlers[method]
	rpc.handlersMu.Unlock()

	var result any
	var handlerErr error
	if h == nil {
		handlerErr = fmt.Errorf("Method not found: %s", method)
	} else {
		result, handlerErr = h(args)
	}

	var respType int
	var respBody []byte
	if handlerErr != nil {
		respType = ContentJSON
		respBody, _ = json.Marshal(map[string]any{"error": handlerErr.Error()})
	} else {
		respType, respBody = serializeResponse(result)
	}
	if err := rpc.ms.Send(buildMessage(TypeResponse, respType, id, respBody)); err != nil {
		log.Printf("[ipc] send response failed: %v", err)
	}
}

// ---- 序列化辅助 ----

func deserialize(contentType int, payload []byte) (any, error) {
	switch contentType {
	case ContentText:
		return string(payload), nil
	case ContentJSON:
		var v any
		if err := json.Unmarshal(payload, &v); err != nil {
			return nil, err
		}
		return v, nil
	case ContentBinary:
		out := make([]byte, len(payload))
		copy(out, payload)
		return out, nil
	default:
		return nil, fmt.Errorf("unknown content type: %d", contentType)
	}
}

func serializeResponse(result any) (int, []byte) {
	switch v := result.(type) {
	case nil:
		return ContentJSON, []byte("null")
	case string:
		return ContentText, []byte(v)
	case []byte:
		return ContentBinary, v
	default:
		body, err := json.Marshal(normalizeJSON(result))
		if err != nil {
			return ContentJSON, []byte("null")
		}
		return ContentJSON, body
	}
}

func expandParams(params any) []any {
	switch v := params.(type) {
	case nil:
		return nil
	case []any:
		return v
	default:
		return []any{v}
	}
}

// normalizeJSON 递归把 []byte 转为数字数组（对齐 JSON.stringify(Buffer)；
// Go 默认将 []byte 编为 Base64，必须转换）。
func normalizeJSON(v any) any {
	switch t := v.(type) {
	case []byte:
		arr := make([]any, len(t))
		for i, b := range t {
			arr[i] = float64(b)
		}
		return arr
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			out[k] = normalizeJSON(val)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, val := range t {
			out[i] = normalizeJSON(val)
		}
		return out
	default:
		return v
	}
}

// isTruthy JS 真值语义：nil/false/0/""/NaN 为假，其余（含空 map/slice）为真。
func isTruthy(v any) bool {
	switch t := v.(type) {
	case nil:
		return false
	case bool:
		return t
	case float64:
		return t != 0 && !math.IsNaN(t)
	case int:
		return t != 0
	case string:
		return t != ""
	default:
		return true
	}
}

func buildMessage(msgType, contentType int, id string, payload []byte) []byte {
	msg := make([]byte, 18+len(payload))
	msg[0] = byte(msgType)
	msg[1] = byte(contentType)
	copy(msg[2:18], uuidToBytes(id))
	copy(msg[18:], payload)
	return msg
}

// encodeBinaryRequest [methodLen u32BE][method][paramsLen u32BE][paramsJSON][binary]
func encodeBinaryRequest(method string, data []byte, args []any) []byte {
	methodBytes := []byte(method)
	paramsJSON, _ := json.Marshal(normalizeJSON(args))
	if args == nil {
		paramsJSON = []byte("[]")
	}
	out := make([]byte, 4+len(methodBytes)+4+len(paramsJSON)+len(data))
	binary.BigEndian.PutUint32(out[0:4], uint32(len(methodBytes)))
	copy(out[4:], methodBytes)
	off := 4 + len(methodBytes)
	binary.BigEndian.PutUint32(out[off:off+4], uint32(len(paramsJSON)))
	copy(out[off+4:], paramsJSON)
	copy(out[off+4+len(paramsJSON):], data)
	return out
}

func decodeBinaryRequest(raw []byte) (string, []any, error) {
	if len(raw) < 4 {
		return "", nil, errors.New("binary request too short")
	}
	methodLen := int(binary.BigEndian.Uint32(raw[0:4]))
	if len(raw) < 4+methodLen+4 {
		return "", nil, errors.New("binary request truncated")
	}
	method := string(raw[4 : 4+methodLen])
	off := 4 + methodLen
	paramsLen := int(binary.BigEndian.Uint32(raw[off : off+4]))
	if len(raw) < off+4+paramsLen {
		return "", nil, errors.New("binary request params truncated")
	}
	var params []any
	if err := json.Unmarshal(raw[off+4:off+4+paramsLen], &params); err != nil {
		return "", nil, err
	}
	bin := raw[off+4+paramsLen:]
	data := make([]byte, len(bin))
	copy(data, bin)
	args := make([]any, 0, len(params)+1)
	args = append(args, data)
	args = append(args, params...)
	return method, args, nil
}

// ---- UUID（RFC 4122 大端字节序，与 JS/Python/Java 一致）----

func newUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant
	return bytesToUUID(b)
}

func uuidToBytes(id string) []byte {
	b, _ := hex.DecodeString(strings.ReplaceAll(id, "-", ""))
	return b
}

func bytesToUUID(b []byte) string {
	h := hex.EncodeToString(b)
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
}
