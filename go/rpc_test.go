package ipc

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"net"
	"sync"
	"testing"
	"time"
)

type testServer struct {
	rpc    *RpcProtocol
	ms     *MessageStream
	syncCh chan struct{}
	ready  chan struct{}
	ln     net.Listener
}

func startTestServer(t *testing.T) *testServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ts := &testServer{ln: ln, syncCh: make(chan struct{}, 4), ready: make(chan struct{})}
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		ms := NewMessageStream(conn, conn)
		ms.OnSyncComplete = func() { ts.syncCh <- struct{}{} }
		rpc := NewRpcProtocol(ms)
		ts.rpc, ts.ms = rpc, ms
		rpc.OnError = func(err error) {} // 静默流错误（CRC 注入测试会触发）
		rpc.RegisterMethod("add", func(a []any) (any, error) {
			return a[0].(float64) + a[1].(float64), nil
		})
		rpc.RegisterMethod("echo", func(a []any) (any, error) { return a[0], nil })
		rpc.RegisterMethod("greet", func(a []any) (any, error) {
			return "你好, " + a[0].(string) + "!", nil
		})
		rpc.RegisterMethod("blob", func(a []any) (any, error) {
			return []byte{0, 1, 2, 250, 255}, nil
		})
		rpc.RegisterMethod("nothing", func(a []any) (any, error) { return nil, nil })
		rpc.RegisterMethod("fail", func(a []any) (any, error) {
			return nil, fmt.Errorf("boom: %v", a[0])
		})
		rpc.RegisterMethod("upload", func(a []any) (any, error) {
			data := a[0].([]byte)
			tag := a[1].(string)
			return map[string]any{"size": float64(len(data)), "tag": tag,
				"head": data[:4], "crcOK": true}, nil
		})
		rpc.RegisterMethod("pushEvents", func(a []any) (any, error) {
			_ = rpc.SendText("text-event")
			_ = rpc.SendJSON(map[string]any{"n": 42})
			_ = rpc.SendBinary([]byte{9, 8, 7})
			return "sent", nil
		})
		rpc.RegisterMethod("slow", func(a []any) (any, error) {
			time.Sleep(500 * time.Millisecond)
			return "late", nil
		})
		close(ts.ready)
	}()
	// 等待服务端就绪（连接建立后才注册完成，先 dial 再等 ready）
	return ts
}

func (ts *testServer) waitReady(t *testing.T) {
	t.Helper()
	select {
	case <-ts.ready:
	case <-time.After(3 * time.Second):
		t.Fatal("server ready timeout")
	}
}

func (ts *testServer) close() {
	if ts.rpc != nil {
		ts.rpc.Close()
	}
	_ = ts.ln.Close()
}

func dialTest(t *testing.T, ts *testServer) (*RpcProtocol, net.Conn) {
	t.Helper()
	conn, err := net.Dial("tcp", ts.ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	ts.waitReady(t)
	rpc := NewRpcProtocol(NewMessageStream(conn, conn))
	rpc.OnError = func(err error) {}
	return rpc, conn
}

func TestJSONRequest(t *testing.T) {
	ts := startTestServer(t)
	defer ts.close()
	rpc, _ := dialTest(t, ts)
	defer rpc.Close()
	v, err := rpc.SendRequest("add", []any{3, 4})
	if err != nil || v.(float64) != 7 {
		t.Fatalf("add = %v, err = %v", v, err)
	}
	v, err = rpc.SendRequest("echo", map[string]any{"x": []any{1, "二", true, nil}})
	m := v.(map[string]any)
	if err != nil || m["x"].([]any)[1] != "二" {
		t.Fatalf("echo = %v, err = %v", v, err)
	}
}

func TestTextAndUnicode(t *testing.T) {
	ts := startTestServer(t)
	defer ts.close()
	rpc, _ := dialTest(t, ts)
	defer rpc.Close()
	v, err := rpc.SendRequest("greet", "世界")
	if err != nil || v != "你好, 世界!" {
		t.Fatalf("greet = %v, err = %v", v, err)
	}
}

func TestBinaryResponse(t *testing.T) {
	ts := startTestServer(t)
	defer ts.close()
	rpc, _ := dialTest(t, ts)
	defer rpc.Close()
	v, err := rpc.SendRequest("blob", nil)
	if err != nil || !bytes.Equal(v.([]byte), []byte{0, 1, 2, 250, 255}) {
		t.Fatalf("blob = %v, err = %v", v, err)
	}
}

func TestNullResponse(t *testing.T) {
	ts := startTestServer(t)
	defer ts.close()
	rpc, _ := dialTest(t, ts)
	defer rpc.Close()
	v, err := rpc.SendRequest("nothing", nil)
	if err != nil || v != nil {
		t.Fatalf("nothing = %v, err = %v", v, err)
	}
}

func TestErrorResponse(t *testing.T) {
	ts := startTestServer(t)
	defer ts.close()
	rpc, _ := dialTest(t, ts)
	defer rpc.Close()
	_, err := rpc.SendRequest("fail", "x")
	re, ok := err.(*RpcError)
	if !ok || re.Value != "boom: x" {
		t.Fatalf("fail err = %v", err)
	}
	_, err = rpc.SendRequest("nope", nil)
	re, ok = err.(*RpcError)
	if !ok || re.Value != "Method not found: nope" {
		t.Fatalf("nope err = %v", err)
	}
}

func TestBinaryRequest(t *testing.T) {
	ts := startTestServer(t)
	defer ts.close()
	rpc, _ := dialTest(t, ts)
	defer rpc.Close()
	data := make([]byte, 75*1024)
	for i := range data {
		data[i] = byte(i % 251)
	}
	v, err := rpc.SendRequestBinary("upload", data, "tag-1")
	if err != nil {
		t.Fatal(err)
	}
	m := v.(map[string]any)
	if m["size"].(float64) != float64(len(data)) || m["tag"] != "tag-1" {
		t.Fatalf("upload = %v", m)
	}
	head := m["head"].([]any)
	if len(head) != 4 || head[0].(float64) != 0 || head[3].(float64) != 3 {
		t.Fatalf("head = %v", head)
	}
}

func TestEvents(t *testing.T) {
	ts := startTestServer(t)
	defer ts.close()
	rpc, _ := dialTest(t, ts)
	defer rpc.Close()
	var mu sync.Mutex
	var events []any
	done := make(chan struct{})
	rpc.OnEvent = func(ct int, content any) {
		mu.Lock()
		events = append(events, content)
		if len(events) == 3 {
			close(done)
		}
		mu.Unlock()
	}
	if _, err := rpc.SendRequest("pushEvents", nil); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("events timeout")
	}
	mu.Lock()
	defer mu.Unlock()
	if events[0] != "text-event" {
		t.Fatalf("event0 = %v", events[0])
	}
	if events[1].(map[string]any)["n"].(float64) != 42 {
		t.Fatalf("event1 = %v", events[1])
	}
	if !bytes.Equal(events[2].([]byte), []byte{9, 8, 7}) {
		t.Fatalf("event2 = %v", events[2])
	}
}

func TestConcurrentRequests(t *testing.T) {
	ts := startTestServer(t)
	defer ts.close()
	rpc, _ := dialTest(t, ts)
	defer rpc.Close()
	var wg sync.WaitGroup
	errCh := make(chan error, 50)
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			v, err := rpc.SendRequest("add", []any{n, 1})
			if err != nil || v.(float64) != float64(n+1) {
				errCh <- fmt.Errorf("concurrent %d: %v %v", n, v, err)
			}
		}(i)
	}
	wg.Wait()
	select {
	case err := <-errCh:
		t.Fatal(err)
	default:
	}
}

func TestCRCRecovery(t *testing.T) {
	ts := startTestServer(t)
	defer ts.close()
	rpc, conn := dialTest(t, ts)
	defer rpc.Close()

	// 注入坏长度头 CRC 帧：服务端报错→错误包→客户端回 RESET_CONFIRM→服务端 syncComplete
	payload := []byte("hello")
	frame := make([]byte, 8+len(payload)+4)
	binary.LittleEndian.PutUint32(frame[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(frame[4:8], 0xDEADBEEF)
	copy(frame[8:], payload)
	binary.LittleEndian.PutUint32(frame[8+len(payload):], crc32.ChecksumIEEE(payload))
	if _, err := conn.Write(frame); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ts.syncCh:
	case <-time.After(3 * time.Second):
		t.Fatal("syncComplete timeout")
	}
	// 恢复后通信继续
	v, err := rpc.SendRequest("add", []any{10, 20})
	if err != nil || v.(float64) != 30 {
		t.Fatalf("after resync: %v %v", v, err)
	}
}

func TestRPCTimeout(t *testing.T) {
	ts := startTestServer(t)
	defer ts.close()
	rpc, _ := dialTest(t, ts)
	defer rpc.Close()

	if rpc.DefaultTimeout != 15*time.Second {
		t.Fatalf("default timeout = %v", rpc.DefaultTimeout)
	}
	// 显式短超时触发 ETIMEDOUT
	_, err := rpc.SendRequestTimeout("slow", nil, 100*time.Millisecond)
	re, ok := err.(*RpcError)
	if !ok || re.Code != "ETIMEDOUT" {
		t.Fatalf("slow err = %v", err)
	}
	// pending 已清理，后续调用正常
	if v, err := rpc.SendRequest("add", []any{1, 2}); err != nil || v.(float64) != 3 {
		t.Fatalf("after timeout: %v %v", v, err)
	}
	// 等 slow 的迟到响应到达（应被静默丢弃）
	time.Sleep(600 * time.Millisecond)
	if v, err := rpc.SendRequest("add", []any{5, 6}); err != nil || v.(float64) != 11 {
		t.Fatalf("late response: %v %v", v, err)
	}
}

// ---- v1.1 用例（PROTOCOL.md §4.5/§8.1/§8.2） ----

var keepAliveMu sync.Mutex
var keepAliveConns []net.Conn // 防止静默对端连接被提前关闭

func keepConn(c net.Conn) {
	keepAliveMu.Lock()
	keepAliveConns = append(keepAliveConns, c)
	keepAliveMu.Unlock()
}

type rpcPair struct {
	ln     net.Listener
	server *RpcProtocol
	cli    *RpcProtocol
}

func newPair(t *testing.T, hbInterval, hbTimeout time.Duration, authToken, requiredToken string) *rpcPair {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &rpcPair{ln: ln}
	ready := make(chan struct{})
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		keepConn(conn)
		ms := NewMessageStream(conn, conn)
		p.server = NewRpcProtocolOptions(ms, hbInterval, hbTimeout, "", requiredToken)
		p.server.OnError = func(err error) {}
		p.server.RegisterMethod("add", func(a []any) (any, error) {
			return a[0].(float64) + a[1].(float64), nil
		})
		close(ready)
	}()
	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	p.cli = NewRpcProtocolOptions(NewMessageStream(conn, conn), hbInterval, hbTimeout, authToken, "")
	p.cli.OnError = func(err error) {}
	<-ready
	return p
}

func (p *rpcPair) close() {
	p.cli.Close()
	p.server.Close()
	_ = p.ln.Close()
}

func TestHelloOK(t *testing.T) {
	p := newPair(t, 0, 0, "secret", "secret")
	defer p.close()
	v, err := p.cli.Hello()
	if err != nil {
		t.Fatal(err)
	}
	m := v.(map[string]any)
	if m["ok"] != true || m["version"].(float64) != 1 {
		t.Fatalf("hello ack = %v", m)
	}
	if v, err := p.cli.SendRequest("add", []any{1, 2}); err != nil || v.(float64) != 3 {
		t.Fatalf("rpc after hello: %v %v", v, err)
	}
}

func TestHelloWrongToken(t *testing.T) {
	p := newPair(t, 0, 0, "wrong", "secret")
	defer p.close()
	_, err := p.cli.Hello()
	re, ok := err.(*RpcError)
	if !ok || re.Code != "EAUTH" || re.Value != "Unauthorized" {
		t.Fatalf("hello err = %v", err)
	}
}

func TestUnauthorizedRequest(t *testing.T) {
	p := newPair(t, 0, 0, "", "secret")
	defer p.close()
	_, err := p.cli.SendRequest("add", []any{1, 2})
	re, ok := err.(*RpcError)
	if !ok || re.Value != "Unauthorized" {
		t.Fatalf("unauthorized err = %v", err)
	}
}

func TestHelloVersionMismatch(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		keepConn(conn)
		ms := NewMessageStream(conn, conn)
		ms.OnMessage = func(data []byte) {
			id := bytesToUUID(data[2:18])
			body, _ := json.Marshal(map[string]any{"version": 99, "ok": true})
			_ = ms.Send(buildMessage(TypeHelloAck, ContentJSON, id, body))
		}
		ms.Start()
	}()
	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	cli := NewRpcProtocolOptions(NewMessageStream(conn, conn), 0, 0, "", "")
	cli.OnError = func(err error) {}
	defer cli.Close()
	_, err = cli.Hello()
	re, ok := err.(*RpcError)
	if !ok || re.Code != "EVERSION" {
		t.Fatalf("version mismatch err = %v", err)
	}
}

func TestHeartbeatKeepalive(t *testing.T) {
	p := newPair(t, 100*time.Millisecond, 5*time.Second, "", "")
	defer p.close()
	time.Sleep(500 * time.Millisecond) // 期间应有多轮 PING/PONG 交换
	if v, err := p.cli.SendRequest("add", []any{3, 4}); err != nil || v.(float64) != 7 {
		t.Fatalf("heartbeat keepalive: %v %v", v, err)
	}
}

func TestHeartbeatTimeout(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		keepConn(conn) // 保持连接但不回任何数据
	}()
	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	ms := NewMessageStream(conn, conn)
	cli := NewRpcProtocolOptions(ms, 100*time.Millisecond, 300*time.Millisecond, "", "")
	cli.OnError = func(err error) {}
	defer cli.Close()
	_, err = cli.SendRequest("add", []any{1, 2})
	re, ok := err.(*RpcError)
	if !ok || re.Code != "EHEARTBEAT" {
		t.Fatalf("heartbeat timeout err = %v", err)
	}
	if !ms.isClosed() {
		t.Fatal("heartbeat timeout should close stream")
	}
}

func TestResyncTimeout(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		keepConn(conn)
		payload := []byte("hello")
		frame := make([]byte, 8+len(payload)+4)
		binary.LittleEndian.PutUint32(frame[0:4], uint32(len(payload)))
		binary.LittleEndian.PutUint32(frame[4:8], 0xDEADBEEF)
		copy(frame[8:], payload)
		binary.LittleEndian.PutUint32(frame[8+len(payload):], crc32.ChecksumIEEE(payload))
		_, _ = conn.Write(frame) // 之后保持静默，不回 RESET_CONFIRM
	}()
	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	ms := NewMessageStream(conn, conn)
	ms.SyncTimeout = 300 * time.Millisecond
	cli := NewRpcProtocolOptions(ms, 0, 0, "", "")
	codes := make(chan string, 16)
	cli.OnError = func(err error) {
		if se, ok := err.(*StreamError); ok {
			select {
			case codes <- se.Code:
			default:
			}
		}
	}
	defer cli.Close()
	_, err = cli.SendRequest("add", []any{1, 2})
	re, ok := err.(*RpcError)
	if !ok || (re.Code != "LENGTH_CRC_FAILED" && re.Code != "ERESYNC_TIMEOUT") {
		t.Fatalf("resync pending err = %v", err)
	}
	deadline := time.After(2 * time.Second)
	for {
		select {
		case code := <-codes:
			if code == "ERESYNC_TIMEOUT" {
				if !ms.isClosed() {
					t.Fatal("resync timeout should close stream")
				}
				return
			}
		case <-deadline:
			t.Fatal("ERESYNC_TIMEOUT not emitted")
		}
	}
}

func TestAutoHelloFirstRequest(t *testing.T) {
	p := newPair(t, 0, 0, "secret", "secret")
	defer p.close()
	// 不显式 Hello，直接业务请求
	if v, err := p.cli.SendRequest("add", []any{2, 3}); err != nil || v.(float64) != 5 {
		t.Fatalf("auto hello first request: %v %v", v, err)
	}
	if !p.cli.AuthSent() {
		t.Fatal("authSent should be true after auto hello")
	}
}

func TestAutoHelloConcurrent(t *testing.T) {
	p := newPair(t, 0, 0, "secret", "secret")
	defer p.close()
	var wg sync.WaitGroup
	errs := make([]error, 8)
	vals := make([]float64, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v, err := p.cli.SendRequest("add", []any{float64(i), 1})
			if err != nil {
				errs[i] = err
				return
			}
			vals[i] = v.(float64)
		}(i)
	}
	wg.Wait()
	for i := 0; i < 8; i++ {
		if errs[i] != nil {
			t.Fatalf("concurrent auto hello[%d]: %v", i, errs[i])
		}
		if vals[i] != float64(i+1) {
			t.Fatalf("concurrent auto hello[%d] value = %v", i, vals[i])
		}
	}
}

func TestAutoHelloWrongToken(t *testing.T) {
	p := newPair(t, 0, 0, "wrong", "secret")
	defer p.close()
	_, err := p.cli.SendRequest("add", []any{1, 1})
	re, ok := err.(*RpcError)
	if !ok || re.Code != "EAUTH" {
		t.Fatalf("auto hello wrong token err = %v", err)
	}
}
