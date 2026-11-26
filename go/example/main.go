// Go 示例：server/client 双模式，用于与其他语言实现互操作。
//
//	go run ./example server 9004
//	go run ./example client <port>
package main

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	ipc "ipcprotocol"
)

func registerMethods(rpc *ipc.RpcProtocol) {
	rpc.RegisterMethod("add", func(a []any) (any, error) {
		return a[0].(float64) + a[1].(float64), nil
	})
	rpc.RegisterMethod("echo", func(a []any) (any, error) { return a[0], nil })
	rpc.RegisterMethod("greet", func(a []any) (any, error) {
		return "hello " + a[0].(string), nil
	})
	rpc.RegisterMethod("blob", func(a []any) (any, error) {
		return []byte{0, 1, 2, 255}, nil
	})
	rpc.RegisterMethod("nothing", func(a []any) (any, error) { return nil, nil })
	rpc.RegisterMethod("fail", func(a []any) (any, error) {
		return nil, fmt.Errorf("boom")
	})
	rpc.RegisterMethod("upload", func(a []any) (any, error) {
		data := a[0].([]byte)
		return map[string]any{"size": float64(len(data)), "tag": a[1], "head": data[:4]}, nil
	})
	rpc.RegisterMethod("pushEvents", func(a []any) (any, error) {
		_ = rpc.SendJSON(map[string]any{"push": 1})
		_ = rpc.SendText("srv-text")
		_ = rpc.SendBinary([]byte{1, 2})
		return "sent", nil
	})
}

func runServer(port int) {
	ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		panic(err)
	}
	fmt.Printf("{\"port\":%d}\n", ln.Addr().(*net.TCPAddr).Port)
	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		go func() {
			rpc := ipc.NewRpcProtocol(ipc.NewMessageStream(conn, conn))
			rpc.OnError = func(err error) {}
			registerMethods(rpc)
		}()
	}
}

var passed int

func check(name string, ok bool, detail any) {
	if ok {
		passed++
		fmt.Printf("PASS %s\n", name)
	} else {
		fmt.Printf("FAIL %s: %v\n", name, detail)
		os.Exit(1)
	}
}

func runClient(port int) {
	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		panic(err)
	}
	rpc := ipc.NewRpcProtocol(ipc.NewMessageStream(conn, conn))
	rpc.OnError = func(err error) {}

	v, err := rpc.SendRequest("add", []any{3, 4})
	check("add", err == nil && v.(float64) == 7, v)

	obj := map[string]any{"name": "测试", "list": []any{1, 2.5, true, nil}}
	v, err = rpc.SendRequest("echo", obj)
	m, _ := v.(map[string]any)
	check("echo", err == nil && m != nil && m["name"] == "测试" && m["list"].([]any)[1].(float64) == 2.5, v)

	v, err = rpc.SendRequest("greet", "世界")
	check("greet", err == nil && v == "hello 世界", v)

	v, err = rpc.SendRequest("blob", nil)
	check("blob", err == nil && bytes.Equal(v.([]byte), []byte{0, 1, 2, 255}), v)

	v, err = rpc.SendRequest("nothing", nil)
	check("nothing", err == nil && v == nil, v)

	_, err = rpc.SendRequest("fail", nil)
	re, ok := err.(*ipc.RpcError)
	check("fail", ok && re.Value == "boom", err)

	_, err = rpc.SendRequest("nope", nil)
	re, ok = err.(*ipc.RpcError)
	check("methodNotFound", ok && re.Value == "Method not found: nope", err)

	data := make([]byte, 256*300)
	for i := range data {
		data[i] = byte(i & 0xFF)
	}
	v, err = rpc.SendRequestBinary("upload", data, "tag-1")
	m, _ = v.(map[string]any)
	check("upload", err == nil && m != nil && m["size"].(float64) == float64(len(data)) &&
		m["tag"] == "tag-1" && m["head"].([]any)[3].(float64) == 3, v)

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
	_, err = rpc.SendRequest("pushEvents", nil)
	select {
	case <-done:
	case <-time.After(3 * time.Second):
	}
	mu.Lock()
	evOK := len(events) == 3 &&
		events[0].(map[string]any)["push"].(float64) == 1 &&
		events[1] == "srv-text" &&
		bytes.Equal(events[2].([]byte), []byte{1, 2})
	mu.Unlock()
	check("events", err == nil && evOK, events)

	var wg sync.WaitGroup
	var failCount int
	var cmu sync.Mutex
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			v, err := rpc.SendRequest("add", []any{n, n})
			if err != nil || v.(float64) != float64(n*2) {
				cmu.Lock()
				failCount++
				cmu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	check("concurrent50", failCount == 0, failCount)

	v, err = rpc.SendRequestTo("echo", "routed", "some-target")
	check("toField", err == nil && v == "routed", v)

	fmt.Printf("ALL %d CHECKS PASSED\n", passed)
	rpc.Close()
}

func main() {
	if len(os.Args) < 3 {
		fmt.Println("usage: example server|client <port>")
		os.Exit(2)
	}
	port, _ := strconv.Atoi(os.Args[2])
	switch os.Args[1] {
	case "server":
		runServer(port)
	case "client":
		runClient(port)
	default:
		os.Exit(2)
	}
}