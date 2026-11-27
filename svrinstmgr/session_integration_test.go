// 本测试临时断开本机 ZooKeeper 会话，验证停发、会话过期和恢复时的临时节点重建。
package svrinstmgr

import (
	"context"
	"errors"
	"io"
	"net"
	"net/netip"
	"os"
	"sync"
	"testing"
	"time"
)

func TestLiveExpiredSession(t *testing.T) {
	zkAddr := os.Getenv("MSGAGENT_TEST_ZK")
	if zkAddr == "" {
		t.Skip("requires an isolated local ZooKeeper")
	}
	host, _, _ := net.SplitHostPort(zkAddr)
	if !net.ParseIP(host).IsLoopback() {
		t.Fatal("requires loopback")
	}
	// 代理只过滤本测试会话，不停止 ZooKeeper，也不影响观察者。
	front, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer front.Close()
	var mu sync.Mutex
	blocked := false
	var active net.Conn
	go func() {
		for {
			client, e := front.Accept()
			if e != nil {
				return
			}
			mu.Lock()
			if blocked {
				mu.Unlock()
				client.Close()
				continue
			}
			active = client
			mu.Unlock()
			go func() {
				defer client.Close()
				server, e := net.Dial("tcp4", zkAddr)
				if e != nil {
					return
				}
				defer server.Close()
				go func() { io.Copy(server, client); server.Close() }()
				io.Copy(client, server)
			}()
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	src, _ := new(ServerInstanceMgr).NewAddressDirectory("249.1.1.1", nil)
	dst, _ := new(ServerInstanceMgr).NewAddressDirectory("249.1.2.1", nil)
	ip := netip.MustParseAddr("10.42.0.1")
	if err := src.Run(ctx, []string{zkAddr}, &testEndpoint{ip: ip, gen: make(chan struct{})}); err != nil {
		t.Fatal(err)
	}
	if err := dst.Run(ctx, []string{front.Addr().String()}, &testEndpoint{ip: ip, gen: make(chan struct{})}); err != nil {
		t.Fatal(err)
	}
	const id = 0xf9010201
	eventually(t, func() bool { _, e := src.Resolve(id); return e == nil })
	session := dst.conn.SessionID()
	mu.Lock()
	blocked = true
	active.Close()
	mu.Unlock()
	eventually(t, func() bool { _, e := dst.Resolve(id); return errors.Is(e, ErrRouteNotReady) })
	end := time.Now().Add(18 * time.Second)
	for {
		if _, e := src.Resolve(id); errors.Is(e, ErrNoRoute) {
			break
		}
		if time.Now().After(end) {
			t.Fatal("expired node still published")
		}
		time.Sleep(30 * time.Millisecond)
	}
	mu.Lock()
	blocked = false
	mu.Unlock()
	eventually(t, func() bool {
		_, a := src.Resolve(id)
		_, b := dst.Resolve(id)
		return a == nil && b == nil && dst.conn.SessionID() != session
	})
}
