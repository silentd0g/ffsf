// 本测试在显式指定的本机 ZooKeeper 上验证地址发布、撤销、重连与冲突隔离。
package svrinstmgr

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"os"
	"sync"
	"testing"
	"time"
)

type testEndpoint struct {
	mu    sync.Mutex
	ip    netip.Addr
	gen   chan struct{}
	ready bool
}

func (e *testEndpoint) Endpoint() (netip.Addr, <-chan struct{}) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.ip, e.gen
}
func (e *testEndpoint) SetRouteReady(g <-chan struct{}, v bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if g == e.gen {
		e.ready = v
	}
}
func (e *testEndpoint) set(ip netip.Addr) {
	e.mu.Lock()
	defer e.mu.Unlock()
	close(e.gen)
	e.gen = make(chan struct{})
	e.ip = ip
	e.ready = false
}
func eventually(t *testing.T, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !f() {
		if time.Now().After(deadline) {
			t.Fatal("directory did not converge")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
func TestLiveAddressMembership(t *testing.T) {
	addr := os.Getenv("MSGAGENT_TEST_ZK")
	if addr == "" {
		t.Skip("set MSGAGENT_TEST_ZK to an isolated local ZooKeeper")
	}
	host, _, _ := net.SplitHostPort(addr)
	if !net.ParseIP(host).IsLoopback() {
		t.Fatal("requires a loopback test ZooKeeper")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a, b := new(ServerInstanceMgr), new(ServerInstanceMgr)
	rules := map[uint32]uint32{2: SvrRouterRule_UID}
	dirA, _ := a.NewAddressDirectory("250.1.1.1", rules)
	dirB, _ := b.NewAddressDirectory("250.1.2.2", rules)
	ipA, ipB := netip.MustParseAddr("10.41.0.1"), netip.MustParseAddr("10.41.0.2")
	epA, epB := &testEndpoint{ip: ipA, gen: make(chan struct{})}, &testEndpoint{ip: ipB, gen: make(chan struct{})}
	if e := dirA.Run(ctx, []string{addr}, epA); e != nil {
		t.Fatal(e)
	}
	if e := dirB.Run(ctx, []string{addr}, epB); e != nil {
		t.Fatal(e)
	}
	const dst = 0xfa010202
	eventually(t, func() bool {
		v, e := dirA.Resolve(dst)
		return e == nil && v == ipB && a.GetSvrInsBySvrType(2, 11, 0) == dst
	})
	epB.set(netip.Addr{})
	eventually(t, func() bool {
		_, e := dirA.Resolve(dst)
		return errors.Is(e, ErrNoRoute) && len(a.GetAllSvrInsBySvrType(2)) == 0
	})
	newIP := netip.MustParseAddr("10.41.0.3")
	epB.set(newIP)
	eventually(t, func() bool { v, e := dirA.Resolve(dst); return e == nil && v == newIP })
	// 同一 BusID 的另一会话不能覆盖已有地址。
	clash, _ := new(ServerInstanceMgr).NewAddressDirectory("250.1.2.2", rules)
	cctx, ccancel := context.WithCancel(ctx)
	defer ccancel()
	result := make(chan error, 1)
	go func() { result <- clash.Run(cctx, []string{addr}, &testEndpoint{ip: ipB, gen: make(chan struct{})}) }()
	eventually(t, func() bool { _, e := dirA.Resolve(dst); return errors.Is(e, ErrNoRoute) })
	ccancel()
	<-result
	eventually(t, func() bool { v, e := dirA.Resolve(dst); return e == nil && v == newIP })
}
