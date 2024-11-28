package iplocation

import (
	"testing"
)

func TestIp2Loc(t *testing.T) {
	m := new(Ip2LocMgr)
	err := m.Init("qqwry.dat")
	if err != nil {
		t.Fatal(err)
	}

	ip := "125.211.146.0"
	rsp := m.Query(ip)
	t.Logf("IP:%s, %v", ip, rsp)
}
