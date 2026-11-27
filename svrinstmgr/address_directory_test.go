// 验证空地址、非法节点和重复 BusID 不会被当作可发送的地址。
package svrinstmgr

import (
	"net/netip"
	"testing"
)

func TestAddressSnapshot(t *testing.T) {
	records := map[string]addressRecord{}
	for n, v := range map[string]string{"7.1.2.1.0000000001": "10.0.0.1", "7.1.2.1.0000000002": "10.0.0.1", "7.1.2.2.0000000003": "", "7.1.2.3.0000000004": "10.0.0.2", "bad.node": ""} {
		if r, ok := decodeNode(n, []byte(v), 1); ok {
			records[n] = r
		}
	}
	types, routes := addressSnapshot(records)
	if len(types[2]) != 3 || types[2][0] != 0x07010201 || len(routes) != 1 || routes[0x07010203] != netip.MustParseAddr("10.0.0.2") {
		t.Fatalf("instances=%v routes=%v", types, routes)
	}
}
