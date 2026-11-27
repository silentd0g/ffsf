// 验证服务器消息在三种总线共用的线上布局中完整往返。
package sharedstruct

import (
	"encoding/binary"
	"testing"
)

func TestSSPacketAddressRoundTrip(t *testing.T) {
	in := SSPacketHeader{SrcBusID: 0x07010101, DstBusID: 0x07010201, SrcTransID: 41, DstTransID: 92, Uid: 123, ExtId: 11, Cmd: 71, BodyLen: 17, CmdSeq: 3, Extra: 19}
	raw := in.ToBytes()
	var out SSPacketHeader
	if len(raw) != 48 || binary.BigEndian.Uint32(raw[4:8]) != in.DstBusID || out.From(raw) != nil || out != in {
		t.Fatalf("header round trip: %#v -> %#v", in, out)
	}
}
