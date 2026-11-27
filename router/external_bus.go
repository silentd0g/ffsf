// 本文件允许业务工程接入自有总线，并将同一实例管理器的地址查询交给该总线。
package router

import (
	"context"
	"github.com/silentd0g/ffsf/bus"
	"github.com/silentd0g/ffsf/svrinstmgr"
	"net/netip"
	"strings"
)

type ExternalBus interface {
	bus.IBus
	svrinstmgr.Endpoint
	Close()
}

type ExternalBusFactory func(uint32, bus.MsgHandler, func(uint32) (netip.Addr, error)) (ExternalBus, error)

// InitAndRunWithBus 只在启动时调用。返回的关闭函数用于撤销在线注册和本地连接。
func InitAndRunWithBus(self string, cb CbOnRecvSSPacket, rules map[uint32]uint32, zkAddr string, open ExternalBusFactory) (func(), error) {
	dir, err := severInstanceMgr.NewAddressDirectory(self, rules)
	if err != nil {
		return nil, err
	}
	impl, err := open(bus.IpStringToInt(self), onRecvBusMsg, dir.Resolve)
	if err != nil {
		return nil, err
	}
	router.cbOnRecvSSPacket = cb
	router.busImpl = impl
	ctx, cancel := context.WithCancel(context.Background())
	closeBus := func() { cancel(); impl.Close() }
	if err := dir.Run(ctx, strings.Split(zkAddr, ","), impl); err != nil {
		closeBus()
		return nil, err
	}
	return closeBus, nil
}
