package cmd_handler

import (
	"github.com/golang/protobuf/proto"
)

type IContext interface {
	Uid() uint64
	OriSrcBusId() uint32

	ParseMsg(data []byte, msg proto.Message) error

	CallMsgBySvrType(svrType uint32, cmd uint32, req proto.Message, rsp proto.Message) error
	SendMsgBySvrType(svrType uint32, cmd uint32, req proto.Message) error
	SendPbMsgByBusId(busId uint32, cmd uint32, req proto.Message) error
	SendMsgBack(pbMsg proto.Message)

	Errorf(format string, args ...interface{})
	Warningf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Debugf(format string, args ...interface{})
}

type ICmdHandler interface {
	ProcessCmd(context IContext, data []byte) int32
}
