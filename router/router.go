package router

import (
	"errors"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/silentd0g/ffsf/bus"
	"github.com/silentd0g/ffsf/logger"
	"github.com/silentd0g/ffsf/sharedstruct"
	"github.com/silentd0g/ffsf/svrinstmgr"
)

// router
// . 主要处理服务器之间的消息收发
// . 使用bus作为底层的消息转输
// 要求：需要保证协程安全

// -------------------------------- public --------------------------------

func SelfBusId() uint32 {
	return router.busImpl.SelfBusId()
}

func SelfSvrType() uint32 {
	return (SelfBusId() >> 8) & 0xff
}

// type CbOnRecvSSPacket func(*sharedstruct.SSPacketHeader, []byte)
type CbOnRecvSSPacket func(*sharedstruct.SSPacket) // frameMsg的所有权，归回调函数

// cb CbOnRecvSSPacket将由底层(bus)协程调用
func InitAndRun(selfBusId string, cb CbOnRecvSSPacket, rabbitmqAddr string,
	routeRules map[uint32]uint32, zookeeperAddr string) error {
	err := severInstanceMgr.InitAndRun(selfBusId, routeRules, zookeeperAddr)
	if err != nil {
		return err
	}

	router.cbOnRecvSSPacket = cb
	router.busImpl = bus.CreateBus("rabbitmq", bus.IpStringToInt(selfBusId), onRecvBusMsg, rabbitmqAddr)
	if router.busImpl == nil {
		return errors.New("failed to create bus implement")
	}
	return nil
}

func InitAndRunMQManualAck(selfBusId string, cb CbOnRecvSSPacket, rabbitmqAddr string,
	routeRules map[uint32]uint32, zookeeperAddr string) error {
	err := severInstanceMgr.InitAndRun(selfBusId, routeRules, zookeeperAddr)
	if err != nil {
		return err
	}

	router.cbOnRecvSSPacket = cb
	router.busImpl = bus.CreateBus("rabbitmq_manual_ack", bus.IpStringToInt(selfBusId), onRecvBusMsg, rabbitmqAddr)
	if router.busImpl == nil {
		return errors.New("failed to create bus implement")
	}
	return nil
}

// 最终通过bus发消息的地方（其他都是易用性封装）
func SendMsg(packetHeader *sharedstruct.SSPacketHeader, packetBody []byte) error {
	logger.Debugf("Send bus message: %#v", packetHeader)
	err := router.busImpl.Send(packetHeader.DstBusID, packetHeader.ToBytes(), packetBody)
	if err != nil {
		e := fmt.Sprintf("failed to send bus message {header:%#v, bodyLen:%v} | %v",
			packetHeader, len(packetBody), err)
		return errors.New(e)
	}
	return nil
}

func SendPbMsg(packetHeader *sharedstruct.SSPacketHeader, pbMsg proto.Message) error {
	logger.Debugf("SendPbMsg. {msg:%v}", pbMsg.String())
	packetBody, err := proto.Marshal(pbMsg)
	if err != nil {
		return err
	}
	packetHeader.BodyLen = uint32(len(packetBody))
	return SendMsg(packetHeader, packetBody)
}

func SendMQAck(queueTag uint64) error {
	err := router.busImpl.Ack(queueTag)
	if err != nil {
		logger.Errorf("failed to ack message. {err:%v}", err)
		return err
	}
	return nil
}

func SendMsgByBusId(busId uint32, uid uint64, cmd uint32, sendSeq uint32, srcTransId uint32, data []byte) error {
	if busId == 0 {
		return fmt.Errorf("server instance is 0, fail to send {busId: %v, uid: %v, cmd: %v}", busId, uid, cmd)
	}

	packetHeader := sharedstruct.SSPacketHeader{
		SrcBusID:   SelfBusId(),
		DstBusID:   busId,
		SrcTransID: srcTransId,
		DstTransID: 0,
		Uid:        uid,
		Cmd:        cmd,
		BodyLen:    uint32(len(data)),
		CmdSeq:     sendSeq,
	}

	logger.Debugf("SendPbMsgBySvrType. {header:%#v}", packetHeader)

	return SendMsg(&packetHeader, data)
}

func SendPbMsgByBusId(busId uint32, uid uint64, cmd uint32, sendSeq uint32, srcTransId uint32, pbMsg proto.Message) error {
	data, err := proto.Marshal(pbMsg)
	if err != nil {
		return err
	}
	return SendMsgByBusId(busId, uid, cmd, sendSeq, srcTransId, data)
}

func SendPbMsgByBusIdSimple(busId uint32, uid uint64, cmd uint32, pbMsg proto.Message) error {
	return SendPbMsgByBusId(busId, uid, cmd, 0, 0, pbMsg)
}

func SendMsgBySvrType(svrType uint32, uid uint64, cmd uint32, sendSeq uint32, srcTransId uint32, data []byte) error {
	dstBusId := severInstanceMgr.GetSvrInsBySvrType(svrType, uid)
	if dstBusId == 0 {
		return fmt.Errorf("cannot get a server instance to send {svrType: %v, uid: %v, cmd: %v}", svrType, uid, cmd)
	}

	return SendMsgByBusId(dstBusId, uid, cmd, sendSeq, srcTransId, data)
}

func SendPbMsgBySvrType(svrType uint32, uid uint64, cmd uint32, sendSeq uint32, srcTransId uint32, pbMsg proto.Message) error {
	data, err := proto.Marshal(pbMsg)
	if err != nil {
		return err
	}
	return SendMsgBySvrType(svrType, uid, cmd, sendSeq, srcTransId, data)
}

func SendPbMsgBySvrTypeSimple(svrType uint32, uid uint64, cmd uint32, pbMsg proto.Message) error {
	return SendPbMsgBySvrType(svrType, uid, cmd, 0, 0, pbMsg)
}

func BroadcastMsgByServerType(svrType uint32, uid uint64, cmd uint32, sendSeq uint32, data []byte) error {
	instances := severInstanceMgr.GetAllSvrInsBySvrType(svrType)
	if len(instances) == 0 {
		return fmt.Errorf("cannot get a server instance to send {svrType: %v, uid: %v, cmd: %v}", svrType, uid, cmd)
	}

	for _, inst := range instances {
		SendMsgByBusId(inst, uid, cmd, sendSeq, 0, data)
	}

	return nil
}

func BroadcastPbMsgByServerType(svrType uint32, uid uint64, cmd uint32, sendSeq uint32, pbMsg proto.Message) error {
	data, err := proto.Marshal(pbMsg)
	if err != nil {
		return err
	}
	return BroadcastMsgByServerType(svrType, uid, cmd, sendSeq, data)
}

func SendMsgBack(originalHeader sharedstruct.SSPacketHeader, srcTransId uint32, pbMsg proto.Message) {
	originalHeader.DstBusID = originalHeader.SrcBusID
	originalHeader.SrcBusID = SelfBusId()
	originalHeader.DstTransID = originalHeader.SrcTransID
	originalHeader.SrcTransID = srcTransId
	originalHeader.Cmd = originalHeader.Cmd + 1

	logger.Debugf("SendMsgBack. {header:%#v}", originalHeader)

	SendPbMsg(&originalHeader, pbMsg)
}

// -------------------------------- private --------------------------------

var severInstanceMgr svrinstmgr.ServerInstanceMgr

var router struct {
	busImpl          bus.IBus
	cbOnRecvSSPacket CbOnRecvSSPacket
}

func onRecvBusMsg(srcBusId uint32, data []byte) {
	if len(data) < sharedstruct.ByteLenOfSSPacketHeader() {
		return
	}

	packet := new(sharedstruct.SSPacket)
	packet.Header.From(data)
	packet.Body = data[sharedstruct.ByteLenOfSSPacketHeader():]
	logger.Debugf("Received bus message. {header:%#v}", packet.Header)
	if router.cbOnRecvSSPacket != nil {
		router.cbOnRecvSSPacket(packet)
		packet = nil
	}
}
