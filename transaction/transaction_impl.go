package transaction

import (
	"errors"
	"ffsf/cmd_handler"
	"ffsf/logger"
	"ffsf/router"
	"ffsf/sharedstruct"
	"fmt"
	"github.com/golang/glog"
	"github.com/golang/protobuf/proto"
	"time"
)

// 使用：
//   . 所有逻辑事务都继承自TransBase，实现Init、ProcessCmd。
//	 . 通过调用transmgr.RegisterCmd注册Cmd和实现类
//   . 在收到指定的Cmd时，Init、ProcessCmd会被相继调用，来实现逻辑功能。
//     其间，可通过TransBase::CallMsgByBusId、Transaction::SendMsgBack等方法，进行通信与回包
// 机制：
//   . TransBase实现transmgr.innerTransaction，由transmgr在收到新cmd时，创建协程，调用run函数，启动此事务。
//   . 其间，transmgr收到指定发给此事务（以事务ID标识）的消息，会通过chanIn转送给此事务。
//   . 事务结束后，通过chanOut将返回值返回给transmgr

type iTransaction interface {
	cmd_handler.IContext
	run(transID uint32, trans interface{}, packet *sharedstruct.SSPacket,
		chanIn <-chan *sharedstruct.SSPacket, chanTransRet chan<- transRet)
}

type Transaction struct {
	OriPacketHeader sharedstruct.SSPacketHeader
	// CurFrameHeader sharedstruct.SSPacketHeader

	transID uint32
	sendSeq uint32
	chanIn  chan *sharedstruct.SSPacket
}

func newTransaction(transID uint32, oriPacketHeader sharedstruct.SSPacketHeader,
	chanIn chan *sharedstruct.SSPacket) *Transaction {
	t := new(Transaction)
	t.transID = transID
	t.OriPacketHeader = oriPacketHeader
	t.chanIn = chanIn
	t.sendSeq = 0
	return t
}

func (t *Transaction) Errorf(format string, args ...interface{}) {
	f := fmt.Sprintf("[%v|%v] %v", t.Uid(), t.TransID(), format)
	logger.ErrorDepthf(1, f, args...)
}

func (t *Transaction) Warningf(format string, args ...interface{}) {
	f := fmt.Sprintf("[%v|%v] %v", t.Uid(), t.TransID(), format)
	logger.WarningDepthf(1, f, args...)
}

func (t *Transaction) Infof(format string, args ...interface{}) {
	f := fmt.Sprintf("[%v|%v] %v", t.Uid(), t.TransID(), format)
	logger.InfoDepthf(1, f, args...)
}

func (t *Transaction) Debugf(format string, args ...interface{}) {
	f := fmt.Sprintf("[%v|%v] %v", t.Uid(), t.TransID(), format)
	t.DebugDepthf(1, f, args...)
}
func (t *Transaction) DebugDepthf(depth int, format string, args ...interface{}) {
	f := fmt.Sprintf("[%v|%v] %v", t.Uid(), t.TransID(), format)
	logger.DebugDepthf(1+depth, f, args...)
}

func (t *Transaction) run(cmdHandler cmd_handler.ICmdHandler, packet *sharedstruct.SSPacket, chanRet chan<- transRet) {
	ret := cmdHandler.ProcessCmd(t, packet.Body)
	chanRet <- transRet{transID: t.transID, ret: ret}
}

func (t *Transaction) Uid() uint64 {
	return t.OriPacketHeader.Uid
}

func (t *Transaction) Cmd() uint32 {
	return t.OriPacketHeader.Cmd
}

func (t *Transaction) OriSrcBusId() uint32 {
	return t.OriPacketHeader.SrcBusID
}

func (t *Transaction) TransID() uint32 {
	return t.transID
}

func (t *Transaction) ParseMsg(data []byte, msg proto.Message) error {
	err := proto.Unmarshal(data, msg)
	if err != nil {
		t.Warningf("Fail to unmarshal req | %v", err)
		return err
	}
	t.Debugf("parse msg: %#v", msg)
	return nil
}

func (t *Transaction) SendMsgBack(pbMsg proto.Message) {
	router.SendMsgBack(t.OriPacketHeader, t.transID, pbMsg)
}

func (t *Transaction) CallMsgBySvrType(svrType uint32, cmd uint32, req proto.Message, rsp proto.Message) error {
	//t.DebugDepthf(1, "CallMsgBySvrType: %#v", req)
	t.sendSeq += 1
	err := router.SendPbMsgBySvrType(svrType, t.Uid(), cmd, t.sendSeq, t.TransID(), req)
	if err != nil {
		glog.Error(err)
		return err
	}

	return t.waitRsp(svrType, 0, cmd, time.Second*3, req, rsp)
}

func (t *Transaction) SendMsgBySvrType(svrType uint32, cmd uint32, req proto.Message) error {
	t.sendSeq += 1
	err := router.SendPbMsgBySvrType(svrType, t.Uid(), cmd, t.sendSeq, t.TransID(), req)
	if err != nil {
		glog.Error(err)
		return err
	}
	return nil
}

func (t *Transaction) SendPbMsgByBusId(busId uint32, cmd uint32, req proto.Message) error {
	t.sendSeq += 1
	err := router.SendPbMsgByBusId(busId, t.Uid(), cmd, t.sendSeq, t.TransID(), req)
	if err != nil {
		glog.Error(err)
		return err
	}
	return nil
}

func (t *Transaction) BroadcastByServerType(svrType uint32, cmd uint32, req proto.Message) error {
	t.Debugf("BroadcastByServerType: %#v", req)
	t.sendSeq += 1
	err := router.BroadcastPbMsgByServerType(svrType, t.Uid(), cmd, t.sendSeq, req)
	if err != nil {
		glog.Error(err)
	}
	return err
}

func (t *Transaction) CallMsgByBusId(busId uint32, cmd uint32, req proto.Message, rsp proto.Message) error {
	t.Debugf("CallMsgByBusId: %#v", req)
	t.sendSeq += 1
	err := router.SendPbMsgByBusId(busId, t.Uid(), cmd, t.sendSeq, t.TransID(), req)
	if err != nil {
		glog.Error(err)
		return err
	}

	return t.waitRsp(0, busId, cmd, time.Second*3, req, rsp)
}

func (t *Transaction) waitRsp(dstSvrType uint32, dstSvrIns uint32, cmd uint32,
	d time.Duration, req proto.Message, rsp proto.Message) error {
	ti := time.NewTimer(d)
	defer ti.Stop()
	for {
		select {
		case <-ti.C:
			glog.Errorf("timeout to CallMsgBySvrType {svrType:%v, svrIns:%v, uid:%v, cmd:%v, req:%#v}",
				dstSvrType, dstSvrIns, t.Uid(), cmd, req)
			return errors.New("timeout")
		case packet, ok := <-t.chanIn:
			if !ok {
				glog.Errorf("Failed to CallMsgBySvrType as chanInPacket is closed "+
					"{svrType:%v, svrIns:%v, uid:%v, cmd:%v, req:%#v}",
					dstSvrType, dstSvrIns, t.Uid(), cmd, req)
				return errors.New("channel is closed")
			}
			if packet.Header.CmdSeq != t.sendSeq || packet.Header.Cmd != cmd+1 {
				glog.Warningf("Received a packet which is not what I'm waiting for "+
					"{dstSvrType:%v, dstSvrIns:%v, uid:%v, cmd:%v, req:%#v, recvPacket:%#v}",
					dstSvrType, dstSvrIns, t.Uid(), cmd, req, packet.Header)
			} else {
				err := proto.Unmarshal(packet.Body, rsp)
				t.Debugf("Received a rsp: %#v", rsp)
				return err
			}
		}
		ti.Stop()
		ti = time.NewTimer(d)
	}
}