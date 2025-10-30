package bus

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/silentd0g/ffsf/logger"
	"github.com/silentd0g/ffsf/sharedstruct"
	"github.com/streadway/amqp"
)

type BusImplRabbitMQ struct {
	selfBusId uint32
	queueName string
	timeout   time.Duration
	chanOut   chan outMsg
	chanAck   chan uint64
	onRecv    MsgHandler
	autoAck   bool
}

func NewBusImplRabbitMQ(selfBusId uint32, onRecvMsg MsgHandler, rabbitmqAddr string) *BusImplRabbitMQ {
	impl := new(BusImplRabbitMQ)
	impl.selfBusId = selfBusId
	impl.queueName = calcQueueName(selfBusId)
	impl.timeout = 3 * time.Second
	impl.chanOut = make(chan outMsg, 10000)
	impl.chanAck = make(chan uint64, 10000)
	impl.onRecv = onRecvMsg
	impl.autoAck = true
	go impl.run(rabbitmqAddr)
	return impl
}

func NewBusImplRabbitMQManualAck(selfBusId uint32, onRecvMsg MsgHandler, rabbitmqAddr string) *BusImplRabbitMQ {
	impl := new(BusImplRabbitMQ)
	impl.selfBusId = selfBusId
	impl.queueName = calcQueueName(selfBusId)
	impl.timeout = 3 * time.Second
	impl.chanOut = make(chan outMsg, 10000)
	impl.chanAck = make(chan uint64, 10000)
	impl.onRecv = onRecvMsg
	impl.autoAck = false
	go impl.run(rabbitmqAddr)
	return impl
}

func (b *BusImplRabbitMQ) SelfBusId() uint32 {
	return b.selfBusId
}

func (b *BusImplRabbitMQ) SetReceiver(onRecvMsg MsgHandler) {
	b.onRecv = onRecvMsg
}

func (b *BusImplRabbitMQ) Send(dstBusId uint32, data1 []byte, data2 []byte) error {
	header := busPacketHeader{}
	header.version = 0
	header.passCode = passCode
	header.srcBusId = b.SelfBusId()
	header.dstBusId = dstBusId

	msg := outMsg{}
	msg.busId = dstBusId
	msg.data = make([]byte, byteLenOfBusPacketHeader()+len(data1)+len(data2))
	pos := 0
	header.To(msg.data[pos:])
	pos += byteLenOfBusPacketHeader()
	copy(msg.data[pos:], data1)
	pos += len(data1)
	if len(data2) > 0 {
		copy(msg.data[pos:], data2)
		pos += len(data2)
	}

	logger.Debugf("Send bus message. {len:%v, msg:%#v}", len(data1)+len(data2), header)

	if !sendToMsgChan(b.chanOut, msg, b.timeout) {
		return fmt.Errorf("bus.chanOut<-msg time out")
	} // msg所有权已转移，后面不能再使用msg

	return nil
}

func (b *BusImplRabbitMQ) Ack(queueTag uint64) error {
	t := time.NewTimer(b.timeout)
	defer t.Stop()
	select {
	case b.chanAck <- queueTag:
	case <-t.C:
		return fmt.Errorf("bus.chanAck<-queueTag time out")
	}

	return nil
}

// -------------------------------- private --------------------------------

const (
	passCode = 0xFEED
)

type busPacket struct {
	Header busPacketHeader
	Body   []byte
}

type busPacketHeader struct {
	version  uint16
	passCode uint16
	srcBusId uint32
	dstBusId uint32
	queueTag uint64
}

func byteLenOfBusPacketHeader() int {
	return 20
}

func (h *busPacketHeader) From(b []byte) {
	h.version = binary.BigEndian.Uint16(b[0:])
	h.passCode = binary.BigEndian.Uint16(b[2:])
	h.srcBusId = binary.BigEndian.Uint32(b[4:])
	h.dstBusId = binary.BigEndian.Uint32(b[8:])
	h.queueTag = binary.BigEndian.Uint64(b[12:])
}

func (h *busPacketHeader) To(b []byte) {
	binary.BigEndian.PutUint16(b[0:], h.version)
	binary.BigEndian.PutUint16(b[2:], h.passCode)
	binary.BigEndian.PutUint32(b[4:], h.srcBusId)
	binary.BigEndian.PutUint32(b[8:], h.dstBusId)
	binary.BigEndian.PutUint64(b[12:], h.queueTag)
}

type outMsg struct {
	busId uint32
	data  []byte
}

func calcQueueName(busId uint32) string {
	return "bus_" + fmt.Sprintf("%x", busId)
}

func sendToMsgChan(ch chan outMsg, msg outMsg, timeout time.Duration) bool {
	t := time.NewTimer(timeout)
	defer t.Stop()
	select {
	case ch <- msg:
	case <-t.C:
		return false
	}

	return true
}

func (b *BusImplRabbitMQ) process(rabbitmqAddr string) error {
	conn, err := amqp.Dial(rabbitmqAddr)
	if err != nil {
		return fmt.Errorf("failed to connect MQ {addr:%v} | %v", rabbitmqAddr, err)
	}
	defer conn.Close()
	logger.Infof("connected to %v", rabbitmqAddr)

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open a channel | %v", err)
	}
	defer ch.Close()

	queueArguments := amqp.Table{ // arguments
		"x-message-ttl":      int32(30 * 60 * 1000),   // 隔多长时间(ms)消息没被处理，则丢弃。
		"x-max-length-bytes": int32(10 * 1024 * 1024), // 消息队列所有包体大小（字节数）总和限制
		"x-overflow":         "reject-publish",        // 消息队列已满时的处理策略：新消息发送失败
	}
	q, err := ch.QueueDeclare(b.queueName, false, false, false, false, queueArguments)
	if err != nil {
		return fmt.Errorf("failed to declare a queue | %v", err)
	}

	chanRecv, err := ch.Consume(q.Name, "", b.autoAck, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("failed to register a consumer | %v", err)
	}

	for {
		select {
		case msgOut, ok := <-b.chanOut:
			if !ok {
				return fmt.Errorf("chanOut of bus is closed")
			}

			logger.Debugf("Send message to MQ. {dstBusId:0x%x, dataLen:%v}", msgOut.busId, len(msgOut.data))
			// send by routering
			err = ch.Publish(
				"",                          // exchange
				calcQueueName(msgOut.busId), // routing key
				false,                       // mandatory
				false,                       // immediate
				amqp.Publishing{
					// ContentType: "text/plain",
					Body: msgOut.data,
				})
			if err != nil {
				logger.Errorf("Failed to publish a message. {busId:%v, dataLen:%v, err:%v}",
					msgOut.busId, len(msgOut.data), err)
				// todo: is it necessary to return the err?
			}
		case delivery, ok := <-chanRecv:
			if !ok {
				return fmt.Errorf("chanRecv of bus is closed")
			}

			header := busPacketHeader{}
			header.From(delivery.Body)
			logger.Debugf("Received message from MQ. {header:%#v}", header)
			if header.passCode != passCode {
				logger.Warningf("Received a bus message with wrong pass code. {header:%#v}", header)
				break
			}

			if b.onRecv != nil {
				// 这里设置delivery.DeliveryTag到extra字段，方便后续Ack。
				packetHeader := new(sharedstruct.SSPacketHeader)
				packetHeader.From(delivery.Body[byteLenOfBusPacketHeader():])
				packetHeader.Extra = delivery.DeliveryTag
				packetHeader.To(delivery.Body[byteLenOfBusPacketHeader():])
				recvData := make([]byte, len(delivery.Body)-byteLenOfBusPacketHeader())
				copy(recvData, delivery.Body[byteLenOfBusPacketHeader():])
				// Todo:不确定delivery.Body的生命周期，保险起见，这里还是先拷贝了一份。
				b.onRecv(header.srcBusId, recvData)
			}
		case tag, ok := <-b.chanAck:
			if !ok {
				return fmt.Errorf("chanAck of bus is closed")
			}
			err = ch.Ack(tag, false)
			if err != nil {
				logger.Warningf("Failed to ack message {queueTag:0x%x, err:%v}", tag, err)
			}
		}
	}
}

func (b *BusImplRabbitMQ) run(rabbitmqAddr string) {
	retryCount := 0
	for {
		processStartTime := time.Now()

		err := b.process(rabbitmqAddr)

		if time.Since(processStartTime) > time.Minute {
			retryCount = 0 // 正常运行1分钟以上，则重置retryCount
		}
		retryCount++
		retryAfterSeconds := (retryCount - 1) * 2
		if retryAfterSeconds > 30 {
			retryAfterSeconds = 30
		}
		logger.Errorf("Error occur in processing bus. Retry later. {retryTimes: %v, afterSeconds:%v, err:%v}",
			retryCount, retryAfterSeconds, err)
		time.Sleep(time.Duration(retryAfterSeconds) * time.Second)
	}
}
