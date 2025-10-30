package bus

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/silentd0g/ffsf/logger"
	"github.com/silentd0g/ffsf/sharedstruct"
)

type BusImplKafka struct {
	selfBusId   uint32
	topic       string
	timeout     time.Duration
	chanOut     chan outMsg
	chanAck     chan kafkaAckMsg
	onRecv      MsgHandler
	autoAck     bool
	kafkaAddr   string
	reader      *kafka.Reader
	writer      *kafka.Writer
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	ackMap      map[string]kafka.Message // 存储待确认的消息
	ackMapMutex sync.RWMutex
}

type kafkaAckMsg struct {
	partition int
	offset    int64
}

func NewBusImplKafka(selfBusId uint32, onRecvMsg MsgHandler, kafkaAddr string) *BusImplKafka {
	impl := new(BusImplKafka)
	impl.selfBusId = selfBusId
	impl.topic = calcTopicNameKafka(selfBusId)
	impl.timeout = 3 * time.Second
	impl.chanOut = make(chan outMsg, 10000)
	impl.chanAck = make(chan kafkaAckMsg, 10000)
	impl.onRecv = onRecvMsg
	impl.autoAck = true
	impl.kafkaAddr = kafkaAddr
	impl.ackMap = make(map[string]kafka.Message)
	impl.ctx, impl.cancel = context.WithCancel(context.Background())

	go impl.run()
	return impl
}

func NewBusImplKafkaManualAck(selfBusId uint32, onRecvMsg MsgHandler, kafkaAddr string) *BusImplKafka {
	impl := new(BusImplKafka)
	impl.selfBusId = selfBusId
	impl.topic = calcTopicNameKafka(selfBusId)
	impl.timeout = 3 * time.Second
	impl.chanOut = make(chan outMsg, 10000)
	impl.chanAck = make(chan kafkaAckMsg, 10000)
	impl.onRecv = onRecvMsg
	impl.autoAck = false
	impl.kafkaAddr = kafkaAddr
	impl.ackMap = make(map[string]kafka.Message)
	impl.ctx, impl.cancel = context.WithCancel(context.Background())

	go impl.run()
	return impl
}

func (b *BusImplKafka) SelfBusId() uint32 {
	return b.selfBusId
}

func (b *BusImplKafka) calcTopicNameKafka(busId uint32) string {
	return "bus_" + fmt.Sprintf("%x", busId)
}

func (b *BusImplKafka) SetReceiver(onRecvMsg MsgHandler) {
	b.onRecv = onRecvMsg
}

func (b *BusImplKafka) Send(dstBusId uint32, data1 []byte, data2 []byte) error {
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

	if !b.sendToMsgChan(b.chanOut, msg, b.timeout) {
		return fmt.Errorf("bus.chanOut<-msg time out")
	}

	return nil
}

func (b *BusImplKafka) Ack(queueTag uint64) error {
	// 将 queueTag 解析为 partition 和 offset
	partition := int(queueTag >> 32)
	offset := int64(queueTag & 0xFFFFFFFF)

	ackMsg := kafkaAckMsg{
		partition: partition,
		offset:    offset,
	}

	t := time.NewTimer(b.timeout)
	defer t.Stop()
	select {
	case b.chanAck <- ackMsg:
	case <-t.C:
		return fmt.Errorf("bus.chanAck<-ackMsg time out")
	}

	return nil
}

func (b *BusImplKafka) Close() error {
	b.cancel()
	b.wg.Wait()

	if b.reader != nil {
		b.reader.Close()
	}
	if b.writer != nil {
		b.writer.Close()
	}

	close(b.chanOut)
	close(b.chanAck)

	return nil
}

// -------------------------------- private --------------------------------

func calcTopicNameKafka(busId uint32) string {
	return "bus_" + fmt.Sprintf("%x", busId)
}

func (b *BusImplKafka) sendToMsgChan(ch chan outMsg, msg outMsg, timeout time.Duration) bool {
	t := time.NewTimer(timeout)
	defer t.Stop()
	select {
	case ch <- msg:
	case <-t.C:
		return false
	}

	return true
}

func (b *BusImplKafka) initKafkaWriter() error {
	b.writer = &kafka.Writer{
		Addr:         kafka.TCP(b.kafkaAddr),
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 10 * time.Millisecond,
		BatchSize:    100,
	}
	return nil
}

func (b *BusImplKafka) initKafkaReader() error {

	b.reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{b.kafkaAddr},
		Topic:       b.topic,
		GroupID:     fmt.Sprintf("bus_group_%x", b.selfBusId),
		MinBytes:    1,
		MaxBytes:    10e6, // 10MB
		MaxWait:     1 * time.Second,
		StartOffset: kafka.LastOffset,
	})

	return nil
}

func (b *BusImplKafka) process() error {
	// 初始化 Kafka writer 和 reader
	if err := b.initKafkaWriter(); err != nil {
		return fmt.Errorf("failed to init kafka writer: %v", err)
	}

	if err := b.initKafkaReader(); err != nil {
		return fmt.Errorf("failed to init kafka reader: %v", err)
	}

	logger.Infof("connected to kafka %v", b.kafkaAddr)

	for {
		select {
		case <-b.ctx.Done():
			return fmt.Errorf("context cancelled")

		case msgOut, ok := <-b.chanOut:
			if !ok {
				return fmt.Errorf("chanOut of bus is closed")
			}

			logger.Debugf("Send message to Kafka. {dstBusId:0x%x, dataLen:%v}", msgOut.busId, len(msgOut.data))

			kafkaMsg := kafka.Message{
				Topic: b.topic,
				Value: msgOut.data,
			}

			ctx, cancel := context.WithTimeout(b.ctx, b.timeout)
			err := b.writer.WriteMessages(ctx, kafkaMsg)
			cancel()

			if err != nil {
				logger.Errorf("Failed to publish a message. {busId:%v, dataLen:%v, err:%v}",
					msgOut.busId, len(msgOut.data), err)
			}

		case ackMsg, ok := <-b.chanAck:
			if !ok {
				return fmt.Errorf("chanAck of bus is closed")
			}

			// 构造消息键用于查找待确认的消息
			msgKey := fmt.Sprintf("%d_%d", ackMsg.partition, ackMsg.offset)

			b.ackMapMutex.RLock()
			msg, exists := b.ackMap[msgKey]
			b.ackMapMutex.RUnlock()

			if exists {
				err := b.reader.CommitMessages(b.ctx, msg)
				if err != nil {
					logger.Warningf("Failed to ack message {partition:%d, offset:%d, err:%v}",
						ackMsg.partition, ackMsg.offset, err)
				} else {
					// 成功确认后从 map 中删除
					b.ackMapMutex.Lock()
					delete(b.ackMap, msgKey)
					b.ackMapMutex.Unlock()
				}
			}

		default:
			// 尝试接收消息
			ctx, cancel := context.WithTimeout(b.ctx, 100*time.Millisecond)
			msg, err := b.reader.ReadMessage(ctx)
			cancel()

			if err != nil {
				if err == context.DeadlineExceeded {
					continue // 超时是正常的，继续循环
				}
				return fmt.Errorf("failed to read message: %v", err)
			}

			b.handleReceivedMessage(msg)
		}
	}
}

func (b *BusImplKafka) handleReceivedMessage(msg kafka.Message) {
	if len(msg.Value) < byteLenOfBusPacketHeader() {
		logger.Warningf("Received message too short: %d bytes", len(msg.Value))
		return
	}

	header := busPacketHeader{}
	header.From(msg.Value)
	logger.Debugf("Received message from Kafka. {header:%#v}", header)

	if header.passCode != passCode {
		logger.Warningf("Received a bus message with wrong pass code. {header:%#v}", header)
		return
	}

	if b.onRecv != nil {
		// 如果是手动确认模式，将消息存储到 ackMap 中
		if !b.autoAck {
			// 将 partition 和 offset 编码到 queueTag 中
			queueTag := uint64(msg.Partition)<<32 | uint64(msg.Offset)
			msgKey := fmt.Sprintf("%d_%d", msg.Partition, msg.Offset)

			b.ackMapMutex.Lock()
			b.ackMap[msgKey] = msg
			b.ackMapMutex.Unlock()

			// 设置 queueTag 到 packet header 的 extra 字段
			packetHeader := new(sharedstruct.SSPacketHeader)
			packetHeader.From(msg.Value[byteLenOfBusPacketHeader():])
			packetHeader.Extra = queueTag
			packetHeader.To(msg.Value[byteLenOfBusPacketHeader():])
		}

		recvData := make([]byte, len(msg.Value)-byteLenOfBusPacketHeader())
		copy(recvData, msg.Value[byteLenOfBusPacketHeader():])
		b.onRecv(header.srcBusId, recvData)

		// 如果是自动确认模式，立即确认消息
		if b.autoAck {
			err := b.reader.CommitMessages(b.ctx, msg)
			if err != nil {
				logger.Warningf("Failed to auto-ack message {partition:%d, offset:%d, err:%v}",
					msg.Partition, msg.Offset, err)
			}
		}
	}
}

func (b *BusImplKafka) run() {
	b.wg.Add(1)
	defer b.wg.Done()

	retryCount := 0
	for {
		select {
		case <-b.ctx.Done():
			return
		default:
		}

		processStartTime := time.Now()
		err := b.process()

		if b.ctx.Err() != nil {
			return // 正常退出
		}

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

		select {
		case <-b.ctx.Done():
			return
		case <-time.After(time.Duration(retryAfterSeconds) * time.Second):
		}
	}
}
