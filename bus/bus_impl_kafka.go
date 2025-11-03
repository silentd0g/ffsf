package bus

import (
	"context"
	"errors"
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
	chanIn      chan kafka.Message // 新增：用于接收 Kafka 消息的 channel
	onRecv      MsgHandler
	autoAck     bool
	kafkaAddrs  []string
	reader      *kafka.Reader
	writer      *kafka.Writer
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	ackMap      map[uint64]ackMapEntry // 使用 queueTag 作为 key
	ackMapMutex sync.RWMutex
	closed      bool
	closeMutex  sync.RWMutex
	ackSeq      uint64 // 用于生成唯一的 queueTag
	ackSeqMutex sync.Mutex
}

type ackMapEntry struct {
	msg       kafka.Message
	timestamp time.Time
}

type kafkaAckMsg struct {
	queueTag uint64
}

func NewBusImplKafka(selfBusId uint32, onRecvMsg MsgHandler, kafkaAddrs []string) *BusImplKafka {
	impl := new(BusImplKafka)
	impl.selfBusId = selfBusId
	impl.topic = calcTopicNameKafka(selfBusId)
	impl.timeout = 3 * time.Second
	impl.chanOut = make(chan outMsg, 10000)
	impl.chanAck = make(chan kafkaAckMsg, 10000)
	impl.chanIn = make(chan kafka.Message, 10000) // 初始化接收消息的 channel
	impl.onRecv = onRecvMsg
	impl.autoAck = true
	impl.kafkaAddrs = kafkaAddrs
	impl.ackMap = make(map[uint64]ackMapEntry)
	impl.ctx, impl.cancel = context.WithCancel(context.Background())
	impl.closed = false
	impl.ackSeq = 0

	go impl.run()
	return impl
}

func NewBusImplKafkaManualAck(selfBusId uint32, onRecvMsg MsgHandler, kafkaAddrs []string) *BusImplKafka {
	impl := new(BusImplKafka)
	impl.selfBusId = selfBusId
	impl.topic = calcTopicNameKafka(selfBusId)
	impl.timeout = 3 * time.Second
	impl.chanOut = make(chan outMsg, 10000)
	impl.chanAck = make(chan kafkaAckMsg, 10000)
	impl.chanIn = make(chan kafka.Message, 10000) // 初始化接收消息的 channel
	impl.onRecv = onRecvMsg
	impl.autoAck = false
	impl.kafkaAddrs = kafkaAddrs
	impl.ackMap = make(map[uint64]ackMapEntry)
	impl.ctx, impl.cancel = context.WithCancel(context.Background())
	impl.closed = false
	impl.ackSeq = 0

	go impl.run()
	return impl
}

func (b *BusImplKafka) SelfBusId() uint32 {
	return b.selfBusId
}

func (b *BusImplKafka) SetReceiver(onRecvMsg MsgHandler) {
	b.onRecv = onRecvMsg
}

func (b *BusImplKafka) Send(dstBusId uint32, data1 []byte, data2 []byte) error {
	b.closeMutex.RLock()
	if b.closed {
		b.closeMutex.RUnlock()
		return fmt.Errorf("bus is closed")
	}
	b.closeMutex.RUnlock()

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
	b.closeMutex.RLock()
	if b.closed {
		b.closeMutex.RUnlock()
		return fmt.Errorf("bus is closed")
	}
	b.closeMutex.RUnlock()

	ackMsg := kafkaAckMsg{
		queueTag: queueTag,
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
	b.closeMutex.Lock()
	if b.closed {
		b.closeMutex.Unlock()
		return nil
	}
	b.closed = true
	b.closeMutex.Unlock()

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
	close(b.chanIn)

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
		Addr:         kafka.TCP(b.kafkaAddrs...),
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 10 * time.Millisecond,
		BatchSize:    100,
	}
	return nil
}

func (b *BusImplKafka) initKafkaReader() error {
	b.reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:  b.kafkaAddrs,
		Topic:    b.topic,
		GroupID:  fmt.Sprintf("bus_group_%x", b.selfBusId),
		MinBytes: 1,
		MaxBytes: 10e6, // 10MB
		MaxWait:  100 * time.Millisecond,
		// 使用 Consumer Group 的 offset 管理，避免丢失历史消息
		// StartOffset: kafka.LastOffset, // 已注释掉，依赖 GroupID 的 offset
	})

	// 预热步骤：主动建立连接，避免第一条消息延迟
	// 通过尝试读取一条消息（超时时间短）来触发连接建立和 Consumer Group 初始化
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err := b.reader.FetchMessage(ctx)
	// 忽略错误（可能是超时或没有消息），只要连接已经建立即可
	if err != nil {
		if !errors.Is(err, context.DeadlineExceeded) {
			logger.Debugf("Kafka reader warmup attempt: %v", err)
		}
	} else {
		// 如果成功读取到消息，记录日志但不处理，留给后续的 ReadMessage 处理
		logger.Debugf("Kafka reader warmup succeeded, connection established")
	}

	logger.Infof("Kafka reader initialized for topic: %s", b.topic)

	return nil
}

// readLoop 独立的 goroutine，专门负责从 Kafka 读取消息并发送到 chanIn
func (b *BusImplKafka) readLoop() {
	b.wg.Add(1)
	defer b.wg.Done()

	logger.Infof("Kafka read loop started")

	for {
		select {
		case <-b.ctx.Done():
			logger.Infof("Kafka read loop stopped")
			return
		default:
		}

		// 阻塞读取消息，不设置超时，让 ReadMessage 自己处理
		msg, err := b.reader.ReadMessage(b.ctx)
		if err != nil {
			if b.ctx.Err() != nil {
				// context 已取消，正常退出
				return
			}
			// 读取错误，记录日志并继续
			logger.Errorf("Failed to read message from Kafka: %v", err)
			time.Sleep(time.Second) // 短暂休眠后重试
			continue
		}

		// 将读取到的消息发送到 chanIn
		select {
		case b.chanIn <- msg:
			// 成功发送
		case <-b.ctx.Done():
			return
		}
	}
}

func (b *BusImplKafka) process() error {
	// 初始化 Kafka writer 和 reader
	if err := b.initKafkaWriter(); err != nil {
		return fmt.Errorf("failed to init kafka writer: %v", err)
	}

	if err := b.initKafkaReader(); err != nil {
		// 如果 reader 初始化失败，需要清理 writer
		if b.writer != nil {
			b.writer.Close()
			b.writer = nil
		}
		return fmt.Errorf("failed to init kafka reader: %v", err)
	}

	logger.Infof("connected to kafka %v", b.kafkaAddrs)

	// 启动独立的消息读取 goroutine
	go b.readLoop()

	// 定时器：用于定期清理过期的 ackMap 条目
	cleanupTicker := time.NewTicker(30 * time.Second)
	defer cleanupTicker.Stop()

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
				Topic: calcTopicNameKafka(msgOut.busId),
				Value: msgOut.data,
			}

			ctx, cancel := context.WithTimeout(b.ctx, b.timeout)
			err := b.writer.WriteMessages(ctx, kafkaMsg)
			cancel()

			if err != nil {
				logger.Errorf("Failed to publish a message. {busId:%v, dataLen:%v, err:%v}",
					msgOut.busId, len(msgOut.data), err)
				// 发送失败返回错误，触发重连
				return fmt.Errorf("failed to write message: %v", err)
			}

		case ackMsg, ok := <-b.chanAck:
			if !ok {
				return fmt.Errorf("chanAck of bus is closed")
			}

			b.ackMapMutex.RLock()
			entry, exists := b.ackMap[ackMsg.queueTag]
			b.ackMapMutex.RUnlock()

			if exists {
				err := b.reader.CommitMessages(b.ctx, entry.msg)
				if err != nil {
					logger.Warningf("Failed to ack message {queueTag:%d, partition:%d, offset:%d, err:%v}",
						ackMsg.queueTag, entry.msg.Partition, entry.msg.Offset, err)
				} else {
					// 成功确认后从 map 中删除
					b.ackMapMutex.Lock()
					delete(b.ackMap, ackMsg.queueTag)
					b.ackMapMutex.Unlock()
				}
			} else {
				logger.Warningf("Ack message not found in ackMap {queueTag:%d}", ackMsg.queueTag)
			}

		case msg, ok := <-b.chanIn:
			if !ok {
				return fmt.Errorf("chanIn of bus is closed")
			}
			b.handleReceivedMessage(msg)

		case <-cleanupTicker.C:
			// 定期清理过期的 ackMap 条目
			b.cleanupExpiredAckEntries()
		}
	}
}

// 清理超过5分钟未确认的消息
func (b *BusImplKafka) cleanupExpiredAckEntries() {
	expireTime := time.Now().Add(-5 * time.Minute)

	b.ackMapMutex.Lock()
	defer b.ackMapMutex.Unlock()

	expiredCount := 0
	for queueTag, entry := range b.ackMap {
		if entry.timestamp.Before(expireTime) {
			delete(b.ackMap, queueTag)
			expiredCount++
			logger.Warningf("Removed expired ack entry {queueTag:%d, partition:%d, offset:%d}",
				queueTag, entry.msg.Partition, entry.msg.Offset)
		}
	}

	if expiredCount > 0 {
		logger.Infof("Cleaned up %d expired ack entries", expiredCount)
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
		// 先拷贝数据
		recvData := make([]byte, len(msg.Value)-byteLenOfBusPacketHeader())
		copy(recvData, msg.Value[byteLenOfBusPacketHeader():])

		// 如果是手动确认模式，将消息存储到 ackMap 中
		if !b.autoAck {
			// 生成唯一的 queueTag
			b.ackSeqMutex.Lock()
			b.ackSeq++
			queueTag := b.ackSeq
			b.ackSeqMutex.Unlock()

			b.ackMapMutex.Lock()
			b.ackMap[queueTag] = ackMapEntry{
				msg:       msg,
				timestamp: time.Now(),
			}
			b.ackMapMutex.Unlock()

			// 设置 queueTag 到 packet header 的 extra 字段
			packetHeader := new(sharedstruct.SSPacketHeader)
			packetHeader.From(recvData)
			packetHeader.Extra = queueTag
			packetHeader.To(recvData)
		}

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
