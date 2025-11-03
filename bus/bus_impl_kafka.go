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
	selfBusId       uint32
	topic           string
	timeout         time.Duration
	chanOut         chan outMsg
	chanAck         chan kafkaAckMsg
	chanIn          chan kafka.Message // 新增：用于接收 Kafka 消息的 channel
	onRecv          MsgHandler
	autoAck         bool
	kafkaAddrs      []string
	reader          *kafka.Reader // 单分区 reader
	writer          *kafka.Writer
	ctx             context.Context
	cancel          context.CancelFunc
	wg              sync.WaitGroup
	ackMap          map[uint64]ackMapEntry // 使用 queueTag 作为 key
	ackMapMutex     sync.RWMutex
	closed          bool
	closeMutex      sync.RWMutex
	ackSeq          uint64 // 用于生成唯一的 queueTag
	ackSeqMutex     sync.Mutex
	consumerWorkers int // 并发消费者数量
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
	impl.consumerWorkers = 4 // 默认4个并发消费者

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
	impl.consumerWorkers = 4 // 默认4个并发消费者

	go impl.run()
	return impl
}

func (b *BusImplKafka) SelfBusId() uint32 {
	return b.selfBusId
}

func (b *BusImplKafka) SetReceiver(onRecvMsg MsgHandler) {
	b.onRecv = onRecvMsg
}

// SetConsumerWorkers 设置并发消费者数量（需要在连接前调用）
// 建议值：1-8，默认为4
func (b *BusImplKafka) SetConsumerWorkers(workers int) {
	if workers < 1 {
		workers = 1
	}
	if workers > 16 {
		workers = 16
	}
	b.consumerWorkers = workers
	logger.Infof("Set consumer workers to %d", workers)
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
		BatchTimeout: 1 * time.Nanosecond, // 极短超时，立即发送（应用层已做批量收集）
		BatchSize:    1,                   // 不在 Writer 层做批量，应用层已收集
		Async:        true,                // 异步写入，不阻塞主循环
		RequiredAcks: kafka.RequireOne,    // 只需要 leader 确认，提高性能
		// Compression:  kafka.Snappy,        // 启用压缩，提高网络效率
	}
	return nil
}

func (b *BusImplKafka) initKafkaReader() error {
	// 【重要优化】使用 Partition 模式而不是 Consumer Group 模式
	// 优点：启动快（< 1秒），无 rebalance 延迟
	// 只消费 partition 0（假设 topic 只有一个分区）

	logger.Infof("Kafka reader initializing (partition mode, no consumer group) for topic: %s...", b.topic)
	warmupStart := time.Now()

	b.reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:   b.kafkaAddrs,
		Topic:     b.topic,
		Partition: 0, // 固定使用 partition 0
		// 不设置 GroupID - 这是关键！不使用 Consumer Group 就不会有 rebalance
		MinBytes:       1,
		MaxBytes:       10e6, // 10MB
		MaxWait:        100 * time.Millisecond,
		StartOffset:    kafka.LastOffset, // 从最新消息开始（新消费者）
		CommitInterval: 0,                // 禁用自动提交
	})

	logger.Infof("Kafka reader ready (took %v): topic=%s, partition=0",
		time.Since(warmupStart), b.topic)

	return nil
}

// readLoop 读取消息的 goroutine
// 使用 ReadMessage 阻塞读取，配合手动 commit（CommitInterval=0）提高性能
func (b *BusImplKafka) readLoop() {
	b.wg.Add(1)
	defer b.wg.Done()

	logger.Infof("Kafka read loop started, waiting for messages...")
	firstMessage := true
	loopStartTime := time.Now()

	for {
		select {
		case <-b.ctx.Done():
			logger.Infof("Kafka read loop stopped")
			return
		default:
		}

		// 记录开始读取的时间（用于诊断）
		readStartTime := time.Now()

		// 使用 ReadMessage 阻塞读取（已通过 CommitInterval=0 禁用自动提交）
		// ReadMessage 内部有批量拉取优化，比 FetchMessage 更高效
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

		// 记录第一条消息的延迟，帮助诊断
		if firstMessage {
			logger.Infof("Kafka read loop: received FIRST message after %v (read took %v, partition=%d, offset=%d)",
				time.Since(loopStartTime), time.Since(readStartTime), msg.Partition, msg.Offset)
			firstMessage = false
		}

		// 将读取到的消息发送到 chanIn（非阻塞，优先处理消息）
		select {
		case b.chanIn <- msg:
			// 成功发送
		case <-b.ctx.Done():
			return
		}
	}
}

// messageWorker 并发处理消息的 worker
func (b *BusImplKafka) messageWorker(workerId int) {
	b.wg.Add(1)
	defer b.wg.Done()

	logger.Infof("Kafka message worker %d started", workerId)
	firstMessage := true
	workerStartTime := time.Now()

	for {
		select {
		case <-b.ctx.Done():
			logger.Infof("Kafka message worker %d stopped", workerId)
			return
		case msg, ok := <-b.chanIn:
			if !ok {
				logger.Infof("Kafka message worker %d: chanIn closed", workerId)
				return
			}

			// 记录第一条消息的处理时间
			if firstMessage {
				logger.Infof("Kafka message worker %d: processing FIRST message after %v",
					workerId, time.Since(workerStartTime))
				firstMessage = false
			}

			b.handleReceivedMessage(msg)
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

	// 启动消息读取 goroutine
	go b.readLoop()

	// 启动多个并发消息处理 worker
	for i := 0; i < b.consumerWorkers; i++ {
		go b.messageWorker(i)
	}

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

			// 批量收集消息以提高吞吐量
			kafkaMsgs := make([]kafka.Message, 0, 100)
			kafkaMsgs = append(kafkaMsgs, kafka.Message{
				Topic: calcTopicNameKafka(msgOut.busId),
				Value: msgOut.data,
			})

			// 非阻塞地尝试收集更多消息（最多99条，因为已有1条）
		collectMore:
			for i := 0; i < 99; i++ {
				select {
				case msgOut2, ok := <-b.chanOut:
					if !ok {
						return fmt.Errorf("chanOut of bus is closed")
					}
					kafkaMsgs = append(kafkaMsgs, kafka.Message{
						Topic: calcTopicNameKafka(msgOut2.busId),
						Value: msgOut2.data,
					})
				default:
					// chanOut 暂时没有更多消息，跳出收集循环
					break collectMore
				}
			}

			logger.Debugf("Send batch messages to Kafka. {batchSize:%v}", len(kafkaMsgs))

			// 批量写入消息（Async=true 时不会阻塞）
			ctx, cancel := context.WithTimeout(b.ctx, b.timeout)
			err := b.writer.WriteMessages(ctx, kafkaMsgs...)
			cancel()

			if err != nil {
				logger.Errorf("Failed to publish batch messages. {batchSize:%v, err:%v}",
					len(kafkaMsgs), err)
				// 发送失败返回错误，触发重连
				return fmt.Errorf("failed to write messages: %v", err)
			}

		case ackMsg, ok := <-b.chanAck:
			if !ok {
				return fmt.Errorf("chanAck of bus is closed")
			}

			b.ackMapMutex.RLock()
			entry, exists := b.ackMap[ackMsg.queueTag]
			b.ackMapMutex.RUnlock()

			if exists {
				// 注意：Partition 模式下不使用 CommitMessages（无 Consumer Group）
				// Ack 操作仅从 ackMap 中删除记录即可
				// Offset 由 Kafka 自动管理（每次从 LastOffset 开始）
				b.ackMapMutex.Lock()
				delete(b.ackMap, ackMsg.queueTag)
				b.ackMapMutex.Unlock()
				logger.Debugf("Acked message {queueTag:%d, partition:%d, offset:%d}",
					ackMsg.queueTag, entry.msg.Partition, entry.msg.Offset)
			} else {
				logger.Warningf("Ack message not found in ackMap {queueTag:%d}", ackMsg.queueTag)
			}

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

		// 注意：Partition 模式下不使用 CommitMessages
		// autoAck 模式下，消息处理完即视为已确认
		// 重启后从 LastOffset 开始，只接收新消息
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
