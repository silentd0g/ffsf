package bus

// implType : args
// "rabbitmq": (rabbitmqAddr string)
func CreateBus(implType string, selfBusId uint32, onRecvMsg MsgHandler, args ...interface{}) IBus {
	switch implType {
	case "rabbitmq":
		return NewBusImplRabbitMQ(selfBusId, onRecvMsg, args[0].(string))
	case "rabbitmq_manual_ack":
		return NewBusImplRabbitMQManualAck(selfBusId, onRecvMsg, args[0].(string))
	default:
		return nil
	}
}
