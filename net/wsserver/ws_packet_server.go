package wsserver

import "github.com/gorilla/websocket"

type WsPacketInfo struct {
	HeaderLen int
	BodyLen   func([]byte) int
}

type IWsPacketSvrEventHandler interface {
	OnConn(*websocket.Conn)
	OnPacket(*websocket.Conn, []byte)
	OnClose(conn *websocket.Conn)
}

type WsPacketSvr struct {
	WsServer
	handler IWsPacketSvrEventHandler
}

func (s *WsPacketSvr) InitAndRun(ip string, port int, handler IWsPacketSvrEventHandler, useTls bool, certFile, keyFile string) error {
	s.handler = handler
	return s.WsServer.InitAndRun(ip, port, s, useTls, certFile, keyFile)
}

func (s *WsPacketSvr) OnConn(conn *websocket.Conn) {
	s.handler.OnConn(conn)
}

func (s *WsPacketSvr) OnRead(conn *websocket.Conn, data []byte) int {
	dataLen := len(data)
	s.handler.OnPacket(conn, data)
	return dataLen
}

func (s *WsPacketSvr) OnClose(conn *websocket.Conn) {
	s.handler.OnClose(conn)
}
