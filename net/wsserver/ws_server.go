package wsserver

import (
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/silentd0g/ffsf/logger"
)

type IWsServerEventHandler interface {
	OnConn(*websocket.Conn)             // 被Listener协程调用，一个TcpSvr对应一个Listener协程
	OnRead(*websocket.Conn, []byte) int // 被Read协程调用，每个Connection对应一个Read协调
	OnClose(*websocket.Conn)            // 被Read协程调用，每个Connection对应一个Read协调
}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024 * 1024 * 1,
	WriteBufferSize: 1024 * 1024 * 1,
	CheckOrigin: func(r *http.Request) bool {
		return true
		//		return r.Host == "www.example.com"
	},
	Error: func(w http.ResponseWriter, r *http.Request, status int, reason error) {
		logger.Errorf("websocket upgrade failed. {status=%v, reason=%v}", status, reason)
		http.Error(w, reason.Error(), status)
	},
}

type ClientInfo struct {
	//	Conn *wsserver.Conn
	chanWrite chan []byte
}

type WsServer struct {
	handler IWsServerEventHandler
	Clients map[*websocket.Conn]ClientInfo
	Mu      sync.RWMutex
}

func (s *WsServer) WriteData(c *websocket.Conn, data1, data2 []byte) error {
	var chanWrite chan []byte = nil
	s.Mu.RLock()
	if info, ok := s.Clients[c]; ok {
		chanWrite = info.chanWrite
	}
	s.Mu.RUnlock()
	if chanWrite == nil {
		return fmt.Errorf("connection doesn't exist")
	}

	data := make([]byte, len(data1)+len(data2))
	pos := 0
	copy(data[pos:], data1)
	pos += len(data1)
	copy(data[pos:], data2)
	pos += len(data2)

	t := time.NewTimer(3 * time.Second)
	defer t.Stop()
	select {
	case chanWrite <- data:
	case <-t.C:
		return fmt.Errorf("time out in 3 seconds")
	}
	return nil
}

func (s *WsServer) Close(c *websocket.Conn) error {
	var chanWrite chan []byte = nil

	s.Mu.RLock()
	info, exists := s.Clients[c]
	if exists {
		chanWrite = info.chanWrite
	}
	s.Mu.RUnlock()

	if chanWrite == nil {
		return fmt.Errorf("connection doesn't exist")
	}

	t := time.NewTimer(3 * time.Second)
	defer t.Stop()
	select {
	case chanWrite <- nil:
	case <-t.C:
		return fmt.Errorf("time out in 3 seconds")
	}

	return nil
}

func (s *WsServer) WsHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		logger.Debugf("Upgrade failed. {err:%v, local:%v, remote:%v}", err, conn.LocalAddr(), conn.RemoteAddr())
		return
	}
	s.addClient(conn)
	for {
		messageType, p, err := conn.ReadMessage()
		if err != nil {
			logger.Debugf("ReadMessage failed. {err:%v, local:%v, remote:%v}", err, conn.LocalAddr(), conn.RemoteAddr())
			s.Close(conn)
			return
		}
		if messageType == websocket.CloseMessage {
			s.Close(conn)
			logger.Debugf("CloseMessage received. {local:%v, remote:%v}", conn.LocalAddr(), conn.RemoteAddr())
			return
		}
		s.handler.OnRead(conn, p)
	}
}

func (s *WsServer) InitAndRun(ip string, port int, handler IWsServerEventHandler, useTls bool, certFile, keyFile string) error {
	s.Clients = make(map[*websocket.Conn]ClientInfo)
	s.handler = handler

	http.HandleFunc("/", s.WsHandler)
	addr := ip + ":" + strconv.Itoa(port)
	if useTls {
		logger.Infof("Listening with TLS. {addr:%s, certFile:%s, keyFile:%s}", addr, certFile, keyFile)
		go s.ListenAndServeTLS(addr, certFile, keyFile)
	} else {
		logger.Infof("Listening. {addr:%s}", addr)
		go s.ListenAndServe(addr)
	}
	return nil
}

func (s *WsServer) ListenAndServe(addr string) {
	err := http.ListenAndServe(addr, nil)
	if err != nil {
		logger.Errorf("ListenAndServe failed. {err:%v}", err)
	}
}

func (s *WsServer) ListenAndServeTLS(addr string, certFile string, keyFile string) {
	err := http.ListenAndServeTLS(addr, certFile, keyFile, nil)
	if err != nil {
		logger.Errorf("ListenAndServeTLS failed. {err:%v}", err)
	}
}

func (s *WsServer) runConnWrite(c *websocket.Conn) {
	for data := range s.Clients[c].chanWrite {
		if data == nil {
			logger.Infof("A 'nil' is passed to chanWrite to close conn. {local:%v, remote:%v}",
				c.LocalAddr(), c.RemoteAddr())
			c.Close()
			s.removeClient(c)
			return
		}
		err := c.WriteMessage(websocket.BinaryMessage, data)
		if err != nil {
			logger.Errorf("WriteMessage failed. {err:%v, local:%v, remote:%v}", err, c.LocalAddr(), c.RemoteAddr())
			c.Close()
			s.removeClient(c)
			return
		}
	}
	logger.Debugf("chanWrite is closed. {local:%v, remote:%v}", c.LocalAddr(), c.RemoteAddr())
}

func (s *WsServer) addClient(c *websocket.Conn) {
	logger.Debugf("addClient {local:%v, remote:%v}", c.LocalAddr(), c.RemoteAddr())
	s.Mu.Lock()
	defer s.Mu.Unlock()
	// 判断是否存在链接，不存在就加进去
	if _, ok := s.Clients[c]; !ok {
		s.Clients[c] = ClientInfo{
			chanWrite: make(chan []byte, 100),
		}
		s.handler.OnConn(c)
		go s.runConnWrite(c)
	}
}

func (s *WsServer) removeClient(c *websocket.Conn) {
	logger.Debugf("removeClient {local:%v, remote:%v}", c.LocalAddr(), c.RemoteAddr())
	s.Mu.Lock()
	defer s.Mu.Unlock()
	if _, ok := s.Clients[c]; ok {
		s.handler.OnClose(c)
		close(s.Clients[c].chanWrite)
		delete(s.Clients, c)
	}
}
