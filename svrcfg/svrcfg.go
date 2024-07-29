package svrcfg

import (
	"ffsf/logger"
	"github.com/golang/protobuf/proto"
	"github.com/samuel/go-zookeeper/zk"
	"io/ioutil"
	"os"
	"strings"
	"time"
)

// 从zookeeper节点获取配置信息
func ParseFromZookepper(host string, msg proto.Message) error {
	zkConn, _, err := zk.Connect([]string{host}, time.Second*5)
	if err != nil {
		logger.Errorf("connect zookeeper error. {err:%v}", err)
		return err
	}

	defer zkConn.Close()

	pname := os.Args[0]
	pos := strings.LastIndex(pname, "/")
	if pos >= 0 {
		pname = pname[pos+1:]
	}

	host, e := os.Hostname()
	if e != nil {
		logger.Errorf("get hostname error. {err:%s}", e.Error())
		return e
	}

	node := string("/conf/svr_conf/") + host + "_" + pname
	logger.Infof("{node:%s}", node)

	data, _, err := zkConn.Get(node)
	if err != nil {
		logger.Errorf("get node error，{node:%s, err:%v}", node, err.Error())
		return err
	}

	return ParseFromString(string(data), msg)
}

func ParseFromFile(config string, msg proto.Message) error {

	file, err := os.Open(config)
	if err != nil {
		logger.Errorf("open file err. {file:%s, err:%s}", config, err.Error())
		return err
	}

	defer file.Close()

	content, err := ioutil.ReadAll(file)
	if err != nil {
		logger.Errorf("read file error. {file:%s, err:%s}", config, err.Error())
		return err
	}

	return ParseFromString(string(content), msg)
}

func ParseFromString(content string, msg proto.Message) error {
	return proto.UnmarshalText(content, msg)
}
