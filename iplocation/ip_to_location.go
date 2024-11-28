package iplocation

import (
	"bytes"
	"encoding/binary"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/transform"
	"io"
	"log"
	"net"
	"os"
	"regexp"
	"strings"
)

type Ip2LocMgr struct {
	Buf        []byte
	File       *bytes.Reader
	IndexStart int64
	IndexEnd   int64
}

type Ip2LocRsp struct {
	Ok          bool
	Ip          string
	AreaName    string
	AreaNameEng string
	AreaDesc    string
	OpDesc      string
	OpName      string
}

// 全国省份中文到拼音的对照表
var provinceMap = map[string]string{
	"北京": "beijing",
	"天津": "tianjin",
	"河北": "hebei",
	"山西": "shanxi",
	"内蒙": "neimeng",
	"辽宁": "liaoning",
	"吉林": "jilin",
	"黑龙": "heilongjiang",
	"上海": "shanghai",
	"江苏": "jiangsu",
	"浙江": "zhejiang",
	"安徽": "anhui",
	"福建": "fujian",
	"江西": "jiangxi",
	"山东": "shandong",
	"河南": "henan",
	"湖北": "hubei",
	"湖南": "hunan",
	"广东": "guangdong",
	"广西": "guangxi",
	"海南": "hainan",
	"重庆": "chongqing",
	"四川": "sichuan",
	"贵州": "guizhou",
	"云南": "yunnan",
	"西藏": "xizang",
	"陕西": "shaanxi",
	"甘肃": "gansu",
	"青海": "qinghai",
	"宁夏": "ningxia",
	"新疆": "xinjiang",
	"香港": "xianggang",
	"澳门": "aomen",
	"台湾": "taiwan",
	"海外": "haiwai",
}

func (m *Ip2LocMgr) Init(dbfile string) error {

	_file, err := os.Open(dbfile)
	if err != nil {
		return err
	}

	fileInfo, _ := _file.Stat()
	m.Buf = make([]byte, fileInfo.Size())
	_file.Read(m.Buf)
	m.File = bytes.NewReader(m.Buf)

	// header
	m.File.ReadAt(m.Buf[0:8], 0)

	m.IndexStart = int64(binary.LittleEndian.Uint32(m.Buf[0:4]))
	m.IndexEnd = int64(binary.LittleEndian.Uint32(m.Buf[4:8]))

	log.Printf("Index range: %d - %d", m.IndexStart, m.IndexEnd)

	return nil
}

func (m *Ip2LocMgr) Query(queryIp string) *Ip2LocRsp {
	ip := net.ParseIP(queryIp)
	ip4 := make([]byte, 4)
	ip4 = ip.To4() // := &net.IPAddr{IP:ip}
	//log.Printf("IP4: %#v",ip4)

	//log.Printf("%#v",req.ip)

	//二分法查找
	maxLoop := int64(32)
	head := m.IndexStart //+ 8
	tail := m.IndexEnd   //+ 8

	//是否找到
	got := false
	rpos := int64(0)

	for ; maxLoop >= 0 && len(ip4) == 4; maxLoop-- {
		idxNum := (tail - head) / 7
		pos := head + int64(idxNum/2)*7

		//pos += maxLoop*7

		m.File.ReadAt(m.Buf[0:7], pos)

		// startIP
		_ip := binary.LittleEndian.Uint32(m.Buf[0:4])

		//log.Printf("%d - INs:%d POS:%d %#v %d.%d.%d.%d",maxLoop,idxNum,pos,buf[0:7],_ip&0xff,_ip>>8&0xff,_ip>>16&0xff,_ip>>24&0xff)

		//记录位置
		_buf := append(m.Buf[4:7], 0x0) // 3byte + 1byte(0x00)
		rpos = int64(binary.LittleEndian.Uint32(_buf))
		//log.Printf("%d %#v",rpos,_buf)

		m.File.ReadAt(m.Buf[0:4], rpos)

		_ip2 := binary.LittleEndian.Uint32(m.Buf[0:4])

		//log.Printf("IP_END:%#v %d.%d.%d.%d",buf[0:4],_ip2&0xff,_ip2>>8&0xff,_ip2>>16&0xff,_ip2>>24&0xff)

		//查询的ip被转成大端了
		_ipq := binary.BigEndian.Uint32(ip4)

		//log.Printf("%d - IP_START: %#v",maxLoop,_ip)
		//log.Printf("%d - IP_END  : %#v",maxLoop,_ip2)
		//log.Printf("%d - IP_QUERY: %#v",maxLoop,_ipq)

		if _ipq > _ip2 {
			head = pos
			continue
		}

		if _ipq < _ip {
			tail = pos
			continue
		}

		// got
		got = true
		break
	}

	//log.Printf("GOT: %#v POS: %d", got, rpos)

	loc := Ip2LocRsp{
		Ok: false,
		Ip: queryIp,
	}
	if got {
		_loc := m.getIpLocation(rpos)

		var tr *transform.Reader
		tr = transform.NewReader(strings.NewReader(_loc.AreaDesc), simplifiedchinese.GBK.NewDecoder())

		if s, err := io.ReadAll(tr); err == nil {
			loc.AreaDesc = string(s)
		}

		tr = transform.NewReader(strings.NewReader(_loc.OpDesc), simplifiedchinese.GBK.NewDecoder())

		if s, err := io.ReadAll(tr); err == nil {
			loc.OpDesc = string(s)
		}

		loc.Ok = _loc.Ok
		re := regexp.MustCompile("(铁通|电信|联通|移动|网通)")
		loc.OpName = re.FindString(loc.OpDesc)
		re_area := regexp.MustCompile("(北京|天津|河北|山西|内蒙|辽宁|吉林|黑龙|上海|江苏|浙江|安徽|福建|江西|山东|河南|湖北|湖南|广东|广西|海南|重庆|四川|贵州|云南|西藏|陕西|甘肃|青海|宁夏|新疆|香港|澳门|台湾)")
		loc.AreaName = re_area.FindString(loc.AreaDesc)
		loc.AreaNameEng = provinceMap[loc.AreaName]
	}

	//log.Printf("LOC: %#v", loc)
	return &loc
}

func (m *Ip2LocMgr) getIpLocation(offset int64) (loc Ip2LocRsp) {

	buf := make([]byte, 1024)

	m.File.ReadAt(buf[0:1], offset+4)

	mod := buf[0]

	//log.Printf("C1 FLAG: %#v", mod)

	descOffset := int64(0)
	op_descOffset := int64(0)

	if 0x01 == mod {
		descOffset = m._readLong3(offset + 5)
		//log.Printf("Redirect to: %#v",descOffset);

		m.File.ReadAt(buf[0:1], descOffset)

		mod2 := buf[0]

		//log.Printf("C2 FLAG: %#v", mod2)

		if 0x02 == mod2 {
			loc.AreaDesc = m._readString(m._readLong3(descOffset + 1))
			op_descOffset = descOffset + 4
		} else {
			loc.AreaDesc = m._readString(descOffset)
			op_descOffset = descOffset + int64(len(loc.AreaDesc)) + 1
			// op_descOffset = file.Seek(0,1) // got the pos
			// log.Printf("cPOS: %#v aPOS: %#v err: %#v",descOffset,op_descOffset,err3)

		}

		loc.OpDesc = m._readArea(op_descOffset)

	} else if 0x02 == mod {
		loc.AreaDesc = m._readString(m._readLong3(offset + 5))
		loc.OpDesc = m._readArea(offset + 8)
	} else {
		loc.AreaDesc = m._readString(offset + 4)
		op_descOffset = offset + 4 + int64(len(loc.AreaDesc)) + 1
		//op_descOffset,_ = file.Seek(0,1)

		loc.OpDesc = m._readArea(op_descOffset)
	}

	loc.Ok = true

	return
}

func (m *Ip2LocMgr) _readArea(offset int64) string {
	buf := make([]byte, 4)

	m.File.ReadAt(buf[0:1], offset)

	mod := buf[0]

	//log.Printf("A FLAG: %#v", mod)

	if 0x01 == mod || 0x02 == mod {
		op_descOffset := m._readLong3(offset + 1)
		if op_descOffset == 0 {
			return ""
		} else {
			return m._readString(op_descOffset)
		}
	}
	return m._readString(offset)
}

func (m *Ip2LocMgr) _readLong3(offset int64) int64 {
	buf := make([]byte, 4)
	m.File.ReadAt(buf, offset)
	buf[3] = 0x00

	return int64(binary.LittleEndian.Uint32(buf))
}

func (m *Ip2LocMgr) _readString(offset int64) string {

	buf := make([]byte, 1024)
	got := int64(0)

	for ; got < 1024; got++ {
		m.File.ReadAt(buf[got:got+1], offset+got)

		if buf[got] == 0x00 {
			break
		}
	}

	return string(buf[0:got])
}
