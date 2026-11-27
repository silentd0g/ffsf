// 本文件为带地址的外部总线维护在线实例及机器地址，旧 MQ 沿用原来的发现路径。
package svrinstmgr

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net/netip"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/samuel/go-zookeeper/zk"
	"github.com/silentd0g/ffsf/logger"
)

var ErrRouteNotReady = errors.New("server address directory is not ready")
var ErrNoRoute = errors.New("server has no unique agent address")

// Endpoint 返回本机已注册的地址及其变化通知。每次注册变化都关闭上一次的通知通道。
type Endpoint interface {
	Endpoint() (netip.Addr, <-chan struct{})
	SetRouteReady(generation <-chan struct{}, ready bool)
}

type AddressDirectory struct {
	instances  *ServerInstanceMgr
	conn       *zk.Conn
	root, self string
	selfID     uint32
	mu         sync.RWMutex
	ready      bool
	addresses  map[uint32]netip.Addr
}

func (s *ServerInstanceMgr) NewAddressDirectory(self string, rules map[uint32]uint32) (*AddressDirectory, error) {
	id, err := netip.ParseAddr(self)
	if err != nil || !id.Is4() || id.As4() == [4]byte{} {
		return nil, fmt.Errorf("invalid self BusID %q", self)
	}
	s.routeRules = rules
	octets := id.As4()
	return &AddressDirectory{selfID: binary.BigEndian.Uint32(octets[:]), instances: s, root: fmt.Sprintf("/online_%03d", id.As4()[0]), self: self}, nil
}

func (d *AddressDirectory) Resolve(id uint32) (netip.Addr, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if !d.ready || d.conn == nil || d.conn.State() != zk.StateHasSession {
		return netip.Addr{}, ErrRouteNotReady
	}
	ip, ok := d.addresses[id]
	if !ok {
		return netip.Addr{}, fmt.Errorf("%w: %08x", ErrNoRoute, id)
	}
	return ip, nil
}

type addressRecord struct {
	id    uint32
	ip    netip.Addr
	owner int64
}

func decodeNode(name string, data []byte, owner int64) (addressRecord, bool) {
	at := strings.LastIndexByte(name, '.')
	if at < 0 || len(name)-at-1 != 10 {
		return addressRecord{}, false
	}
	for _, c := range name[at+1:] {
		if c < '0' || c > '9' {
			return addressRecord{}, false
		}
	}
	addr, err := netip.ParseAddr(name[:at])
	if err != nil || !addr.Is4() {
		return addressRecord{}, false
	}
	b := addr.As4()
	id := uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
	ip, _ := netip.ParseAddr(string(data))
	return addressRecord{id, ip, owner}, id != 0
}

func addressSnapshot(records map[string]addressRecord) (map[uint32][]uint32, map[uint32]netip.Addr) {
	instances := make(map[uint32][]uint32)
	addresses := make(map[uint32]netip.Addr)
	present := make(map[uint32]bool)
	for _, r := range records {
		if present[r.id] {
			delete(addresses, r.id)
			continue
		}
		present[r.id] = true
		kind := r.id >> 8 & 255
		instances[kind] = append(instances[kind], r.id)
		// 框架也支持隔离开发环境中的回环单播地址，部署方限制环境内可达的机器。
		if r.ip.Is4() && (r.ip.IsGlobalUnicast() || r.ip.IsLoopback()) {
			addresses[r.id] = r.ip
		}
	}
	for _, list := range instances {
		sort.Slice(list, func(i, j int) bool { return list[i] < list[j] })
	}
	return instances, addresses
}

// Run 等待第一次可用的注册及地址快照，随后由同一协程负责重连和临时节点的所有权。
func (d *AddressDirectory) Run(ctx context.Context, servers []string, ep Endpoint) error {
	conn, events, err := zk.Connect(servers, 10*time.Second)
	if err != nil {
		return err
	}
	d.mu.Lock()
	d.conn = conn
	d.mu.Unlock()
	ready := make(chan struct{})
	go func() { defer conn.Close(); d.watch(ctx, events, ep, ready) }()
	select {
	case <-ready:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(30 * time.Second):
		return errors.New("timed out waiting for local bus and ZooKeeper registration")
	}
}

func (d *AddressDirectory) disable(ep Endpoint, gen <-chan struct{}) {
	d.mu.Lock()
	d.ready = false
	d.mu.Unlock()
	ep.SetRouteReady(gen, false)
}

func (d *AddressDirectory) watch(ctx context.Context, events <-chan zk.Event, ep Endpoint, ready chan struct{}) {
	var watch <-chan zk.Event
	var retry <-chan time.Time
	var cached map[string]addressRecord
	refresh := true
	for {
		ip, gen := ep.Endpoint()
		if refresh && d.conn.State() == zk.StateHasSession {
			next, records, changed, err := d.refresh(ip, cached)
			if err != nil {
				d.disable(ep, gen)
				logger.Warningf("agent discovery refresh failed: %v", err)
				retry = time.After(300 * time.Millisecond)
			} else {
				watch, cached, retry = next, records, nil
				instances, addresses := addressSnapshot(records)
				active := !changed && ip.IsValid() && addresses[d.selfID] == ip
				select {
				case <-gen:
					active = false
				default:
				}
				d.mu.Lock()
				d.instances.lock.Lock()
				d.instances.mapSvrTypeToIns = instances
				d.addresses = addresses
				d.ready = active
				d.instances.lock.Unlock()
				d.mu.Unlock()
				ep.SetRouteReady(gen, active)
				if active && ready != nil {
					close(ready)
					ready = nil
				}
			}
		}
		refresh = false
		select {
		case <-ctx.Done():
			d.disable(ep, gen)
			return
		case ev, ok := <-events:
			if !ok {
				d.disable(ep, gen)
				return
			}
			if ev.Type == zk.EventSession {
				if ev.State != zk.StateHasSession {
					d.disable(ep, gen)
				}
				watch, cached = nil, nil
				refresh = true
			}
		case <-gen:
			d.disable(ep, gen)
			refresh = true
		case <-watch:
			watch = nil
			refresh = true
		case <-retry:
			retry = nil
			refresh = true
		}
	}
}

func (d *AddressDirectory) refresh(ip netip.Addr, cached map[string]addressRecord) (<-chan zk.Event, map[string]addressRecord, bool, error) {
	names, _, watch, err := d.conn.ChildrenW(d.root)
	if err == zk.ErrNoNode {
		_, err = d.conn.Create(d.root, nil, 0, zk.WorldACL(zk.PermAll))
		if err != nil && err != zk.ErrNodeExists {
			return nil, nil, false, err
		}
		names, _, watch, err = d.conn.ChildrenW(d.root)
	}
	if err != nil {
		return nil, nil, false, err
	}
	records := make(map[string]addressRecord, len(names))
	own := false
	changed := false
	for _, name := range names {
		record, ok := cached[name]
		if !ok {
			data, stat, err := d.conn.Get(d.root + "/" + name)
			if err == zk.ErrNoNode {
				continue
			}
			if err != nil {
				return nil, nil, false, err
			}
			record, ok = decodeNode(name, data, stat.EphemeralOwner)
			if !ok {
				continue
			}
		}
		if record.owner == d.conn.SessionID() && strings.HasPrefix(name, d.self+".") {
			// Create 的响应也可能丢失，按会话和节点内容收养已有节点，避免重复在线。
			if !ip.IsValid() || record.ip != ip || own {
				err := d.conn.Delete(d.root+"/"+name, -1)
				if err != nil && err != zk.ErrNoNode {
					return nil, nil, false, err
				}
				changed = true
				continue
			}
			own = true
		}
		records[name] = record
	}
	if ip.IsValid() && !own {
		_, err = d.conn.Create(d.root+"/"+d.self+".", []byte(ip.String()), zk.FlagEphemeral|zk.FlagSequence, zk.WorldACL(zk.PermAll))
		if err != nil {
			return nil, nil, false, err
		}
		changed = true
	}
	return watch, records, changed, nil
}
