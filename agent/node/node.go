package node

/*
#include "node.h"
*/
import "C"
import (
    "context"
    "etcdagent/agent/log"
    "fmt"
    "reflect"
    "regexp"
    "strconv"
    "strings"
    "sync"
    "unsafe"
    "time"
    "github.com/etcd-io/etcd/clientv3"
)

const (
    NODE_PREFIX      = "/CoreNet/Node/"
    NODE_DEFAULT_TTL = 1
)

type Node interface {
    NodeOnline(nodeId uint32, serviceAddr string) error
    NodeOffline(nodeId uint32) error
    NodeKeepalive(nodeId uint32) error
    GetAllNodes() ([]uint32, error)
    GetNodeServiceAddr(nodeId uint32) (string, error)
    NodeSetTTL(ttl int64)
    CGetNodeServiceAddr(nodeId uint32) (*C.struct_ServiceAddr, error)
    CGetAllNodes() (*C.struct_Nodes, error)
}

type node struct {
    sync.Mutex
    client *clientv3.Client
    leases map[uint32]clientv3.LeaseID
    ttl    int64
}

func NewNode(client *clientv3.Client) Node {
    return &node{
        client: client,
        leases: make(map[uint32]clientv3.LeaseID),
        ttl:    NODE_DEFAULT_TTL,
    }
}

func (n *node) NodeSetTTL(ttl int64) {
    n.ttl = ttl
    log.Info("Set ttl = %v", ttl)
}

func (n *node) NodeOnline(nodeId uint32, serviceAddr string) error {
    n.Lock()
    defer n.Unlock()
    log.Warn("receive nodeonline request, nodeid=%v, serivce=%v\n", nodeId, serviceAddr)
    var err error
    var resp *clientv3.LeaseGrantResponse
    ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
    defer cancel()
    if resp, err = n.client.Grant(ctx, n.ttl); err != nil {
        log.Warn("Lease grant error, nodeId: %v, reason: %v\n", nodeId, err.Error())
        return err
    }

    lease := resp.ID
    key := fmt.Sprintf("%s%v", NODE_PREFIX, nodeId)
    if _, err = n.client.Put(context.TODO(), key, serviceAddr, clientv3.WithLease(lease)); err != nil {
        log.Warn("Put %v with lease: %v error, nodeId: %v, reason: %v\n", key, lease, nodeId, err.Error())
        return err
    }

    n.leases[nodeId] = lease
    return nil
}

func (n *node) NodeKeepalive(nodeId uint32) error {
    n.Lock()
    defer n.Unlock()

    ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
    defer cancel()
    if lease, ok := n.leases[nodeId]; ok {
        if _, err := n.client.KeepAliveOnce(ctx, lease); err != nil {
            log.Warn("Node keepalive error, lease: %v, nodeId: %v, reason: %v\n", lease, nodeId, err.Error())
            return err
        }
        return nil
    }

    return fmt.Errorf("Node keepalive error, cannot find lease for the node: %v", nodeId)
}

func (n *node) NodeOffline(nodeId uint32) error {
    n.Lock()
    defer n.Unlock()

    ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
    defer cancel()
    if _, ok := n.leases[nodeId]; ok {
        key := fmt.Sprintf("%s%v", NODE_PREFIX, nodeId)
        if _, err := n.client.Delete(ctx, key); err != nil {
            log.Warn("Node offline error, nodeId: %v, reason: %v\n", nodeId, err.Error())
            return err
        }

        delete(n.leases, nodeId)
        log.Warn("Node offline, nodeId = %v", nodeId)
        return nil
    }

    return fmt.Errorf("Node offline error, cannot find lease for the node: %v", nodeId)
}

func (n *node) GetAllNodes() ([]uint32, error) {
    var err error
    var resp *clientv3.GetResponse

    ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
    defer cancel()
    if resp, err = n.client.Get(ctx, NODE_PREFIX, clientv3.WithPrefix()); err != nil {
        log.Warn("Get %v with prefix error, reason: %v", NODE_PREFIX, err.Error())
        return nil, err
    }

    remote := make([]uint32, 0)
    for _, v := range resp.Kvs {
        var nodeId uint32
        pattern := fmt.Sprintf("%s%s", NODE_PREFIX, "[0-9]+") //至少一位数字的nodeId
        re := regexp.MustCompile(pattern)
        key := string(v.Key)
        if re.MatchString(key) {
            tmp := strings.Split(key, "/")
            inodeId, _ := strconv.Atoi(tmp[len(tmp)-1])
            nodeId = uint32(inodeId)
        }

        remote = append(remote, nodeId)
    }

    local := make([]uint32, 0)
    for node := range n.leases {
        local = append(local, node)
    }

    if !reflect.DeepEqual(remote, local) {
        log.Warn("Data inconsistent, local nodes:%v, remote nodes:%v", local, remote)
    }

    return remote, nil
}

func (n *node) GetNodeServiceAddr(nodeId uint32) (string, error) {
    // n.Lock()
    // defer n.Unlock()

    var err error
    var resp *clientv3.GetResponse

    // if _, ok := n.leases[nodeId]; ok {
    // }
    ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
    defer cancel()
    key := fmt.Sprintf("%s%v", NODE_PREFIX, nodeId)
    if resp, err = n.client.Get(ctx, key); err != nil {
        log.Warn("Get %v with prefix error, reason: %v", NODE_PREFIX, err.Error())
        return "", err
	}
	
    var addr string
    if len(resp.Kvs) != 0 {
    addr = string(resp.Kvs[0].Value)
        return addr, nil
    }

    return "", fmt.Errorf("Get node service addr response is empty, nodeId = %v", nodeId)

    //return "", fmt.Errorf("Get node service error, node doesn't exist in local: %v", nodeId)
}

func (n *node) CGetAllNodes() (*C.struct_Nodes, error) {
    var err error
    var nodes []uint32
    if nodes, err = n.GetAllNodes(); err != nil {
        return nil, err
    }

    var p *C.struct_Nodes
    if p, err = C.CNodes(); err != nil {
        return nil, err
    }

    for _, n := range nodes {
        C.AddNode(p, C.uint32_t(n))
    }
    log.Info("Get all nodes = %v", nodes)
    return p, nil
}

func (n *node) CGetNodeServiceAddr(nodeId uint32) (*C.struct_ServiceAddr, error) {
    var err error
    var addr string
    if addr, err = n.GetNodeServiceAddr(nodeId); err != nil {
        return nil, err
    }

    cstr := C.CString(addr)
    defer C.free(unsafe.Pointer(cstr))

    var p *C.struct_ServiceAddr
    if p, err = C.CServiceAddr(cstr, C.uint8_t(len(addr))); err != nil {
        return nil, err
    }
    log.Info("Get service addr: nodeId = %v, addr = %v", nodeId, addr)
    return p, nil
}
