package ms

import (
    "context"
    "etcdagent/agent/log"
    "fmt"
    "regexp"
    "strconv"
    "strings"
    "sync"

    "github.com/etcd-io/etcd/clientv3"
)

const (
    MS_PREFIX      = "/CoreNet/MS/"
    MS_DEFAULT_TTL = 1
    INVALID_NODE   = 0xffffffff
)

type MS interface {
    MSCompete(nodeId uint32) error
    MSGiveUp(nodeId uint32) error
    MSKeepalive(nodeId uint32) error
    IsMaster(nodeId uint32) bool
    GetMaster() (uint32, error)
    MSSetTTL(int64)
}

type ms struct {
    sync.Mutex
    client *clientv3.Client
    leases map[uint32]clientv3.LeaseID
    ttl    int64
}

func NewMS(client *clientv3.Client) MS {
    return &ms{
        client: client,
        leases: make(map[uint32]clientv3.LeaseID),
        ttl:    MS_DEFAULT_TTL,
    }
}

func (m *ms) MSCompete(nodeId uint32) error {
    m.Lock()
    defer m.Unlock()

    var err error
    var grantResp *clientv3.LeaseGrantResponse
    if grantResp, err = m.client.Grant(context.TODO(), m.ttl); err != nil {
        log.Warn("Lease grant error, reason: %v\n", err.Error())
        return err
    }

    key := fmt.Sprintf("%s%v", MS_PREFIX, nodeId)
    if _, err = m.client.Put(context.TODO(), key, "", clientv3.WithLease(grantResp.ID)); err != nil {
        log.Warn("Put %v with lease %v error, reason: %v", key, grantResp.ID, err.Error())
        return err
    }

    m.leases[nodeId] = grantResp.ID
    log.Info("MS compete, node: %v", nodeId)
    return nil
}

func (m *ms) MSGiveUp(nodeId uint32) error {
    m.Lock()
    defer m.Unlock()

    var err error
    var resp *clientv3.DeleteResponse
    key := fmt.Sprintf("%s%v", MS_PREFIX, nodeId)

    //如果key不存在，Delete也不会返回错误，resp中 Deleted = 0
    if resp, err = m.client.Delete(context.TODO(), key); err != nil {
        log.Warn("Put %v error, reason: %v", key, err.Error())
        return err
    }

    delete(m.leases, nodeId)
    log.Info("MS give up, node: %v, deleted count: %v", nodeId, resp.Deleted)
    return nil
}

func (m *ms) MSKeepalive(nodeId uint32) error {
    m.Lock()
    defer m.Unlock()

    if lease, ok := m.leases[nodeId]; ok {
        if _, err := m.client.KeepAliveOnce(context.TODO(), lease); err != nil {
            log.Warn("MS keepalive error, nodeId: %v, reason: %v\n", nodeId, err.Error())
            return err
        }
        return nil
    }

    return fmt.Errorf("Not ms node, nodeId:%v", nodeId)
}

func (m *ms) IsMaster(nodeId uint32) bool {
    var err error
    var resp *clientv3.GetResponse

    //查找第一个创建的key， etcd中保存有全局的revision
    if resp, err = m.client.Get(context.TODO(), MS_PREFIX, clientv3.WithFirstCreate()...); err != nil {
        log.Warn("Get prefix: %v with first create error, reason: %v\n", MS_PREFIX, err.Error())
        return false
    }

    pattern := fmt.Sprintf("%s%s", MS_PREFIX, "[0-9]+") //至少一位数字的nodeId
    re := regexp.MustCompile(pattern)

    if len(resp.Kvs) != 0 {
        var rnodeId uint32
        key := string(resp.Kvs[0].Key)
        if re.MatchString(key) {
            tmp := strings.Split(key, "/")
            inodeId, _ := strconv.Atoi(tmp[len(tmp)-1])
            rnodeId = uint32(inodeId)
        }
        return rnodeId == nodeId
    }

    return false
}

func (m *ms) GetMaster() (uint32, error) {
    var err error
    var resp *clientv3.GetResponse

    //查找第一个创建的key， etcd中保存有全局的revision
    if resp, err = m.client.Get(context.TODO(), MS_PREFIX, clientv3.WithFirstCreate()...); err != nil {
        return INVALID_NODE, err
    }

    var nodeId uint32
    pattern := fmt.Sprintf("%s%s", MS_PREFIX, "[0-9]+") //至少一位数字的nodeId
    re := regexp.MustCompile(pattern)

    if len(resp.Kvs) != 0 {
        key := string(resp.Kvs[0].Key)
        if re.MatchString(key) {
            tmp := strings.Split(key, "/")
            inodeId, _ := strconv.Atoi(tmp[len(tmp)-1])
            nodeId = uint32(inodeId)
        }
        return nodeId, nil
    }

    log.Info("Get %s response kvs is empty", MS_PREFIX)
    return INVALID_NODE, nil
}

func (m *ms) MSSetTTL(ttl int64) {
    m.ttl = ttl
}
