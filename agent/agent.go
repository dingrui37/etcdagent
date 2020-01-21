package agent

import (
    "context"
    "etcdagent/agent/event"
    "etcdagent/agent/log"
    "etcdagent/agent/ms"
    "etcdagent/agent/node"
    "os"
    "strconv"
    "strings"
    "time"

    mvccpb "github.com/coreos/etcd/mvcc/mvccpb"
    "github.com/etcd-io/etcd/clientv3"
)

const (
    ETCD_DEFAULT_TIMEOUT = 5
    ETCD_DEFAULT_ADDR    = "127.0.0.1:2379"
)

type Agent struct {
    node.Node
    ms.MS
    event.Event
    client *clientv3.Client
}

func NewAgent(addrs []string, timeout time.Duration) (*Agent, error) {
    conf := clientv3.Config{
        Endpoints:   addrs,
        DialTimeout: timeout,
    }

    var client *clientv3.Client
    var err error

    if client, err = clientv3.New(conf); err != nil {
        log.Warn("New v3 client error, reason: %v\n", err.Error())
        return nil, err
    }

    return &Agent{
        Node:   node.NewNode(client),
        MS:     ms.NewMS(client),
        Event:  event.NewEvent(client),
        client: client,
    }, nil
}

func NewDefaultAgent() (*Agent, error) {
    addrs := strings.Split(os.Getenv("ETCD_NODES"), ",")
    if len(addrs) == 0 {
        addrs = []string{ETCD_DEFAULT_ADDR}
    }

    timeout, err := strconv.Atoi(os.Getenv("ETCD_TIMEOUT"))
    if timeout == 0 || err != nil {
        timeout = ETCD_DEFAULT_TIMEOUT
    }

    return NewAgent(addrs, time.Duration(timeout)*time.Second)
}

func (a *Agent) Run() {
    log.Info("Start etcdagent")
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    evtChan := make(chan *clientv3.Event, 1024)
    go a.Event.Watch(ctx, evtChan)

    //如果Watch到的事件与node或者ms相关，则修改本地状态
    for e := range evtChan {
        log.Info("Event = %v", e)
        if e.Type == mvccpb.DELETE {
            key := string(e.Kv.Key)
            tmp := strings.Split(key, "/")
            inodeId, _ := strconv.Atoi(tmp[len(tmp)-1])
            nodeId := uint32(inodeId)
            if strings.HasPrefix(key, node.NODE_PREFIX) {
                a.NodeOffline(nodeId)
            }

            if strings.HasPrefix(key, ms.MS_PREFIX) {
                a.MSGiveUp(nodeId)
            }
        }
    }
}
