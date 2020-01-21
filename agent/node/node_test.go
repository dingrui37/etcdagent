package node

import (
    "context"
    "fmt"
    "os"
    "testing"
    "time"

    "github.com/etcd-io/etcd/clientv3"
)

const ETCDADDR = "172.100.1.239:2379"

func TestNodeOnline(t *testing.T) {
    var client *clientv3.Client
    conf := clientv3.Config{
        Endpoints:   []string{ETCDADDR},
        DialTimeout: 5 * time.Second,
    }

    var err error
    if client, err = clientv3.New(conf); err != nil {
        fmt.Println("New client failed")
        os.Exit(1)
    }

    node := NewNode(client)
    node.NodeSetTTL(2)
    data := []struct {
        nodeId      uint32
        serviceAddr string
        expected    string
    }{
        {1, "192.168.0.1:50051", "192.168.0.1:50051"},
        {2, "192.168.0.2:50052", "192.168.0.2:50052"},
        {3, "192.168.0.3:50053", "192.168.0.3:50053"},
        {4, "192.168.0.4:50054", "192.168.0.4:50054"},
    }

    for _, info := range data {
        if err := node.NodeOnline(info.nodeId, info.serviceAddr); err != nil {
            t.Errorf("Node online error, nodeId: %v, serviceAddr: %v, reason: %v",
                info.nodeId, info.serviceAddr, err.Error())
        }

        if acctually, err := node.GetNodeServiceAddr(info.nodeId); err != nil {
            t.Errorf("Get node error, nodeId: %v, reason: %v", info.nodeId, err.Error())
        } else {
            if acctually != info.expected {
                t.Errorf("Test node online failed, expected = %v, acctually = %v, ", info.expected, acctually)
            }
        }
    }
    <-time.After(2 * time.Second)
    client.Close()
}

func TestNodeKeepalive(t *testing.T) {
    var client *clientv3.Client
    conf := clientv3.Config{
        Endpoints:   []string{ETCDADDR},
        DialTimeout: 5 * time.Second,
    }

    var err error
    if client, err = clientv3.New(conf); err != nil {
        fmt.Println("New client failed")
        os.Exit(1)
    }

    ctx, cancel := context.WithCancel(context.TODO())
    node := NewNode(client)
    data := []struct {
        nodeId      uint32
        serviceAddr string
        expected    string
    }{
        {1, "192.168.0.1:50051", "192.168.0.1:50051"},
        {2, "192.168.0.2:50052", "192.168.0.2:50052"},
        {3, "192.168.0.3:50053", "192.168.0.3:50053"},
        {4, "192.168.0.4:50054", "192.168.0.4:50054"},
    }

    for _, info := range data {
        if err := node.NodeOnline(info.nodeId, info.serviceAddr); err != nil {
            t.Errorf("Node online error, reason: %v", err.Error())
        }

        go func(ctx context.Context, node Node, nodeId uint32) {
            for {
                select {
                case <-ctx.Done():
                    return
                case <-time.After(500 * time.Millisecond):
                    if err := node.NodeKeepalive(nodeId); err != nil {
                        t.Errorf("Node keepalive error, nodeId: %v, reason：%v",
                            nodeId, err.Error())
                    }
                }
            }
        }(ctx, node, info.nodeId)
    }

    //超时后测试保活效果
    <-time.After(5 * time.Second)

    for _, info := range data {
        //测试获取单个node服务
        if acctually, err := node.GetNodeServiceAddr(info.nodeId); err != nil {
            t.Errorf("Get node error, reason:%v", err.Error())
        } else {
            if acctually != info.expected {
                t.Errorf("Test node keepalive failed, expected = %v, acctually = %v",
                    info.expected, acctually)
            }
        }
    }

    //测试获取所有nodes
    expected := []uint32{1, 2, 3, 4}
    if acctually, err := node.GetAllNodes(); err != nil {
        t.Errorf("Get all nodes error, reason: %v", err.Error())
    } else {
        if len(expected) != len(acctually) {
            t.Errorf("Test node keepalive failed, expected = %v, acctually = %v",
                expected, acctually)
        }

        contains := func(s []uint32, v uint32) bool {
            result := false
            for _, m := range s {
                if m == v {
                    result = true
                    break
                }
            }
            return result
        }

        for _, n := range expected {
            if !contains(acctually, n) {
                t.Errorf("Test node keepalive failed, expected = %v, acctually = %v",
                    expected, acctually)
            }
        }
    }

    cancel()
    client.Close()
    <-time.After(5 * time.Second)
}

func TestNodeOffline(t *testing.T) {
    var client *clientv3.Client
    conf := clientv3.Config{
        Endpoints:   []string{ETCDADDR},
        DialTimeout: 5 * time.Second,
    }

    var err error
    if client, err = clientv3.New(conf); err != nil {
        fmt.Println("New client failed")
        os.Exit(1)
    }

    data := []struct {
        nodeId      uint32
        serviceAddr string
        expected    string
    }{
        {1, "192.168.0.1:50051", "192.168.0.1:50051"},
        {2, "192.168.0.2:50052", "192.168.0.2:50052"},
        {3, "192.168.0.3:50053", "192.168.0.3:50053"},
        {4, "192.168.0.4:50054", "192.168.0.4:50054"},
    }

    node := NewNode(client)
    for _, info := range data {
        node.NodeSetTTL(10)
        if err := node.NodeOnline(info.nodeId, info.serviceAddr); err != nil {
            t.Errorf("Node online error, nodeId: %v, serviceAddr: %v, reason: %v",
                info.nodeId, info.serviceAddr, err.Error())
        }
    }

    time.After(5 * time.Second)
    for _, info := range data {
        if err := node.NodeOffline(info.nodeId); err != nil {
            t.Errorf("Node offline error, nodeId: %v, reason: %v", info.nodeId, err.Error())
        }
    }

    //测试获取所有nodes
    if acctually, err := node.GetAllNodes(); err != nil {
        t.Errorf("Get all nodes error, reason:%v", err.Error())
    } else {
        if len(acctually) != 0 {
            t.Errorf("Node offline error, get all node expected = [], acctually = %v", acctually)
        }
    }

    client.Close()
}
