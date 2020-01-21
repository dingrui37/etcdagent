package ms

import (
    "context"
    "fmt"
    "os"
    "sync"
    "testing"
    "time"

    "github.com/etcd-io/etcd/clientv3"
)

const ETCDADDR = "172.100.1.239:2379"

func TestMSCompete(t *testing.T) {
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

    ms := NewMS(client)
    ms.MSSetTTL(10)
    for _, info := range []struct {
        nodeId   uint32
        expected uint32
    }{
        {1, 1},
        {2, 1},
        {3, 1},
        {4, 1},
    } {
        if err := ms.MSCompete(info.nodeId); err != nil {
            t.Errorf("MS compete error, node: %v, reason: %v", info.nodeId, err.Error())
        }

        if acctually, err := ms.GetMaster(); err != nil {
            t.Errorf("Get master error, reason: %v", err.Error())
        } else {
            if acctually != info.expected {
                t.Errorf("Test MS compete failed, expected = %v, acctually = %v", info.expected, acctually)
            }
        }
    }

    for _, info := range []struct {
        nodeId   uint32
        expected uint32
    }{
        {1, 2},
        {2, 3},
        {3, 4},
        {4, 0xff},
    } {
        if err := ms.MSGiveUp(info.nodeId); err != nil {
            t.Errorf("MS give up error, node: %v, reason:%v", info.nodeId, err.Error())
        }

        if acctually, err := ms.GetMaster(); err != nil {
            t.Errorf("Get master error, reason: %v", err.Error())
        } else {
            if acctually != info.expected {
                t.Errorf("Test MS compete failed, expected = %v, acctually = %v", info.expected, acctually)
            }
        }
    }

    <-time.After(10 * time.Second) //不影响下一个测试用例
    client.Close()
}

func TestMSKeepalive(t *testing.T) {
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

    ms := NewMS(client)
    var wg sync.WaitGroup
    for _, info := range []struct {
        nodeId   uint32
        expected uint32
    }{
        {1, 1},
        {2, 1},
        {3, 1},
        {4, 1},
    } {
        wg.Add(1)
        go func(nodeId uint32, expected uint32) {
            <-time.After(time.Duration(nodeId) * time.Millisecond * 100) //随机延时，确保nodeId=1为master
            if err := ms.MSCompete(nodeId); err != nil {
                t.Errorf("MS compete error, reason: %v", err.Error())
            }

            for i := 0; i < 100; i++ {
                if err := ms.MSKeepalive(nodeId); err != nil {
                    t.Errorf("MS keepalive error, reason: %v", err.Error())
                }

                if acctually, err := ms.GetMaster(); err != nil {
                    t.Errorf("Get master error, reason: %v", err.Error())
                } else {
                    if acctually != expected {
                        t.Errorf("Test MS keepalive failed, expected = %v, acctually = %v", expected, acctually)
                    }
                }
                <-time.After(100 * time.Millisecond)
            }

            //3s后GetMaster获取到无效值
            <-time.After(3 * time.Second)
            if acctually, err := ms.GetMaster(); err != nil {
                t.Errorf("Get master error, reason: %v", err.Error())
            } else {
                if acctually != 0xff {
                    t.Errorf("Get master should give an invalid node, expected: 0xff, acctually = %v", acctually)
                }
            }

            wg.Done()
        }(info.nodeId, info.expected)
    }
    wg.Wait()
    client.Close()
}

func TestIsMaster(t *testing.T) {
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

    ms := NewMS(client)
    var wg sync.WaitGroup
    for _, info := range []struct {
        nodeId   uint32
        expected bool
    }{
        {1, true},
        {2, false},
        {3, false},
        {4, false},
    } {
        wg.Add(1)
        go func(nodeId uint32, expected bool) {
            <-time.After(time.Duration(nodeId) * time.Millisecond * 100) //随机延时，确保nodeId=1为master
            if err := ms.MSCompete(nodeId); err != nil {
                t.Errorf("MS compete error, node: %v, reason: %v", nodeId, err.Error())
            }

            for i := 0; i < 100; i++ {
                if err := ms.MSKeepalive(nodeId); err != nil {
                    t.Errorf("MS keepalive error, node: %v, reason: %v", nodeId, err.Error())
                }

                if acctually := ms.IsMaster(nodeId); acctually != expected {
                    t.Errorf("Test isMaster failed, nodeId = %v, expected = %v, acctually = %v", nodeId, expected, acctually)
                }
                <-time.After(100 * time.Millisecond)
            }
            wg.Done()
        }(info.nodeId, info.expected)
    }

    wg.Wait()
    client.Close()
}

func TestMSSetTTL(t *testing.T) {
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

    ms := NewMS(client)
    ms.MSSetTTL(1)

    go func() {
        fmt.Printf("Start watch, time = %v\n", time.Now())
        ctx, cancel := context.WithTimeout(context.TODO(), 5*time.Second)
        defer cancel()
        wChan := client.Watch(ctx, "/CoreNet/MS/", clientv3.WithPrefix())

        for wResp := range wChan {
            fmt.Printf("Receive event, time = %v\n", time.Now())
            for _, ev := range wResp.Events {
                fmt.Printf("evt = %v\n", ev)
            }
        }
    }()

    var nodeId uint32 = 100
    fmt.Printf("Start MS compete, time = %v\n", time.Now())
    if err := ms.MSCompete(nodeId); err != nil {
        t.Errorf("Test MS Set TTL error, reason: %v\n", err.Error())
    }
    <-time.After(5 * time.Second)
}

// 实测：1s的超时，客户端需要约2s后才能感知到, 无论是Get还是Watch ！！！！
// 当超时时间n 》1s时候，
// dingrui@dingrui:~/go/src/etcdagent/agent/ms$ go test -v
// === RUN   TestTTL
// start time = 2019-09-23 17:20:07.676003337 +0800 CST m=+0.016133154
// end time = 2019-09-23 17:20:09.721022874 +0800 CST m=+2.061152752 error = Get /CoreNet/MS/ response kvs is empty
// --- PASS: TestTTL (2.06s)
// PASS
// ok      etcdagent/agent/ms      2.066s
