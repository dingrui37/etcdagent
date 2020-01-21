package event

import (
    "context"
    "fmt"
    "os"
    "sync"
    "testing"
    "time"

    mvccpb "github.com/coreos/etcd/mvcc/mvccpb"
    "github.com/etcd-io/etcd/clientv3"
)

const ETCDADDR = "172.100.1.239:2379"

func TestWatch(t *testing.T) {
    var client *clientv3.Client
    conf := clientv3.Config{
        Endpoints:   []string{ETCDADDR},
        DialTimeout: 5 * time.Second,
    }

    var err error
    if client, err = clientv3.New(conf); err != nil {
        t.Errorf("New client failed, reason:%v", err.Error())
        os.Exit(1)
    }

    evt := NewEvent(client)
    evtCh := make(chan *clientv3.Event, 100)

    ctx, cancel := context.WithCancel(context.Background())
    go evt.Watch(ctx, evtCh)

    var wg sync.WaitGroup
    data := [...]struct {
        key   string
        value string
    }{
        {key: "/CoreNet/Node/1", value: "192.168.0.1:50051"},
        {key: "/CoreNet/Node/2", value: "192.168.0.2:50052"},
        {key: "/CoreNet/Node/3", value: "192.168.0.3:50053"},
        {key: "/CoreNet/Node/4", value: "192.168.0.4:50054"},
    }

    //准备测试夹具
    //校验结果

    wg.Add(1)
    go func(wg *sync.WaitGroup, evtCh <-chan *clientv3.Event) {
        var putCnt uint32
        var delCnt uint32
        for {
            select {
            case <-time.After(10 * time.Second):
                if putCnt != uint32(len(data)) || delCnt != uint32(len(data)) {
                    t.Errorf("Test Watch failed, expected = 4, acctully put = %v, delete = %v", putCnt, delCnt)
                }
                wg.Done()
                return

            case ev := <-evtCh:
                fmt.Println(ev)
                switch ev.Type {
                case mvccpb.PUT:
                    for _, v := range data {
                        if string(ev.Kv.Key) == v.key && string(ev.Kv.Value) == v.value {
                            putCnt += 1
                        }
                    }

                case mvccpb.DELETE:
                    for _, v := range data {
                        if string(ev.Kv.Key) == v.key {
                            delCnt += 1
                        }
                    }
                }
            }
        }
    }(&wg, evtCh)

    //模拟输入
    wg.Add(1)
    go func(wg *sync.WaitGroup, client *clientv3.Client) {
        //Put
        for _, v := range data {
            if _, err := client.Put(ctx, v.key, v.value); err != nil {
                t.Errorf("Put data to etcd error, key: %v, value: %v, reason: %v", v.key, v.value, err.Error())
            }

            time.After(time.Second)
        }

        //Delete
        for _, v := range data {
            if _, err := client.Delete(ctx, v.key); err != nil {
                t.Errorf("Delete data from etcd error, key: %v, reason: %v", v.key, err.Error())
            }
        }

        wg.Done()
    }(&wg, client)

    wg.Wait()

    cancel()
    client.Close()
}
