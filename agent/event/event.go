package event

/*
#cgo LDFLAGS: -lrt
#include "mq.h"
*/
import "C"
import (
    "context"
    "etcdagent/agent/log"
    "unsafe"
    "strings"
    "github.com/coreos/etcd/mvcc/mvccpb"
    "github.com/etcd-io/etcd/clientv3"
    //mvccpb "github.com/coreos/etcd/mvcc/mvccpb"
)

type Event interface {
    Watch(ctx context.Context, eventChan chan<- *clientv3.Event)
}

type event struct {
    client *clientv3.Client
}

const (
    EVENT_ROOT_PREFIX = "/CoreNet/"
)

func NewEvent(client *clientv3.Client) Event {
    return &event{
        client: client,
    }
}

func (e *event) Watch(ctx context.Context, eventChan chan<- *clientv3.Event) {
    if _, err := C.MqOpen(); err != nil {
        log.Warn("Open message queue error, reason: %v", err.Error())
        return
    }

    prefix := EVENT_ROOT_PREFIX
    wChan := e.client.Watch(ctx, prefix, clientv3.WithPrefix())
    for {
        select {
        case <-ctx.Done():
            log.Info("Event watch done")
            return
        case wResp := <-wChan:
            l := len(wResp.Events)
            message := C.NewMessage(C.uint32_t(l))
            for _, ev := range wResp.Events {
                evtKey := string(ev.Kv.Key)
                tmp := strings.Split(evtKey, "/")
                nodeId := tmp[len(tmp) - 1]
                kstr := C.CString(nodeId)
                vstr := C.CString(string(ev.Kv.Value))
                var evtType uint8
                if ev.Type == mvccpb.DELETE {
                    evtType = 1
                } else {
                    evtType = 0
                }
                
                C.AddEvent(message, kstr, vstr, C.uint8_t(evtType))
                C.free(unsafe.Pointer(kstr))
                C.free(unsafe.Pointer(vstr))
            }

            C.DumpMessage(message)
            C.MqSend(message, C.GetMessageSize(message))
            C.free(unsafe.Pointer(message))

            for _, ev := range wResp.Events {
                eventChan <- ev
            }
        }
    }
}
