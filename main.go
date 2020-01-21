package main

import "C"
import (
    "etcdagent/agent"
    "etcdagent/agent/log"
    "etcdagent/agent/ms"
	"os"
	"os/exec"
    "os/signal"
    "strings"
    "sync"
    "syscall"
    "time"
    "unsafe"
)

var once sync.Once
var etcd *agent.Agent

const (
    ETCD_SUCCESS = 0
    ETCD_ERROR   = 1
)

func main() {
    exit := make(chan os.Signal, 10)
    signal.Notify(exit, syscall.SIGINT, syscall.SIGTERM)

    var a *agent.Agent
    var err error

    if a, err = agent.NewAgent([]string{"172.100.1.226:2379"}, 5*time.Second); err != nil {
        log.Warn("New agent error, reason: %v", err.Error())
        os.Exit(1)
    }

    go a.Run()

    sig := <-exit
    log.Warn("Receive signal = %v, etcdagent will stop", sig)
}

//export EtcdAgentInit
func EtcdAgentInit(etdcdservers string) {
    once.Do(func() {
        err := exec.Command("bash", "-c", "echo 1024 > /proc/sys/fs/mqueue/msg_max").Run()
        if err != nil {
            log.Warn("Set mqueue msg_max error: %v", err.Error())
            os.Exit(1)
        }
        
        addrs := strings.Split(etdcdservers, ";")
        log.Warn("ETCD_ADDRS:%v\n", addrs)
        if a, err := agent.NewAgent(addrs, 5*time.Second); err != nil {
            log.Warn("New etcd agent error, addrs = %v, reason: %v.\n ", addrs, err.Error())
            os.Exit(1)
        } else {
            etcd = a
        }

        go etcd.Run()
    })
}

//export EtcdNodeOnline
func EtcdNodeOnline(nodeId uint32, serviceAddr string) int {
    if err := etcd.NodeOnline(nodeId, serviceAddr); err != nil {
        return ETCD_ERROR
    }
    return ETCD_SUCCESS
}

//export EtcdNodeKeepalive
func EtcdNodeKeepalive(nodeId uint32) int {
    if err := etcd.NodeKeepalive(nodeId); err != nil {
        return ETCD_ERROR
    }
    return ETCD_SUCCESS
}

//export EtcdNodeOffline
func EtcdNodeOffline(nodeId uint32) int {
    if err := etcd.NodeOffline(nodeId); err != nil {
        return ETCD_ERROR
    }
    return ETCD_SUCCESS
}

//export EtcdGetAllNodes
func EtcdGetAllNodes() *C.struct_Nodes {
    p, _ := etcd.CGetAllNodes()
    return (*C.struct_Nodes)(unsafe.Pointer(p))
}

//export EtcdGetNodeServiceAddr
func EtcdGetNodeServiceAddr(nodeId uint32) *C.struct_ServiceAddr {
    p, _ := etcd.CGetNodeServiceAddr(nodeId)
    return (*C.struct_ServiceAddr)(unsafe.Pointer(p))
}

func EtcdNodeSetTTL(ttl uint32) {
    etcd.NodeSetTTL(int64(ttl))
}

//export EtcdMSCompete
func EtcdMSCompete(nodeId uint32) int {
    if err := etcd.MSCompete(nodeId); err != nil {
        return ETCD_ERROR
    }
    return ETCD_SUCCESS
}

//export EtcdMSGiveUp
func EtcdMSGiveUp(nodeId uint32) int {
    if err := etcd.MSGiveUp(nodeId); err != nil {
        return ETCD_ERROR
    }
    return ETCD_SUCCESS
}

//export EtcdMSKeepalive
func EtcdMSKeepalive(nodeId uint32) int {
    if err := etcd.MSKeepalive(nodeId); err != nil {
        return ETCD_ERROR
    }
    return ETCD_SUCCESS
}

//export EtcdIsMaster
func EtcdIsMaster(nodeId uint32) bool {
    return etcd.IsMaster(nodeId)
}

//export EtcdGetMaster
func EtcdGetMaster() uint32 {
    var err error
    var master uint32
    if master, err = etcd.GetMaster(); err != nil {
        return ms.INVALID_NODE
    }
    return master
}
