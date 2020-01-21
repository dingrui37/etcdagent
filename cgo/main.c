#include <mqueue.h>
#include <sys/stat.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdint.h>
#include <pthread.h>
#include <errno.h>

#include "libetcd.h"
#include "node.h"
#include "mq.h"

struct TEST_NODE
{
    char *addr;
    GoUint32 nodeId;
};

void *node_thread(void *s)
{
    struct TEST_NODE data[8] =
        {
            {.addr = "1.1.1.1:10011", .nodeId = 1},
            {.addr = "2.2.2.2:20022", .nodeId = 2},
            {.addr = "3.3.3.3:30033", .nodeId = 3},
            {.addr = "4.4.4.4:40044", .nodeId = 4},
            {.addr = "5.5.5.5:50055", .nodeId = 5},
            {.addr = "6.6.6.6:60066", .nodeId = 6},
            {.addr = "7.7.7.7:70077", .nodeId = 7},
            {.addr = "8.8.8.8:80088", .nodeId = 8},
        };

    int i = 0;
    int len = sizeof(data) / sizeof(struct TEST_NODE);

    for (; i < len; i++)
    {
        int length = strlen(data[i].addr);
        GoString value = {
            .p = data[i].addr,
            .n = length,
        };

        //测试node上线
        GoUint32 nodeId = data[i].nodeId;
        GoInt ret = EtcdNodeOnline(nodeId, value);
        if (ret != 0)
        {
            printf("Node online error, nodeId = %u\n", nodeId);
        }
    }

    //测试node 保活
    int deadline = 100;
    int j = 0;
    for (; j < deadline; j++)
    {
        for (i = 0; i < len - 1; i++)
        {
            GoUint32 nodeId = data[i].nodeId;
            GoInt ret = EtcdNodeKeepalive(nodeId);
            if (ret != 0)
            {
                printf("Node keepalive error, nodeId = %u\n", nodeId);
            }
        }
        //保活过后测试get信息
        if (j == deadline - 1)
        {
            struct Nodes *pNodes = EtcdGetAllNodes();
            printf("Get all nodes:");
            uint32_t i = 0;
            for (; i < pNodes->length; i++)
            {
                GoUint32 nodeId = pNodes->nodes[i];
                struct ServiceAddr *pAddr = EtcdGetNodeServiceAddr(nodeId);
                printf("Get node = %u service addr = %s\n", nodeId, pAddr->addr);
                free(pAddr); //使用者释放内存
            }
            free(pNodes); //使用者释放内存
            printf("\n");
        }
        usleep(500 * 1000);
    }
    return NULL;
}

void *ms_thread(void *s)
{
    GoUint32 data[5] = {1, 2, 3, 4, 5};
    int i = 0;
    int len = sizeof(data) / sizeof(GoInt32);
    for (; i < len; i++)
    {
        int ret = EtcdMSCompete(data[i]);
        if (ret != 0)
        {
            printf("MS compete error, nodeId = %u\n", data[i]);
        }

        ret = EtcdMSKeepalive(data[i]);
        if (ret != 0)
        {
            printf("MS keepalive error, nodeId = %u\n", data[i]);
        }
    }

    GoUint32 master = EtcdGetMaster();
    printf("master = %u\n", master);

    GoUint8 isMaster = EtcdIsMaster(1);
    printf("node = 1 %s master.\n", isMaster == 0 ? "isn't" : "is");
    return NULL;
}

void *read_mq(void *s)
{
    mqd_t mqd;
    struct mq_attr attrs;
    char *msg_ptr;
    ssize_t recvd;

    mqd = mq_open(ETCDMQ, O_RDONLY | O_CREAT, 0666, NULL);
    if (mqd == (mqd_t)-1)
    {
        perror("mq_open");
        return NULL;
    }

    if (mq_getattr(mqd, &attrs) == -1)
    {
        perror("mq_getattr");
        mq_close(mqd);
        return NULL;
    }

    printf("mq_maxmsg = %ld\n", attrs.mq_maxmsg);
    printf("mq_msgsize = %ld\n", attrs.mq_msgsize);
    msg_ptr = malloc(attrs.mq_msgsize);
    if (msg_ptr == NULL)
    {
        perror("malloc");
        mq_close(mqd);
        return NULL;
    }

    while (1)
    {
        recvd = mq_receive(mqd, msg_ptr, attrs.mq_msgsize, NULL);
        if (recvd == -1)
        {
            perror("mq_receive");
            continue;
        }

        printf("----- Receive message, len = %ld -------\n", recvd);

        int i = 0;
        int length = *((uint32_t *)msg_ptr);
        printf("Events = %u\n", length);

        for (; i < length; i++)
        {
            printf("%u: key = %s, value = %s type = %u\n", i,
                   ((Message *)msg_ptr)->evts[i].key,
                   ((Message *)msg_ptr)->evts[i].value,
                   ((Message *)msg_ptr)->evts[i].type);
        }
    }
}

int main(int argc, char *argv[])
{
    EtcdAgentInit();
    sleep(1);
    int err;
    pthread_t node_tid;
    err = pthread_create(&node_tid, NULL, node_thread, NULL);
    if (err != 0)
    {
        perror("node");
        return 1;
    }

    // pthread_t ms_tid;
    // err = pthread_create(&ms_tid, NULL, ms_thread, NULL);
    // if (err != 0)
    // {
    //     perror("ms");
    //     return 1;
    // }

    // pthread_t mq_tid;
    // err = pthread_create(&mq_tid, NULL, read_mq, NULL);
    // if (err != 0)
    // {
    //     perror("mq");
    //     return 1;
    // }

    pthread_join(node_tid, NULL);
    // pthread_join(ms_tid, NULL);
    //pthread_join(mq_tid, NULL);

    return 0;
}