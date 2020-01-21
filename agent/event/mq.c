#include <mqueue.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <pthread.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <stdio.h>
#include <stdint.h>
#include "mq.h"

#define MQ_MAX_MSGSIZE 1024 //系统配置为8192字节

mqd_t etcdmqd;

/*
 * 1）为变长消息申请内存，使用完后需主动释放
 * 2）单个消息大小不能超过MQ的最大值
 */
Message *NewMessage(uint32_t maxEvents)
{
    uint32_t size = sizeof(Message) + maxEvents * sizeof(Event);
    if (size >= MQ_MAX_MSGSIZE)
    {
        errno = EINVAL;
        return NULL;
    }

    Message *ptMessage = (Message *)malloc(size);
    if (ptMessage != NULL)
    {
        memset(ptMessage, 0, size);
        return ptMessage;
    }

    return NULL;
}

/*
 * 根据消息体指针计算消息体总长度   
 */
uint32_t GetMessageSize(Message *ptMessage)
{
    if (ptMessage == NULL)
    {
        errno = EINVAL;
        return 0;
    }

    return sizeof(Message) + *((uint32_t *)ptMessage) * sizeof(Event);
}

/*
 * 构建消息体中的事件
 */
void AddEvent(Message *ptMessage, char *key, char *value, uint8_t type)
{
    strncpy(ptMessage->evts[ptMessage->length].key, key, MAX_KEYLENGTH);
    strncpy(ptMessage->evts[ptMessage->length].value, value, MAX_VALUELENGTH);
    ptMessage->evts[ptMessage->length].type = type;
    ptMessage->length++;
}

void DumpMessage(Message *ptMessage)
{
    if (ptMessage == NULL)
    {
        printf("Message pointer is NULL.\n");
        return;
    }
    else
    {
        int i = 0;
        printf(" -------- Send Message -----------------\n");
        printf("Events: %u\n", ptMessage->length);
        for (; i < ptMessage->length; i++)
        {
            printf("%u: %s -> %s\n", i, ptMessage->evts[i].key, ptMessage->evts[i].value);
        }
    }
}

/*
 * 创建消息队列，mq_maxmsg 最大消息数，mq_msgsize 单个消息最大长度
 * dingrui@dingrui-PC:~/Programes$ grep msg /proc/self/limits 
 * Max msgqueue size         819200               819200               bytes     
 *  
 */
int MqOpen()
{
    int flags;
    mode_t perms;
    mqd_t mqd;
    struct mq_attr attrs;

    attrs.mq_maxmsg = 512;
    attrs.mq_msgsize = 1024;
    flags = O_RDWR | O_CREAT;
    mq_unlink(ETCDMQ);
    etcdmqd = mq_open(ETCDMQ, flags, 0666, &attrs);
    if (etcdmqd == (mqd_t)-1)
    {
        perror("mq_open");
        return 1;
    }

    return 0;
}

/* 
 * 向MQ发送消息
 */
int MqSend(Message *message, uint32_t size)
{
    return mq_send(etcdmqd, (char *)message, size, 0);
}

/* 
 * 关闭MQ描述符
 */
int MqClose()
{
    return close(etcdmqd);
}

/* 
 * 删除MQ队列
 */
int MqUnlink()
{
    return mq_unlink(ETCDMQ);
}