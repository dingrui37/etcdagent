#include <stdint.h>
#include <stdlib.h>

#define MAX_KEYLENGTH 64
#define MAX_VALUELENGTH 64
#define ETCDMQ "/etcdmq"

typedef struct _Event
{
    char key[MAX_KEYLENGTH];
    char value[MAX_VALUELENGTH];
    uint8_t type; /*0：put 1:delete */
} Event;

typedef struct _Message
{
    uint32_t length; //变长数组长度
    Event evts[0];
} Message;

Message *NewMessage(uint32_t maxEvents);
uint32_t GetMessageSize(Message *ptMessage);
void AddEvent(Message *ptMessage, char *key, char *value, uint8_t type);
void DumpMessage(Message *ptMessage);
int MqOpen();
int MqSend(Message *message, uint32_t size);
int MqClose();
int MqUnlink();