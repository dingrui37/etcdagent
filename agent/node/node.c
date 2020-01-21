#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include "node.h"

struct ServiceAddr *CServiceAddr(char *addr, uint8_t len)
{
    size_t size = sizeof(struct ServiceAddr);
    struct ServiceAddr *p = malloc(size);
    if (p != NULL)
    {
        memset(p, 0, size);
        strncpy(p->addr, addr, 64);
        p->length = len;
        return p;
    }
    return NULL;
}

struct Nodes *CNodes()
{
    size_t size = sizeof(struct Nodes);
    struct Nodes *p = malloc(size);
    if (p != NULL)
    {
        memset(p, 0, size);
        return p;
    }
    return NULL;
}

void AddNode(struct Nodes *p, uint32_t node)
{
    if (p == NULL)
    {
        errno = EINVAL;
        return;
    }

    p->nodes[p->length] = node;
    p->length++;
}
