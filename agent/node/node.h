#include <stdint.h>
#include <stdlib.h>

struct ServiceAddr
{
    char addr[64];
    uint8_t length;
};

struct Nodes
{
    uint32_t nodes[64];
    uint32_t length;
};

struct ServiceAddr *CServiceAddr(char *addr, uint8_t len);
struct Nodes *CNodes();
void AddNode(struct Nodes* p, uint32_t node);