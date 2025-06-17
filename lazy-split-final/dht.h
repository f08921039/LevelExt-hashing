#ifndef __DHT_H
#define __DHT_H

#include "compiler.h"

//#define DHT_INTEGER 1

#ifdef  __cplusplus
extern  "C" {
#endif

struct dht_work_function {
    void *(*start_routine)(void *);
    void *arg;
};

struct dht_node_context {
    int nodes;
    int *max_node_thread;
    int *node_thread;
    struct dht_work_function **node_func;
};


int dht_init_structure(struct dht_node_context *node_context);

int dht_create_thread(struct dht_node_context *node_context);

void dht_terminate_thread();

int dht_add_thread(int node_id,
            struct dht_work_function *func);


#ifdef DHT_INTEGER
    int dht_kv_insert(u64 key, u64 value);

    int dht_kv_update(u64 key, u64 value);

    int dht_kv_delete(u64 key);

    int dht_kv_lookup(u64 key, u64 *val_buf);
#else
    int dht_kv_insert(void *key, void *value, 
                int key_len, int val_len);

    int dht_kv_update(void *key, void *value, 
                int key_len, int val_len);

    int dht_kv_delete(void *key, int key_len);

    int dht_kv_lookup(void *key, void *val_buf, int key_len, int buf_len);
#endif


#ifdef  __cplusplus
}
#endif  //__cplusplus

#endif  //__DHT_H
