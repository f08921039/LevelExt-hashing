#ifndef __DHT_H
#define __DHT_H

#include "compiler.h"

//#define DHT_INTEGER 1

#ifdef DHT_INTEGER
    #define MAX_KEY_LEN 32
    #define MAX_VAL_LEN 32
#endif

#ifdef  __cplusplus
extern  "C" {
#endif

#define failed_dht_kv_context(context) (((void *)(context)) == MAP_FAILED)
#define empty_dht_kv_context(context) ((context) == NULL)

struct dht_kv_context {
#if (BYTEORDER_ENDIAN == BYTEORDER_LITTLE_ENDIAN)
    s16 diff_len;
    u8 padding[4];
    u8 key_len;
    u8 val_len;
#else
    u8 val_len;
    u8 key_len;
    u8 padding[4];
    s16 diff_len;
#endif
#ifdef DHT_INTEGER 
    u64 key;
    u64 value;
#else
    u8 buffer[0];
#endif
};

//struct kv;

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
    int dht_kv_put(u64 key, u64 value);

    int dht_kv_delete_context(struct dht_kv_context *del_con);

    struct dht_kv_context *dht_kv_delete(u64 key);

    int dht_kv_get_context(struct dht_kv_context *get_con);

    struct dht_kv_context *dht_kv_get(u64 key);
#else
    int dht_kv_put(void *key, void *value, 
                int key_len, int val_len);

    int dht_kv_delete_context(struct dht_kv_context *del_con);

    struct dht_kv_context *dht_kv_delete(void *key, int key_len);

    int dht_kv_get_context(struct dht_kv_context *get_con);

    struct dht_kv_context *dht_kv_get(void *key, int key_len, 
                                            int max_val_len);
#endif


#ifdef  __cplusplus
}
#endif  //__cplusplus

#endif  //__DHT_H
