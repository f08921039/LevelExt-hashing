#ifndef __DHT_H
#define __DHT_H

//#define DHT_INTEGER 1

#include "dht_init.h"


#ifdef DHT_INTEGER
    #define MAX_KEY_LEN 32
    #define MAX_VAL_LEN 32
#endif

#ifdef  __cplusplus
extern  "C" {
#endif

#define failed_kv_ret_context(context) (((void *)(context)) == MAP_FAILED)
#define empty_kv_ret_context(context) ((context) == NULL)

struct kv_ret_context {
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


extern int dht_init_structure();
extern int dht_create_thread(void *(**start_routine)(void *),
                                        void **restrict arg, 
                                        int threads, int cpus);
extern int dht_add_thread(void *(**start_routine)(void *),
                        void **restrict arg, int threads);

//assert tid
static inline pthread_t dht_pthread_id(int tid) {
    return get_pthread_id(tid);
}

//assert tid
static inline int dht_cpuid(int tid) {
    return get_cpuid(tid);
}

static inline int dht_running_thread() {
    return get_active_thread_num();
}

static inline int dht_cpu_num() {
    return get_cpu_num();
}

#ifdef DHT_INTEGER
    int dht_kv_put(u64 key, u64 value);

    int dht_kv_delete_context(struct kv_ret_context *del_con);

    struct kv_ret_context *dht_kv_delete(u64 key);

    int dht_kv_get_context(struct kv_ret_context *get_con);

    struct kv_ret_context *dht_kv_get(u64 key);
#else
    int dht_kv_put(void *key, void *value, 
                int key_len, int val_len);

    int dht_kv_delete_context(struct kv_ret_context *del_con);

    struct kv_ret_context *dht_kv_delete(void *key, int key_len);

    int dht_kv_get_context(struct kv_ret_context *get_con);

    struct kv_ret_context *dht_kv_get(void *key, int key_len, 
                                            int max_val_len);
#endif


#ifdef  __cplusplus
}
#endif  //__cplusplus

#endif  //__DHT_H
