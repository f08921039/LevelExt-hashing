#include "dht.h"
#include "kv.h"
#include "per_thread.h"
#include "ext_hash.h"
#include "prehash.h"

#ifdef DHT_INTEGER
int dht_kv_put(u64 key, u64 value) {
    struct kv *kv;
    u64 prehash;
    int ret;

    kv = (struct kv *)alloc_kv();

    if (unlikely(!kv))
        return -1;

    prehash = prehash64(&key, sizeof(u64), 0);

    init_kv(kv, key, value, prehash);

    ret = put_kv(kv, prehash);

    inc_epoch_per_thread();

    if (unlikely(ret))
        free(kv);

    return ret;
}

int dht_kv_delete(u64 key) {
    struct kv *kv;
    u64 prehash;
    int ret;

    kv = (struct kv *)alloc_del_kv();

    if (unlikely(!kv))
        return -1;

    prehash = prehash64(&key, sizeof(u64), 0);

    init_del_kv(kv, key, prehash);

    ret = put_kv(kv, prehash);

    inc_epoch_per_thread();

    if (unlikely(ret))
        free(kv);

    return ret;
}

int dht_kv_get_context(struct kv_get_contex *get_con) {
    struct kv *kv = (struct kv *)get_con;
    u64 prehash;
    int ret;

    prehash = prehash64(&get_con->key, sizeof(u64), 0);

    set_kv_signature(kv, prehash);

    ret = get_kv(kv, prehash);

    inc_epoch_per_thread();

    return ret;
}

struct kv_get_contex *dht_kv_get(u64 key) {
    struct kv *kv;
    u64 prehash;
    int ret;

    kv = (struct kv *)alloc_kv();

    if (unlikely(!kv))
        return NULL;

    prehash = prehash64(&key, sizeof(u64), 0);

	init_get_kv(kv, key, prehash);

    ret = get_kv(kv, prehash);

    inc_epoch_per_thread();

    if (unlikely(ret))
        kv->val_len = 0;
    
    return (struct kv_get_contex *)kv;
}
#else
int dht_kv_put(void *key, void *value, 
            int key_len, int val_len) {
    struct kv *kv;
    u64 prehash;
    int ret;

    kv = (struct kv *)alloc_kv(key_len, val_len);

    if (unlikely(!kv))
        return -1;

    prehash = prehash64(key, key_len, 0);

    init_kv(kv, key, value, key_len, val_len, prehash);

    ret = put_kv(kv, prehash);

    inc_epoch_per_thread();

    if (unlikely(ret))
        free(kv);

    return ret;
}


int dht_kv_delete(void *key, int key_len) {
    return dht_kv_put(key, NULL, key_len, 0);
}

int dht_kv_get_context(struct kv_get_contex *get_con) {
    struct kv *kv = (struct kv *)get_con;
    u64 prehash;
    int ret;

    prehash = prehash64(&get_con->buffer[0], get_con->key_len, 0);

    set_kv_signature(kv, prehash);

    ret = get_kv(kv, prehash);

    inc_epoch_per_thread();

    return ret;
}

//assert max_val_len larger than 0
struct kv_get_contex *dht_kv_get(void *key, int key_len, int max_val_len) {
    struct kv *kv;
    u64 prehash;
    int ret;

    kv = (struct kv *)alloc_kv(key_len, max_val_len);

    if (unlikely(!kv))
        return NULL;

    prehash = prehash64(key, key_len, 0);

	init_get_kv(kv, key, key_len, max_val_len, prehash);

    ret = get_kv(kv, prehash);

    inc_epoch_per_thread();

    if (unlikely(ret))
        kv->val_len = 0;
    
    return (struct kv_get_contex *)kv;
}
#endif
