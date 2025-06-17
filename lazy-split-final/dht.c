#include "dht.h"
#include "kv.h"
#include "prehash.h"

#include "per_thread.h"

#include "eh.h"

#ifdef DHT_INTEGER
int dht_kv_insert(u64 key, u64 value) {
    u64 prehash;
    int ret;
    
    prehash = prehash64(&key, sizeof(u64), 0);

    ret = eh_insert_kv(key, value, prehash);

    inc_epoch_per_thread();

    return ret;
}

int dht_kv_update(u64 key, u64 value) {
    u64 prehash;
    int ret;

    prehash = prehash64(&key, sizeof(u64), 0);

    ret = eh_update_kv(key, value, prehash);

    inc_epoch_per_thread();

    return ret;
}

int dht_kv_lookup(u64 key, u64 *val_buf) {
    struct kv *kv;
    u64 prehash;
    int ret;

    prehash = prehash64(&key, sizeof(u64), 0);

    kv = eh_lookup_kv(key, prehash);

    if (kv == NULL)
        ret = 1;
    else {
        ret = 0;
        __copy_kv_val(kv, val_buf);
    }

    inc_epoch_per_thread();

    return ret;
}

int dht_kv_delete(u64 key) {
    u64 prehash;
    int ret;

    prehash = prehash64(&key, sizeof(u64), 0);

    ret = eh_delete_kv(key, prehash);

    inc_epoch_per_thread();

    return ret;
}

#else
int dht_kv_insert(void *key, void *value, 
            int key_len, int val_len) {
    KEY_ITEM key_item;
    VALUE_ITEM val_item;
    u64 prehash;
    int ret;

    prehash = prehash64(key, key_len, 0);

    key_item.key = key;
    key_item.len = key_len;

    val_item.value = value;
    val_item.len = val_len;

    ret = eh_insert_kv(key_item, val_item, prehash);

    inc_epoch_per_thread();

    return ret;
}

int dht_kv_update(void *key, void *value, 
            int key_len, int val_len) {
    KEY_ITEM key_item;
    VALUE_ITEM val_item;
    u64 prehash;
    int ret;

    prehash = prehash64(key, key_len, 0);

    key_item.key = key;
    key_item.len = key_len;

    val_item.value = value;
    val_item.len = val_len;

    ret = eh_update_kv(key_item, val_item, prehash);

    inc_epoch_per_thread();

    return ret;
}

int dht_kv_delete(void *key, int key_len) {
    u64 prehash;
    KEY_ITEM key_item;
    int ret;

    key_item.key = key;
    key_item.len = key_len;

    prehash = prehash64(key, key_len, 0);

    ret = eh_delete_kv(key_item, prehash);

    inc_epoch_per_thread();

    return ret;
}

int dht_kv_lookup(void *key, void *val_buf, int key_len, int buf_len) {
    u64 prehash;
    struct kv *kv;
    KEY_ITEM key_item;
    VALUE_ITEM val_item;
    int ret;

    key_item.key = key;
    key_item.len = key_len;

    prehash = prehash64(key, key_len, 0);

    kv = eh_lookup_kv(key_item, prehash);

    if (kv == NULL)
        ret = 0;
    else {
        val_item.value = val_buf;
        val_item.len = buf_len;
        ret = __copy_kv_val(kv, &val_item);
    }

    inc_epoch_per_thread();

    return ret;
}
#endif
