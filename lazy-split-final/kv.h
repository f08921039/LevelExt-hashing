#ifndef __KV_H
#define __KV_H

#include "compiler.h"

#define PREHASH_KEY_LEN 8
#define PREHASH_KEY_BITS    MUL_2(PREHASH_KEY_LEN, 3)

//#define PREHASH_KEY(key, bit1, bit2) ((key) & INTERVAL(bit1, bit2))


#ifdef  __cplusplus
extern  "C" {
#endif


#ifdef DHT_INTEGER
    typedef u64 KEY_ITEM;
    typedef u64 VALUE_ITEM;

    #define KV_SIZE sizeof(struct kv)
#else
    typedef struct {
        void *key;
        int len;
    } KEY_ITEM;

    typedef struct {
        void *value;
        int len;
    } VALUE_ITEM;

    #define MAX_KEY_BITS    8
    #define MAX_VAL_BITS    8

    #define MAX_KEY_SIZE    ~MASK(MAX_KEY_BITS)
    #define MAX_VAL_SIZE    ~MASK(MAX_VAL_BITS)

    #define KV_SIZE(key_len, val_len)   (sizeof(struct kv) + (key_len) + (val_len))

    #define KV_HEADER_LST48_KEY_START_BIT 0
    #define KV_HEADER_LST48_KEY_END_BIT 47

    #define KV_HEADER_KEY_LEN_START_BIT 48
    #define KV_HEADER_KEY_LEN_END_BIT (KV_HEADER_KEY_LEN_START_BIT + MAX_KEY_BITS - 1)

    #define KV_HEADER_VAL_LEN_START_BIT 56
    #define KV_HEADER_VAL_LEN_END_BIT (KV_HEADER_VAL_LEN_START_BIT + MAX_VAL_BITS - 1)

    #define LST48_KEY_MASK  INTERVAL(KV_HEADER_LST48_KEY_START_BIT, KV_HEADER_LST48_KEY_END_BIT)

    #define KV_COMPARE_MASK  (LST48_KEY_MASK |  \
                INTERVAL(KV_HEADER_KEY_LEN_START_BIT, KV_HEADER_KEY_LEN_END_BIT))
#endif


struct kv {
    union {
        u64 header; //8bit val_len, 8bit key_len, 48bit least significant hashed key
        struct {
        #if (BYTEORDER_ENDIAN == BYTEORDER_LITTLE_ENDIAN)
            u8 hash_lst[6];
            u8 key_len;
            u8 val_len;
        #else
            u8 val_len;
            u8 key_len;
            u8 hash_lst[6];
        #endif
        };
    };

#ifdef DHT_INTEGER
    KEY_ITEM key;
    VALUE_ITEM value;
#else
    u8 kv[0];
#endif

};//__attribute__((aligned(8)));


#ifdef DHT_INTEGER
static inline 
u64 get_kv_prehash64(struct kv *kv) {
    return kv->header;
}

static inline 
int __compare_kv_key(struct kv *kv, KEY_ITEM key) {
    return (kv->key != key);
}

static inline 
int compare_kv_key(struct kv *kv1, struct kv *kv2) {
    return (kv1->key != kv2->key);
}

static inline 
void __copy_kv_val(struct kv *kv_src, VALUE_ITEM *val_buf) {
    *val_buf = kv_src->value;
}

static inline 
void copy_kv_val(struct kv *kv_dest, struct kv *kv_src) {
    kv_dest->value = kv_src->value;
}

//8 byte alignment
static inline 
struct kv *alloc_kv() {
    return (struct kv *)malloc(KV_SIZE);
}

static inline 
void dealloc_kv(struct kv *kv) {
    free(kv);
}

static inline 
void init_kv(
        struct kv *kv, 
        KEY_ITEM key, 
        VALUE_ITEM val,
        u64 prehash) {
    kv->header = prehash;

    kv->key = key;
    kv->value = val;
}

#else
static inline 
u64 get_kv_prehash48(struct kv *kv) {
    return kv->header & LST48_KEY_MASK;
}

static inline 
int __compare_kv_key(struct kv *kv, KEY_ITEM key) {
    if (kv->key_len != key.len)
        return 1;

    return memcmp(&kv->kv[0], key.key, key.len);
}

static inline 
int compare_kv_key(struct kv *kv1, struct kv *kv2) {
    if ((kv1->header ^ kv2->header) & KV_COMPARE_MASK)
        return 1;

    return memcmp(&kv1->kv[0], &kv2->kv[0], kv1->key_len);
}

static inline 
int __copy_kv_val(struct kv *kv_src, VALUE_ITEM *val_buf) {
    int len = ((kv_src->val_len < val_buf->len) ? kv_src->val_len : val_buf->len);

    memcpy(val_buf->value, &kv_src->kv[kv_src->key_len], len);

    if (kv_src->val_len > len)
        return len - kv_src->val_len;

    return len;
}

static inline 
int copy_kv_val(struct kv *kv_dest, struct kv *kv_src) {
    int len = ((kv_src->val_len < kv_dest->val_len) ? kv_src->val_len : kv_dest->val_len);

    memcpy(&kv_dest->kv[kv_dest->key_len], &kv_src->kv[kv_src->key_len], len);

    if (kv_src->val_len > kv_dest->val_len)
        return kv_dest->val_len - kv_src->val_len;
    
    return len;
}

//8 byte alignment
static inline 
struct kv *alloc_kv(int key_len, int val_len) {
    return (struct kv *)malloc(KV_SIZE(key_len, val_len));
}

static inline 
void dealloc_kv(struct kv *kv) {
    free(kv);
}

static inline 
void init_kv(
        struct kv *kv, 
        KEY_ITEM key, 
        VALUE_ITEM val,
        u64 prehash) {
    kv->header = (prehash & LST48_KEY_MASK);
    kv->key_len = key.len;
    kv->val_len = val.len;

    memcpy(&kv->kv[0], key.key, key.len);
    memcpy(&kv->kv[key.len], val.value, val.len);
}
#endif

#ifdef  __cplusplus
}
#endif  //__cplusplus

#endif  //__KV_H
