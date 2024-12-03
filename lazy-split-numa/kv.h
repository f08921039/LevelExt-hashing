#ifndef __KV_H
#define __KV_H

#include "compiler.h"

#define PREHASH_KEY_LEN 8
#define PREHASH_KEY_BITS    MUL_2(PREHASH_KEY_LEN, 3)

#define PREHASH_KEY(key, bit1, bit2) ((key) & INTERVAL(bit1, bit2))


#ifdef  __cplusplus
extern  "C" {
#endif


#ifdef DHT_INTEGER
    #define KV_SIZE (sizeof(struct kv) + sizeof(u64) * 2) 
#else
    #define KV_SIZE(key_len, val_len)   (sizeof(struct kv) + (key_len) + (val_len))
#endif


#define LST48_KEY_MASK  ~MASK(48)

#define KV_COMPARE_MASK  ~MASK(56)


struct kv {
    union {
        u64 header; //8bit val_len, 8bit key_len, 48bit least significant hashed key
        struct {
        #if (BYTEORDER_ENDIAN == BYTEORDER_LITTLE_ENDIAN)
            s16 diff;
            u8 padding[4];
            u8 key_len;
            u8 val_len;
        #else
            u8 val_len;
            u8 key_len;
            u8 padding[4];
            s16 diff;
        #endif
        };
    };

#ifdef DHT_INTEGER
    u64 kv[0];
#else
    u8 kv[0];
#endif

};//__attribute__((aligned(8)));


#ifdef DHT_INTEGER
static inline int compare_kv_key(struct kv *kv1, struct kv *kv2) {
    return (kv1->kv[0] != kv2->kv[0]);
}

static inline void copy_kv_val(struct kv *kv1, struct kv *kv2) {
    kv1->kv[1] = kv2->kv[1];
}

//8 byte alignment
static inline struct kv *alloc_kv() {
    return (struct kv *)malloc(KV_SIZE);
}
/*
static inline struct kv *alloc_del_kv() {
    return (struct kv *)malloc(KV_SIZE - sizeof(u64));
}*/

static inline void init_kv(struct kv *kv, 
                        u64 key, u64 val,
                        u64 prehash) {
    kv->header = prehash;

    kv->kv[0] = key;
    kv->kv[1] = val;
}

static inline void init_del_kv(struct kv *kv, 
                            u64 key, u64 prehash) {
    kv->header = prehash;
    kv->kv[0] = key;
}

static inline void init_get_kv(struct kv *kv, 
                        u64 key, u64 prehash) {
    kv->header = prehash;
    kv->kv[0] = key;
}

static inline void set_kv_signature(struct kv *kv, u64 prehash) {
    kv->header = prehash;
}

static inline u64 get_kv_prehash64(struct kv *kv) {
    return kv->header;
}
#else
static inline int compare_kv_key(struct kv *kv1, struct kv *kv2) {
    if ((kv1->header ^ kv2->header) & KV_COMPARE_MASK)
        return 1;

    return memcmp(&kv1->kv[0], &kv2->kv[0], kv1->key_len);
}

static inline void copy_kv_val(struct kv *kv1, struct kv *kv2) {
    memcpy(&kv1->kv[kv1->key_len], &kv2->kv[kv1->key_len], 
            ((kv1->val_len < kv2->val_len) ? kv1->val_len : kv2->val_len));
    kv1->diff = (s16)(kv1->val_len - kv2->val_len);
}

//8 byte alignment
static inline struct kv *alloc_kv(int key_len, int val_len) {
    return (struct kv *)malloc(KV_SIZE(key_len, val_len));
}

static inline struct kv *alloc_del_kv(int key_len) {
    return alloc_kv(KV_SIZE(key_len, 0));
}

static inline void init_kv(struct kv *kv, 
                            void *key, void *val,
                            int key_len, int val_len,
                            u64 prehash) {
    kv->header = (prehash & LST48_KEY_MASK);
    kv->key_len = key_len;
    kv->val_len = val_len;

    memcpy(&kv->kv[0], key, key_len);
    memcpy(&kv->kv[key_len], key, val_len);
}

static inline void init_del_kv(struct kv *kv, 
                            void *key, int key_len,
                            u64 prehash) {
    kv->header = (prehash & LST48_KEY_MASK);
    kv->key_len = key_len;
    kv->val_len = 0;

    memcpy(&kv->kv[0], key, key_len);
}

static inline void init_get_kv(struct kv *kv, 
                            void *key, int key_len, 
                            int val_len, u64 prehash) {
    kv->header = (prehash & LST48_KEY_MASK);
    kv->key_len = key_len;
    kv->val_len = val_len;

    memcpy(&kv->kv[0], key, key_len);
}

static inline void set_kv_signature(struct kv *kv, u64 prehash) {
    kv->header = (kv->header & ~LST48_KEY_MASK) | (prehash & LST48_KEY_MASK);
}

static inline u64 get_kv_prehash48(struct kv *kv) {
    return kv->header & LST48_KEY_MASK;
}
#endif

#ifdef  __cplusplus
}
#endif  //__cplusplus

#endif  //__KV_H
