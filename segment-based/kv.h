#ifndef __KV_H
#define __KV_H

#include "compiler.h"


#ifdef  __cplusplus
extern  "C" {
#endif

/*static void* (*kv_alloc)(size_t) = &malloc;
static void (*kv_free)(void *) = &free;*/

#ifdef DHT_INTEGER
    #define KV_SIZE (sizeof(struct kv) + sizeof(u64) * 2) 
#else
    #define KV_SIZE(key_len, val_len)   (sizeof(struct kv) + (key_len) + (val_len))
#endif




#define KV_COMPARE_MASK  ((1UL << 56) - 1)
#define deleted_kv(kv_addr)  ((kv_addr)->val_len == 0)
#define specific_interval_kv_hashed_key(kv_addr, fingerprint16, depth, interval_bit)    \
                                            ((((kv_addr)->header << 16) | (fingerprint16))  \
                                                        << (depth)) >> (64 - (interval_bit))
                                                        
#define combined_kv_hashed_key(kv_addr, fingerprint16)  specific_interval_kv_hashed_key(kv_addr, fingerprint16, 0, 64) 

struct kv {
    union {
        u64 header; //8bit val_len, 8bit key_len, 48bit most significant hashed key
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

//8 byte alignment
static inline struct kv *alloc_del_kv() {
    return (struct kv *)malloc(KV_SIZE - sizeof(u64));
}

static inline void init_kv(struct kv *kv, 
                        u64 key, u64 val,
                        u64 prehash) {
    kv->header = (prehash >> 16);
    kv->val_len = sizeof(u64);

    kv->kv[0] = key;
    kv->kv[1] = val;
}

static inline void init_del_kv(struct kv *kv, 
                            u64 key, u64 prehash) {
    kv->header = (prehash >> 16);
    kv->kv[0] = key;
}

static inline void init_get_kv(struct kv *kv, 
                        u64 key, u64 prehash) {
    kv->header = (prehash >> 16);
    kv->val_len = sizeof(u64);

    kv->kv[0] = key;
}

static inline void set_kv_signature(struct kv *kv, u64 prehash) {
    kv->header = (prehash >> 16);
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

static inline void init_kv(struct kv *kv, 
                            void *key, void *val,
                            int key_len, int val_len ,
                            u64 prehash) {
    kv->header = (prehash >> 16);
    kv->key_len = key_len;
    kv->val_len = val_len;

    memcpy(&kv->kv[0], key, key_len);
    memcpy(&kv->kv[key_len], key, val_len);
}

static inline void init_get_kv(struct kv *kv, 
                            void *key, int key_len, 
                            int val_len, u64 prehash) {
    kv->header = (prehash >> 16);
    kv->key_len = key_len;
    kv->val_len = val_len;

    memcpy(&kv->kv[0], key, key_len);
}

static inline void set_kv_signature(struct kv *kv, u64 prehash) {
    kv->header = (kv->header & ~((1UL << 48) - 1)) | (prehash >> 16);
}
#endif

#ifdef  __cplusplus
}
#endif  //__cplusplus

#endif  //__KV_H
