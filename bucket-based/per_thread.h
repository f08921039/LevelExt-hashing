#ifndef __PER_THREAD_H
#define __PER_THREAD_H

#include "compiler.h"
#include "kv.h"

#ifdef  __cplusplus
extern  "C" {
#endif

#define MAX_THREAD_NUM  ((1 << 13) - 1)
#define INVALID_THREAD_ID   MAX_THREAD_NUM

#ifndef THREAD_NUM
#define THREAD_NUM  20
#endif


#define RECLAIM_PAGE_SIZE    4096UL
#define RECLAIM_PAGE_ENT_NUM 511UL
#define RECLAIM_PAGE_MASK    (RECLAIM_PAGE_SIZE - 1)

#define set_reclaim_page_index(idx, next_page)  ((idx) | (uintptr_t)(next_page))
#define ent_num_of_reclaim_page(idx)  ((idx) & RECLAIM_PAGE_MASK)
#define next_reclaim_page(idx)  ((idx) & ~RECLAIM_PAGE_MASK)


struct reclaim_page {
    uintptr_t index;
    void *entry[RECLAIM_PAGE_ENT_NUM];
};



struct split_entry {
    u64 hashed_key;
    uintptr_t bucket_ptr; //last 8-bits is l_depth
};


struct tls_context {
    u64 epoch;
    struct reclaim_page *kv_rcpage;
    struct reclaim_page *kv_rclist_head;
    struct reclaim_page *kv_rclist_tail;
}__attribute__((aligned(CACHE_LINE_SIZE)));


extern struct tls_context tls_context_array[THREAD_NUM];

extern __thread int thread_id;
extern __thread struct tls_context *tls_context;

static inline void inc_epoch_per_thread() {
    release_fence();
    WRITE_ONCE(tls_context->epoch, tls_context->epoch + 1);
}

int reclaim_kv_to_rcpage(struct kv *kv);


#ifdef  __cplusplus
}
#endif  //__cplusplus

#endif  //__PER_THREAD_H
