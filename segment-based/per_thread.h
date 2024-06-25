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

enum tls_reclaim_type {
    CHUNK = 0,
    PAGE = 1,
    TLS_RECLAIM_TYPE = 2
};


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
    struct reclaim_page *rcpage[TLS_RECLAIM_TYPE];
    struct reclaim_page *rclist_head[TLS_RECLAIM_TYPE];
    struct reclaim_page *rclist_tail[TLS_RECLAIM_TYPE];
}__attribute__((aligned(CACHE_LINE_SIZE)));


extern struct tls_context tls_context_array[THREAD_NUM];

extern __thread int thread_id;
extern __thread struct tls_context *tls_context;

static inline void inc_epoch_per_thread() {
    release_fence();
    WRITE_ONCE(tls_context->epoch, tls_context->epoch + 1);
}

int reclaim_to_rcpage(void *ent, enum tls_reclaim_type type);

static inline int reclaim_page_to_rcpage(void *ent, size_t size_shift) {
	uintptr_t e = ((uintptr_t)ent) | size_shift;
	return reclaim_to_rcpage((void *)e, PAGE);
}

static inline int reclaim_chunk_to_rcpage(void *ent) {
	return reclaim_to_rcpage(ent, CHUNK);
}


#ifdef  __cplusplus
}
#endif  //__cplusplus

#endif  //__PER_THREAD_H
