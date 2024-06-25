#ifndef __BACKGROUND_H
#define __BACKGROUND_H

#include "compiler.h"
#include "per_thread.h"


#define BACKGROUND_PERIOD   2000

#ifdef  __cplusplus
extern  "C" {
#endif

extern __thread int thread_id;
extern __thread struct tls_context *tls_context;
extern struct tls_context tls_context_array[THREAD_NUM];


struct epoched_reclaim_context {
    struct reclaim_page *rclist_head;
    struct reclaim_page *rclist_tail;
    struct reclaim_page *wait_rclist;
    //struct reclaim_page *free_rclist;
};




struct background_context {
    u64 epoch;
    struct epoched_reclaim_context chunk_reclaim;
    struct epoched_reclaim_context page_reclaim;
}__attribute__((aligned(CACHE_LINE_SIZE)));


extern struct background_context bg_context;
static int background_lock = 0;


int delay_to_rcpage(struct epoched_reclaim_context *reclaim_ctx, 
                                                    void *reclaim_ent);


static inline int delay_chunk_to_rcpage(void *reclaim_ent) {
    struct epoched_reclaim_context *reclaim_ctx = &bg_context.chunk_reclaim;
    return delay_to_rcpage(reclaim_ctx, reclaim_ent);
}

static inline int delay_page_to_rcpage(void *reclaim_ent, size_t size_shift) {
    struct epoched_reclaim_context *reclaim_ctx = &bg_context.page_reclaim;
    uintptr_t ent = ((uintptr_t)reclaim_ent) | size_shift;
    int ret;

    while (unlikely(READ_ONCE(background_lock)) || unlikely(!cas_bool(&background_lock, 0, 1)))
        spin_fence();
        
    ret = delay_to_rcpage(reclaim_ctx, (void *)ent);
    
    release_fence();
    WRITE_ONCE(background_lock, 0);
    return ret;
}


void *background_task(void *id);

#ifdef  __cplusplus
}
#endif  //__cplusplus

#endif  //__BACKGROUND_H
