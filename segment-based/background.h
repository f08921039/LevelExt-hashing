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
    /*struct epoched_reclaim_context chunk_reclaim;
    struct epoched_reclaim_context page_reclaim;*/
    struct epoched_reclaim_context reclaim[TLS_RECLAIM_TYPE];
}__attribute__((aligned(CACHE_LINE_SIZE)));


struct background_context bg_context;


/*int delay_to_rcpage(struct epoched_reclaim_context *reclaim_ctx, 
                                                    void *reclaim_ent);



static inline int delay_chunk_to_rcpage(void *reclaim_ent) {
    struct epoched_reclaim_context *reclaim_ctx = &bg_context.chunk_reclaim;
    return delay_to_rcpage(reclaim_ctx, reclaim_ent);
}

static inline int delay_page_to_rcpage(void *reclaim_ent, size_t size_shift) {
    struct epoched_reclaim_context *reclaim_ctx = &bg_context.page_reclaim;
    uintptr_t ent = ((uintptr_t)reclaim_ent) | size_shift;
    return delay_to_rcpage(reclaim_ctx, (void *)ent);
}*/


void *background_task(void *id);

#ifdef  __cplusplus
}
#endif  //__cplusplus

#endif  //__BACKGROUND_H
