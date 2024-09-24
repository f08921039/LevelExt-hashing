#ifndef __PER_THREAD_H
#define __PER_THREAD_H

#include "compiler.h"
#include "kv.h"

#include "eh.h"

#ifdef  __cplusplus
extern  "C" {
#endif


#ifndef THREAD_NUM
#define THREAD_NUM  20
#endif

#ifndef BACKGROUNG_THREAD_NUM
#define BACKGROUNG_THREAD_NUM   1
#endif

#define RECORD_PAGE_SIZE    PAGE_SIZE
#define RECORD_PAGE_HEADER_SIZE 16

#define SPLIT_ENT_PER_RECORD_PAGE   ((RECORD_PAGE_SIZE - RECORD_PAGE_HEADER_SIZE) / sizeof(struct eh_split_entry))
#define RECLAIM_ENT_PER_RECORD_PAGE   ((RECORD_PAGE_SIZE - RECORD_PAGE_HEADER_SIZE) / sizeof(struct reclaim_entry))

#define INVALID_RECORD_POINTER  0UL

#define page_reclaim_addr(ent) ((ent) & PAGE_MASK)
#define page_reclaim_size(ent) EXP_2((ent) & ~PAGE_MASK)

#define make_page_reclaim_ent(addr, shift)  ((uintptr_t)(addr) | (shift))

#define page_of_record_pointer(pointer) ((pointer) & VALID_POINTER_MASK)
#define ent_num_of_record_pointer(pointer) ((pointer) >> VALID_POINTER_BITS)

#define make_record_pointer(page, ent_num)  (((uintptr_t)(page)) | SHIFT_OF(ent_num, VALID_POINTER_BITS))


typedef uintptr_t RECORD_POINTER;


struct reclaim_entry {
    uintptr_t addr;
};

struct record_page {
    union {
        struct eh_split_entry s_ent[SPLIT_ENT_PER_RECORD_PAGE];
        struct reclaim_entry r_ent[RECLAIM_ENT_PER_RECORD_PAGE];
    };
    void *carve;
    struct record_page *next;
};


struct tls_context {
    u64 epoch;
    u64 split_count;
    RECORD_POINTER lp_split_tail;
    RECORD_POINTER hp_split_tail;
    RECORD_POINTER chunk_reclaim_tail;
    RECORD_POINTER page_reclaim_tail;
    void *seg_alloc;
    void *seg_alloc_end;

    RECORD_POINTER lp_split;
    RECORD_POINTER hp_split;
    RECORD_POINTER chunk_reclaim;
    RECORD_POINTER page_reclaim;
    void *seg_prefault;
    void *seg_backup;
    int seg_prefetch_level;
};

extern __thread int thread_id;
extern __thread struct tls_context *tls_context;
extern struct tls_context tls_context_array[THREAD_NUM + BACKGROUNG_THREAD_NUM];


static inline struct tls_context *tls_context_itself() {
    return tls_context;
}

static inline struct tls_context *tls_context_of_tid(int tid) {
    return &tls_context_array[tid];
}

static inline void inc_epoch_per_thread() {
    release_fence();
    WRITE_ONCE(tls_context->epoch, tls_context->epoch + 1);
}

int prepare_all_tls_context();
void init_tls_context(int tid);


struct eh_split_entry *append_split_record(
                    RECORD_POINTER *new_pointer,
                    struct eh_split_context *split, 
                    int high_prio);

void commit_split_record(
            RECORD_POINTER new_pointer, 
            int high_prio);

int reclaim_chunk(void *addr);
int reclaim_page(void *addr, int shift);


#ifdef  __cplusplus
}
#endif  //__cplusplus

#endif  //__PER_THREAD_H
