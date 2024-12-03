#ifndef __PER_THREAD_H
#define __PER_THREAD_H

#include "compiler.h"
#include "kv.h"

#include "eh.h"

#ifdef  __cplusplus
extern  "C" {
#endif

#define THREADS_PER_SPLIT_THREAD    36

#define RECORD_PAGE_SHIFT    (PAGE_SHIFT + 3)
#define RECORD_PAGE_SIZE    EXP_2(RECORD_PAGE_SHIFT)
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


#define MAX_OTHER_SEG4_IN_POOL  SPLIT_ENT_PER_RECORD_PAGE
#define OTHER_SEG4_POOL_SIZE \
            MUL_2(MAX_OTHER_SEG4_IN_POOL, EH_FOUR_SEGMENT_SIZE_BITS)


typedef uintptr_t RECORD_POINTER;


struct reclaim_entry {
    uintptr_t addr;
};

struct record_page {
    union {
        struct eh_split_entry s_ent[SPLIT_ENT_PER_RECORD_PAGE];
        struct reclaim_entry r_ent[RECLAIM_ENT_PER_RECORD_PAGE];
    };
    union {
        PAGE_POOL pool;
        void *carve;
        //void *mapping_seg;
    };
    struct record_page *next;
};


struct thread_paramater {
    int thread_id;
    int node_id;
    pthread_t work_pthread_id;
    pthread_t split_pthread_id;
    void *(*callback_fuction)(void *);
    void *paramater;
};

struct per_node_context {
    int max_work_thread_num;
    int work_thread_num;
    int split_thread_num;
    int total_thread_num;
    short gc_enable;
    short gc_main;
    int gc_version;
    pthread_t gc_pthread;
    struct tls_context *max_tls_context;
    struct thread_paramater *thread_paramater;
    RECORD_POINTER *chunk_rp;
    RECORD_POINTER *page_rp;
};

struct node_context {
    short node_num;
    short actual_node_num;
    int gc_version;
    u64 epoch;
    u64 max_epoch;
    u64 min_epoch;
    struct per_node_context *all_node_context;
};

struct tls_context {
    u64 epoch;
    PAGE_POOL record_pool;
    RECORD_POINTER lp_split_tail;
    RECORD_POINTER hp_split_tail;
    RECORD_POINTER chunk_reclaim_tail;
    union {
        RECORD_POINTER page_reclaim_tail;
        int advance_spilt;
    };
    void *seg_alloc;
    void *seg_alloc_end;

    RECORD_POINTER lp_split;
    RECORD_POINTER hp_split;
    RECORD_POINTER chunk_reclaim;
    union {
        RECORD_POINTER page_reclaim;
        struct {
            RECORD_POINTER numa_split;
            RECORD_POINTER numa_split_tail;
        };
    };
    void *seg_prefault;
    void *seg_backup;
    int seg_prefetch_level;
    int fullness;
};


extern __thread struct tls_context *tls_context;
extern __thread struct tls_context *tls_context_array;
extern __thread int node_id;
extern __thread int tls_context_id;

extern struct node_context node_context;


static inline struct tls_context *tls_context_itself() {
    return tls_context;
}

static inline struct tls_context *tls_context_of_tid(int tid) {
    return &tls_context_array[tid];
}

static inline int tid_itself() {
    return tls_context_id;
}

static inline int tls_adv_split_count() {
    return tls_context->advance_spilt;
}

static inline int tls_node_id() {
    return node_id;
}


static inline void inc_epoch_per_thread() {
    release_fence();
    WRITE_ONCE(tls_context->epoch, tls_context->epoch + 1);
}

int prepare_all_tls_context();
void release_all_tls_context();

void init_tls_context(int nid, int tid);


struct eh_split_entry *new_split_record(
                        struct eh_split_context *split, 
                        RECORD_POINTER *new_pointer, 
                        int high_prio);

void commit_split_record(
            RECORD_POINTER new_pointer, 
            int high_prio);

struct eh_split_entry *new_other_split_record(
                        struct eh_split_context *split, 
                        int other_nid);

int reclaim_chunk(void *addr);
int reclaim_page(void *addr, int shift);


#ifdef  __cplusplus
}
#endif  //__cplusplus

#endif  //__PER_THREAD_H
