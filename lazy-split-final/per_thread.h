#ifndef __PER_THREAD_H
#define __PER_THREAD_H

#include "compiler.h"
#include "kv.h"

#include "eh.h"

#ifdef  __cplusplus
extern  "C" {
#endif

#define THREADS_PER_SPLIT_THREAD    9


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
    unsigned char gc_enable;
    unsigned char gc_main;
    unsigned short global_depth;
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


typedef enum {
	URGENT_NUMA_MAP = 0, 
    INCOMPLETE_MAP = URGENT_NUMA_MAP, 
    URGENT_MAP = 2, 
    NUMA_MAP = 1, 
	HIGH_MAP = 3, 
	LOW_MAP = 4, 
	IDLE_MAP = 5, 
    SPLIT_MAP_NUM = 6
} /*__attribute__ ((__packed__))*/ SPLIT_MAP;

struct tls_context {
    void *seg_backup;
    void *seg_prefault;
    unsigned long seg_prefault_count;
    union {
        struct {
            PAGE_POOL numa_seg_pool;
            PAGE_POOL numa_extra_seg;
            PAGE_POOL numa_record_pool;
        };
        struct {
            unsigned long urgent_splits;
            unsigned long low_splits;
            unsigned long total_splits;
        };
    };
    RECORD_POINTER chunk_reclaim;
    RECORD_POINTER page_reclaim;

    RECORD_POINTER split_rp[SPLIT_MAP_NUM];
    RECORD_POINTER split_rp_tail[SPLIT_MAP_NUM];
    RECORD_POINTER chunk_reclaim_tail;
    RECORD_POINTER page_reclaim_tail;
    PAGE_POOL seg_pool;
    PAGE_POOL record_pool;

    unsigned long padding;
    unsigned long seg_prefault_hint;
    union {
        struct eh_split_context thread_split_context;
        struct {
            unsigned long sample_time;
            double urgent_consume;
            double total_consume;
        };
    };
    unsigned long epoch;
};


extern __thread struct tls_context *tls_context;
extern __thread struct tls_context *tls_context_array;
extern __thread struct per_node_context *per_node_context;
extern __thread int node_id;
extern __thread int tls_context_id;

extern struct node_context node_context;

static inline 
struct tls_context *tls_context_itself() {
    return tls_context;
}

static inline 
struct tls_context *tls_context_of_tid(int tid) {
    return &tls_context_array[tid];
}

static inline 
int tid_itself() {
    return tls_context_id;
}

static inline 
int tls_node_id() {
    return node_id;
}

static inline 
struct per_node_context *tls_node_context() {
    return per_node_context;
}

static inline 
struct eh_split_context *tls_split_context() {
    return &tls_context->thread_split_context;
}

static inline 
struct eh_split_entry *tls_split_entry() {
    struct eh_split_context *split_context;

    split_context = tls_split_context();

    return &split_context->entry;
}

static inline 
SPLIT_STATE tls_split_state() {
    struct eh_split_context *split_context;
    SPLIT_STATE state;

    split_context = tls_split_context();

    state = READ_ONCE(split_context->state);

    return state;
}

static inline 
bool is_tls_help_split() {
    struct eh_split_entry *s_ent;
    uintptr_t target_ent;

    s_ent = tls_split_entry();

    target_ent = READ_ONCE(s_ent->target);

    return (target_ent != INVALID_EH_SPLIT_TARGET);
}

static inline 
void init_tls_split_entry(
			struct eh_segment *target_seg, 
			struct eh_segment *dest_seg, 
			u64 hashed_key, 
            int depth, 
            SPLIT_TYPE type) {
    struct eh_split_context *s_context;
    uintptr_t target_ent;
    
    s_context = tls_split_context();

    s_context->hashed_prefix = hashed_key & MASK(PREHASH_KEY_BITS - depth);
    s_context->depth = depth;
    s_context->bucket_id = 0;
    s_context->dest_seg = dest_seg;
    s_context->inter_seg = NULL;
    s_context->type = type;

    target_ent = make_eh_split_target_entry(target_seg, 0, 0, type);

    release_fence();
    WRITE_ONCE(s_context->entry.target, target_ent);
}

static inline
struct eh_split_context *possess_tls_split_context() {
    struct eh_split_context *s_context;
    struct eh_split_entry *s_ent;
    uintptr_t target, possess_target;
    struct eh_segment *target_seg;

    s_context = tls_split_context();
    s_ent = &s_context->entry;

    target = READ_ONCE(s_ent->target);

    if (target == INVALID_EH_SPLIT_TARGET)
        return NULL;

    target_seg = (struct eh_segment *)(target & EH_SPLIT_TARGET_SEG_MASK);
    possess_target = make_eh_split_target_entry(target_seg, 0, 0, INVALID_SPLIT);

    if (unlikely(!cas_bool(&s_ent->target, target, possess_target)))
        return NULL;

    return s_context;
}

void modify_tls_split_entry(
                    struct eh_segment *target_seg, 
                    struct eh_segment *dest_seg);

static inline
int dispossess_tls_split_context() {
    struct eh_split_context *s_context;
    struct eh_split_entry *s_ent;
    EH_BUCKET_HEADER header, ori_header;
    uintptr_t target_ent;
    struct eh_segment *dest_seg, *target_seg;
    SPLIT_TYPE type;

    s_context = tls_split_context();
    s_ent = &s_context->entry;

    dest_seg = s_context->dest_seg;
    target_seg = s_context->target_seg;
    type = s_context->type;

    target_ent = make_eh_split_target_entry(target_seg, 0, 0, type);

    release_fence();
    WRITE_ONCE(s_ent->target, target_ent);

    memory_fence();
    header = READ_ONCE(dest_seg->bucket[0].header);

    if (unlikely(eh_seg_low(header)) || 
                unlikely(type == URGENT_SPLIT && 
                            header == INITIAL_EH_BUCKET_TOP_HEADER)) {
        if (type == NORMAL_SPLIT)
            dest_seg = eh_next_high_seg(header);
        else {
            ori_header = set_eh_split_entry(s_ent, THREAD2_PRIO);
            cas(&dest_seg[2].bucket[0].header, ori_header, INITIAL_EH_BUCKET_TOP_HEADER);
        }

        modify_tls_split_entry(target_seg, dest_seg);
    }

    return 0;
}


static inline 
void inc_epoch_per_thread() {
    release_fence();
    WRITE_ONCE(tls_context->epoch, tls_context->epoch + 1);
}

int prepare_all_tls_context();
void release_all_tls_context();

void init_tls_context(int nid, int tid);


struct eh_split_entry *new_split_record(SPLIT_PRIORITY prio);
struct eh_split_entry *new_other_split_record(
                            SPLIT_PRIORITY prio, 
                            int other_nid);

int reclaim_chunk(void *addr);
int reclaim_page(void *addr, int shift);


#ifdef  __cplusplus
}
#endif  //__cplusplus

#endif  //__PER_THREAD_H
