#include "per_thread.h"

__thread struct tls_context *tls_context;
__thread struct tls_context *tls_context_array;
__thread int node_id;

struct node_context node_context;

void release_all_tls_context() {
    struct per_node_context *per_nc;
    struct tls_context *tls;
    struct record_page *r_page;
    int i, j, n, total_threads;

    n = node_context.node_num;
    per_nc = node_context.all_node_context;

    for (i = 0; i < n; ++i) {
        total_threads = per_nc[i].total_thread_num;

        for (j = 0; j < total_threads; ++j) {
            tls = &per_nc[i].max_tls_context[j];

            if (tls->hp_split != INVALID_RECORD_POINTER) {
                r_page = (struct record_page *)
                                page_of_record_pointer(tls->hp_split);
                free_node_page(r_page, RECORD_PAGE_SIZE);
            }

            if (tls->lp_split != INVALID_RECORD_POINTER) {
                r_page = (struct record_page *)
                                page_of_record_pointer(tls->lp_split);
                free_node_page(r_page, RECORD_PAGE_SIZE);
            }

            if (tls->chunk_reclaim != INVALID_RECORD_POINTER) {
                r_page = (struct record_page *)
                            page_of_record_pointer(tls->chunk_reclaim);
                free_node_page(r_page, RECORD_PAGE_SIZE);
            }

            if (j >= per_nc[i].max_work_thread_num && 
                            tls->page_reclaim != INVALID_RECORD_POINTER) {
                r_page = (struct record_page *)
                            page_of_record_pointer(tls->page_reclaim);
                free_node_page(r_page, RECORD_PAGE_SIZE);
            }

            if (tls->seg_prefault)
                free_node_page(tls->seg_prefault, EH_ALLOC_POOL_SIZE);
        }
    }
}

int prepare_all_tls_context() {
    struct per_node_context *per_nc;
    struct tls_context *tls;
    struct record_page *r_page;
    size_t r_size;
    int i, j, n, mt, st, count, total_threads;

    n = node_context.node_num;
    per_nc = node_context.all_node_context;

    for (i = 0; i < n; ++i) {
        total_threads = per_nc[i].total_thread_num;

        if (total_threads == 0)
            continue;

        for (j = 0; j < total_threads; ++j) {
            tls = &per_nc[i].max_tls_context[j];

            tls->hp_split = INVALID_RECORD_POINTER;
            tls->lp_split = INVALID_RECORD_POINTER;
            tls->chunk_reclaim = INVALID_RECORD_POINTER;
            tls->page_reclaim = INVALID_RECORD_POINTER;
            tls->seg_prefault = NULL;
        }
    }

    for (i = 0; i < n; ++i) {
        count = 0;

        total_threads = per_nc[i].total_thread_num;

        if (total_threads == 0)
            continue;

        mt = per_nc[i].max_work_thread_num;
        st = total_threads - mt;

        r_size = RECORD_PAGE_SIZE * (3 * mt + 4 * st);
        r_page = alloc_node_page(r_size, i);

        if (unlikely(r_page == NULL)) {
            release_all_tls_context();
            return -1;
        }

        memset(r_page, 0, r_size);

        for (j = 0; j < total_threads; ++j) {
            tls = &per_nc[i].max_tls_context[j];

            tls->epoch = MAX_LONG_INTEGER;
            tls->lp_split_tail = tls->hp_split_tail = INVALID_RECORD_POINTER;
            tls->page_reclaim_tail = tls->chunk_reclaim_tail = INVALID_RECORD_POINTER;
            tls->seg_alloc = tls->seg_alloc_end = NULL;

            tls->hp_split = make_record_pointer(&r_page[count++], 0);
            tls->lp_split = make_record_pointer(&r_page[count++], 0);

            r_page[count].carve = (void *)&(r_page[count].r_ent[0]);
            tls->chunk_reclaim = make_record_pointer(&r_page[count++], 0);

            if (j < mt)
                tls->page_reclaim = INVALID_RECORD_POINTER;
            else {
                r_page[count].carve = (void *)&(r_page[count].r_ent[0]);
                tls->page_reclaim = make_record_pointer(&r_page[count++], 0);
            }

            tls->seg_prefault = alloc_node_page(EH_ALLOC_POOL_SIZE, i);

            if (unlikely(tls->seg_prefault == NULL)) {
                release_all_tls_context();
                return -1;
            }
        }
    }

    memory_fence();
    return 0;
}

void init_tls_context(int nid, int tid) {
    struct per_node_context *per_nc;
    u64 epoch;

    per_nc = node_context.all_node_context;

    node_id = nid;
    tls_context_array = per_nc[nid].max_tls_context;
    tls_context = tls_context_of_tid(tid);

    WRITE_ONCE(tls_context->lp_split_tail, tls_context->lp_split);
    WRITE_ONCE(tls_context->hp_split_tail, tls_context->hp_split);

    WRITE_ONCE(tls_context->chunk_reclaim_tail, tls_context->chunk_reclaim);
    WRITE_ONCE(tls_context->page_reclaim_tail, tls_context->page_reclaim);

    tls_context->seg_alloc = tls_context->seg_prefault;
    tls_context->seg_backup = NULL;
    tls_context->seg_prefetch_level = 0;

    release_fence();
    WRITE_ONCE(tls_context->seg_alloc_end, tls_context->seg_alloc + EH_ALLOC_POOL_SIZE);

    if (tid >= per_nc[nid].max_work_thread_num)
        epoch = 0;
    else {
        int id = (tid / THREADS_PER_SPLIT_THREAD) + per_nc[nid].max_work_thread_num;
        epoch = READ_ONCE(tls_context_of_tid(id)->epoch);

        if (epoch == MAX_LONG_INTEGER)
            epoch = 0;
    }

    release_fence();
    WRITE_ONCE(tls_context->epoch, epoch);

    memory_fence();
}



__attribute__((always_inline))
static struct eh_split_entry *new_split_record(
                        RECORD_POINTER record_ptr, 
                        RECORD_POINTER *new_pointer) {
    struct record_page *page = (struct record_page *)
                                page_of_record_pointer(record_ptr);
    int ent_num = ent_num_of_record_pointer(record_ptr);

    if (likely(ent_num != SPLIT_ENT_PER_RECORD_PAGE - 1))
        *new_pointer = make_record_pointer(page, ent_num + 1);
    else {
        void *addr = malloc_prefault_page_aligned(RECORD_PAGE_SIZE);

        if (unlikely(addr == MAP_FAILED))
            return (struct eh_split_entry *)MAP_FAILED;

        page->next = (struct record_page *)addr;
        *new_pointer = make_record_pointer(addr, 0);
    }

    return &page->s_ent[ent_num];
}

int upgrade_split_record(
            RECORD_POINTER *new_pointer,
            struct eh_split_entry *lp_entry) {
    struct eh_split_entry *ret_ent = new_split_record(
                            tls_context->hp_split_tail, new_pointer);
    
    if (likely((void *)ret_ent != MAP_FAILED))
        return upgrade_eh_split_entry(ret_ent, lp_entry);
    
    return -1;
}

struct eh_split_entry *append_split_record(
                    RECORD_POINTER *new_pointer,
                    struct eh_split_context *split, 
                    int high_prio) {
    RECORD_POINTER record_ptr = (high_prio ? 
                tls_context->hp_split_tail : tls_context->lp_split_tail);
    struct eh_split_entry *ret_ent = new_split_record(record_ptr, new_pointer);
    
    if (likely((void *)ret_ent != MAP_FAILED))
        init_eh_split_entry(ret_ent, split);

    return ret_ent;
}

void commit_split_record(
                    RECORD_POINTER new_pointer, 
                    int high_prio) {
    RECORD_POINTER *record_ptr = (high_prio ? 
        (&tls_context->hp_split_tail) : (&tls_context->lp_split_tail));

    release_fence();
    WRITE_ONCE(*record_ptr, new_pointer);
}


int reclaim_chunk(void *addr) {
    RECORD_POINTER r, record_ptr = tls_context->chunk_reclaim_tail;
    struct record_page *page = (struct record_page *)page_of_record_pointer(record_ptr);
    int ent_num = ent_num_of_record_pointer(record_ptr);

    if (likely(ent_num != RECLAIM_ENT_PER_RECORD_PAGE - 1))
        r = make_record_pointer(page, ent_num + 1);
    else {
        void *next_page = malloc_prefault_page_aligned(RECORD_PAGE_SIZE);

        if (unlikely(next_page == MAP_FAILED))
            return -1;

        page->next = (struct record_page *)next_page;
        r = make_record_pointer(next_page, 0);
    }

    page->r_ent[ent_num].addr = (uintptr_t)addr;

    release_fence();
    WRITE_ONCE(tls_context->chunk_reclaim_tail, r);

    return 0;
}

int reclaim_page(void *addr, int shift) {
    RECORD_POINTER r, record_ptr = tls_context->page_reclaim_tail;
    struct record_page *page = (struct record_page *)page_of_record_pointer(record_ptr);
    int ent_num = ent_num_of_record_pointer(record_ptr);

    if (likely(ent_num != RECLAIM_ENT_PER_RECORD_PAGE - 1))
        r = make_record_pointer(page, ent_num + 1);
    else {
        void *next_page = malloc_prefault_page_aligned(RECORD_PAGE_SIZE);

        if (unlikely(next_page == MAP_FAILED))
            return -1;

        page->next = (struct record_page *)next_page;
        r = make_record_pointer(next_page, 0);
    }

    page->r_ent[ent_num].addr = make_page_reclaim_ent(addr, shift);

    release_fence();
    WRITE_ONCE(tls_context->page_reclaim_tail, r);

    return 0;
}
