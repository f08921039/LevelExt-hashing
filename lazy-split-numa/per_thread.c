#include "per_thread.h"


__thread struct tls_context *tls_context;
__thread struct tls_context *tls_context_array;
__thread int node_id;
__thread int tls_context_id;

struct node_context node_context;

void release_all_tls_context() {
    struct per_node_context *per_nc;
    struct tls_context *tls;
    struct record_page *r_page;
    u64 pool_page_addr, pool_page_size;
    int i, j, n, total_threads, ent_num;

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
            } else if (j < per_nc[i].max_work_thread_num && 
                            tls->numa_split != INVALID_RECORD_POINTER) {
                r_page = (struct record_page *)
                            page_of_record_pointer(tls->numa_split_tail);
                ent_num = ent_num_of_record_pointer(tls->numa_split_tail);

                pool_page_addr = MUL_2(r_page->pool.base_page, PAGE_SHIFT) + 
                                            MUL_2(ent_num, EH_FOUR_SEGMENT_SIZE_BITS);
                /*pool_page_addr = (uintptr_t)r_page->mapping_seg + 
                                    MUL_2(ent_num, EH_FOUR_SEGMENT_SIZE_BIT);*/
                pool_page_size = OTHER_SEG4_POOL_SIZE - 
                                    MUL_2(ent_num, EH_FOUR_SEGMENT_SIZE_BITS);

                free_node_page((void *)pool_page_addr, pool_page_size);
                free_node_page(r_page, RECORD_PAGE_SIZE);
            }


            if (tls->seg_prefault)
                free_node_page(tls->seg_prefault, EH_ALLOC_POOL_SIZE);//to dooooooo: need to revise

            if (tls->record_pool.pool != 0) {//to dooooooo: need to revise
                pool_page_addr = MUL_2(tls->record_pool.base_page, PAGE_SHIFT);
                pool_page_size = MUL_2(tls->record_pool.page_num, RECORD_PAGE_SHIFT);

                pool_page_addr += pool_page_size;
                pool_page_size = RECORD_POOL_SIZE - pool_page_size;

                free_node_page((void *)pool_page_addr, pool_page_size);
            }
        }
    }
}

int prepare_all_tls_context() {
    struct per_node_context *per_nc;
    struct tls_context *tls;
    struct record_page *r_page;
    void *r_pool_base;
    void *other_s_pool_base;
    size_t r_size;
    int i, j, n, mt, count, total_threads, advance_spilt;

    n = node_context.node_num;
    per_nc = node_context.all_node_context;

    for (i = 0; i < n; ++i) {
        total_threads = per_nc[i].total_thread_num;

        if (total_threads == 0)
            continue;

        for (j = 0; j < total_threads; ++j) {
            tls = &per_nc[i].max_tls_context[j];

            tls->record_pool.pool = 0;
            tls->hp_split = INVALID_RECORD_POINTER;
            tls->lp_split = INVALID_RECORD_POINTER;
            tls->chunk_reclaim = INVALID_RECORD_POINTER;
            tls->page_reclaim = INVALID_RECORD_POINTER;
            tls->numa_split = INVALID_RECORD_POINTER;
            tls->seg_prefault = NULL;
        }
    }

    advance_spilt = EH_DEFAULT_ADV_SPLIT(node_context.actual_node_num);

    for (i = 0; i < n; ++i) {
        count = 0;

        total_threads = per_nc[i].total_thread_num;

        if (total_threads == 0)
            continue;

        mt = per_nc[i].max_work_thread_num;

        r_size = RECORD_PAGE_SIZE * 4 * total_threads;
        r_page = alloc_node_page(r_size, i);

        if (unlikely(r_page == NULL)) {
            release_all_tls_context();
            return -1;
        }

        //memset(r_page, 0, r_size);

        for (j = 0; j < total_threads; ++j) {
            tls = &per_nc[i].max_tls_context[j];

            tls->epoch = MAX_LONG_INTEGER;
            tls->seg_alloc = tls->seg_alloc_end = NULL;

            tls->hp_split = make_record_pointer(&r_page[count++], 0);
            tls->lp_split = make_record_pointer(&r_page[count++], 0);

            tls->hp_split_tail = tls->hp_split;
            tls->lp_split_tail = tls->lp_split;

            r_page[count].carve = (void *)&(r_page[count].r_ent[0]);
            tls->chunk_reclaim = make_record_pointer(&r_page[count++], 0);

            tls->chunk_reclaim_tail = tls->chunk_reclaim;

            if (j < mt) {
                other_s_pool_base = alloc_node_page(OTHER_SEG4_POOL_SIZE, i);
                
                if (unlikely(other_s_pool_base == NULL))
                    goto prepare_all_tls_context_failed;

                //r_page[count].mapping_seg = other_s_pool_base;
                r_page[count].pool.base_page = DIV_2((uintptr_t)other_s_pool_base, PAGE_SHIFT);
                r_page[count].pool.page_num = 0;

                tls->numa_split = make_record_pointer(&r_page[count++], 0);
                tls->numa_split_tail = tls->numa_split;

                tls->advance_spilt = advance_spilt;
            } else {
                r_page[count].carve = (void *)&(r_page[count].r_ent[0]);
                tls->page_reclaim = make_record_pointer(&r_page[count++], 0);
                tls->page_reclaim_tail = tls->page_reclaim;
            }

            tls->seg_prefault = alloc_node_page(EH_ALLOC_POOL_SIZE, i);

            if (unlikely(tls->seg_prefault == NULL))
                goto prepare_all_tls_context_failed;
            
            r_pool_base = alloc_node_page(RECORD_POOL_SIZE, i);

            if (unlikely(r_pool_base == NULL))
                goto prepare_all_tls_context_failed;
            
            tls->record_pool.page_num = 0;
            tls->record_pool.base_page = DIV_2((uintptr_t)r_pool_base, PAGE_SHIFT);

            continue;
        
        prepare_all_tls_context_failed :
            free_node_page(&r_page[count], 
                                r_size - count * RECORD_PAGE_SIZE * 4);
            release_all_tls_context();
            return -1;
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
    tls_context_id = tid;
    tls_context_array = per_nc[nid].max_tls_context;
    tls_context = tls_context_of_tid(tid);

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


struct eh_split_entry *new_split_record(
                        struct eh_split_context *split, 
                        RECORD_POINTER *new_pointer, 
                        int high_prio) {
    RECORD_POINTER record_ptr;
    void *addr;
    struct record_page *page;
    int ent_num;
    
    if (split) {
        split->dest_seg = (struct eh_four_segment *)alloc_eh_seg();
    
        if (unlikely((void *)split->dest_seg == MAP_FAILED))
            return (struct eh_split_entry *)MAP_FAILED;

        if (high_prio == -1)
            return NULL;
    }

    record_ptr = (high_prio ? 
                tls_context->hp_split_tail : tls_context->lp_split_tail);

    page = (struct record_page *)page_of_record_pointer(record_ptr);
    ent_num = ent_num_of_record_pointer(record_ptr);

    if (likely(ent_num != SPLIT_ENT_PER_RECORD_PAGE - 1))
        *new_pointer = make_record_pointer(page, ent_num + 1);
    else {
        if (likely(page->next == NULL)) {
            addr = alloc_record_page();

            if (unlikely(addr == MAP_FAILED)) {
                if (split)
                    free_page_aligned(split->dest_seg, EH_FOUR_SEGMENT_SIZE);

                return (struct eh_split_entry *)MAP_FAILED;
            }

            page->next = (struct record_page *)addr;
        }

        *new_pointer = make_record_pointer(page->next, 0);
    }

    return &page->s_ent[ent_num];
}

void commit_split_record(
                    RECORD_POINTER new_pointer, 
                    int high_prio) {
    RECORD_POINTER *record_ptr = (high_prio ? 
        (&tls_context->hp_split_tail) : (&tls_context->lp_split_tail));

    release_fence();
    WRITE_ONCE(*record_ptr, new_pointer);
}

struct eh_split_entry *new_other_split_record(
                        struct eh_split_context *split, 
                        int other_nid) {
    RECORD_POINTER record_ptr, new_r_pointer;
    struct per_node_context *per_nc;
    struct tls_context *tls;
    struct record_page *page, *new_page;
    struct eh_four_segment *seg4;
    struct eh_split_entry *ret_ent;
    PAGE_POOL pool;
    void *new_seg4;
    int ent_num, tid;

    per_nc = &node_context.all_node_context[other_nid];
    tid = tid_itself() % per_nc->max_work_thread_num;
    tls = &per_nc->max_tls_context[tid];

new_other_split_record_again :
    record_ptr = READ_ONCE(tls->numa_split_tail);

    page = (struct record_page *)page_of_record_pointer(record_ptr);
    ent_num = ent_num_of_record_pointer(record_ptr);

    ret_ent = &page->s_ent[ent_num];

    //seg4 = (struct eh_four_segment *)page->mapping_seg;
    pool.pool = READ_ONCE(page->pool.pool);
    seg4 = (struct eh_four_segment *)MUL_2(pool.base_page, PAGE_SHIFT);

    split->dest_seg = &seg4[ent_num];

    if (likely(ent_num != SPLIT_ENT_PER_RECORD_PAGE - 1)) {
        new_r_pointer = make_record_pointer(page, ent_num + 1);

        if (unlikely(!cas_bool(&tls->numa_split_tail, 
                                        record_ptr, new_r_pointer)))
            goto new_other_split_record_again;
    } else {
        new_page = READ_ONCE(page->next);

        if (new_page) {
            new_r_pointer = make_record_pointer(new_page, 0);
            cas(&tls->numa_split_tail, record_ptr, new_r_pointer);

            goto new_other_split_record_again;
        }

        new_page = (struct record_page *)
                        alloc_node_page(RECORD_PAGE_SIZE, other_nid);

        if (unlikely(new_page == NULL))
            return (struct eh_split_entry *)MAP_FAILED;

        new_seg4 = alloc_node_page(OTHER_SEG4_POOL_SIZE, other_nid);

        if (unlikely(new_seg4 == NULL)) {
            free_node_page(new_page, RECORD_PAGE_SIZE);
            return (struct eh_split_entry *)MAP_FAILED;
        }

        new_page->pool.base_page = DIV_2((uintptr_t)new_seg4, PAGE_SHIFT);
        new_page->pool.page_num = 0;
        //new_page->mapping_seg = new_seg4;
        new_page->next = NULL;

        page = cas(&page->next, NULL, new_page);

        if (unlikely(page != NULL)) {
            free_node_page(new_page, RECORD_PAGE_SIZE);
            free_node_page(new_seg4, OTHER_SEG4_POOL_SIZE);

            new_r_pointer = make_record_pointer(page, 0);
            cas(&tls->numa_split_tail, record_ptr, new_r_pointer);

            goto new_other_split_record_again;
        }

        new_r_pointer = make_record_pointer(new_page, 0);
        cas(&tls->numa_split_tail, record_ptr, new_r_pointer);
    }

    return ret_ent;
}


int reclaim_chunk(void *addr) {
    RECORD_POINTER r, record_ptr = tls_context->chunk_reclaim_tail;
    struct record_page *page = (struct record_page *)page_of_record_pointer(record_ptr);
    int ent_num = ent_num_of_record_pointer(record_ptr);

    if (likely(ent_num != RECLAIM_ENT_PER_RECORD_PAGE - 1))
        r = make_record_pointer(page, ent_num + 1);
    else {
        void *next_page = alloc_record_page();

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
        void *next_page = alloc_record_page();

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
