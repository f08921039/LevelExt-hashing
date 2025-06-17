#include "per_thread.h"


__thread struct tls_context *tls_context;
__thread struct tls_context *tls_context_array;
__thread struct per_node_context *per_node_context;
__thread int node_id;
__thread int tls_context_id;


struct node_context node_context;

void modify_tls_split_entry(
                    struct eh_segment *target_seg, 
                    struct eh_segment *dest_seg) {
    struct eh_split_context *s_context;
    struct eh_split_entry *s_ent, *new_ent;
    u64 hashed_prefix;
    int depth;
    
    s_context = tls_split_context();
    s_ent = &s_context->entry;

    new_ent = new_split_record(URGENT_PRIO);

    if (unlikely((void *)new_ent == MAP_FAILED)) {
        //to dooooooooooo
        return;
    }

    hashed_prefix = s_context->hashed_prefix;
    depth = s_context->depth;

    if (unlikely(upgrade_eh_split_entry(s_ent, target_seg)))
        invalidate_eh_split_entry(new_ent);
    else
        init_eh_split_entry(new_ent, target_seg, dest_seg, 
                                hashed_prefix, depth, URGENT_SPLIT);
}

void release_all_tls_context() {
    struct per_node_context *per_nc;
    struct tls_context *tls;
    struct record_page *r_page;
    u64 pool_page_addr, pool_page_size;
    int i, j, k, n, total_threads;

    n = node_context.node_num;
    per_nc = node_context.all_node_context;

    for (i = 0; i < n; ++i) {
        total_threads = per_nc[i].total_thread_num;

        for (j = 0; j < total_threads; ++j) {
            tls = &per_nc[i].max_tls_context[j];

            for (k = 0; k < SPLIT_MAP_NUM; ++k) {
                if (tls->split_rp[k] != INVALID_RECORD_POINTER) {
                    r_page = page_of_record_pointer(tls->split_rp[k]);
                    free_node_page(r_page, RECORD_PAGE_SIZE);
                }
            }

            if (tls->chunk_reclaim != INVALID_RECORD_POINTER) {
                r_page = page_of_record_pointer(tls->chunk_reclaim);
                free_node_page(r_page, RECORD_PAGE_SIZE);
            }

            if (tls->page_reclaim != INVALID_RECORD_POINTER) {
                r_page = page_of_record_pointer(tls->page_reclaim);
                free_node_page(r_page, RECORD_PAGE_SIZE);
            }

            if (j < per_nc[i].max_work_thread_num) {//to dooooooo: need to revise
                if (tls->numa_record_pool.pool != 0) {
                    pool_page_addr = MUL_2(tls->numa_record_pool.base_page, PAGE_SHIFT);
                    pool_page_size = MUL_2(tls->numa_record_pool.page_num, RECORD_PAGE_SHIFT);

                    pool_page_addr += pool_page_size;
                    pool_page_size = RECORD_POOL_SIZE - pool_page_size;

                    free_node_page((void *)pool_page_addr, pool_page_size);
                }

                if (tls->numa_seg_pool.page_num != MAX_EH_SEG_FOR_OTHER_POOL) {
                    //to doooooooooooooooo release numa_seg_pool
                }

                if (tls->numa_extra_seg.page_num != 0) {
                    //to doooooooooooooooo release numa_extra_seg
                }
            }


            if (tls->seg_prefault)
                free_node_page(tls->seg_prefault, EH_ALLOC_POOL_SIZE);//to dooooooo: need to revise

            if (tls->seg_backup)
                free_node_page(tls->seg_backup, EH_ALLOC_POOL_SIZE);

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
    size_t r_size;
    int i, j, k, n, mt, count, total_threads;

    n = node_context.node_num;
    per_nc = node_context.all_node_context;

    for (i = 0; i < n; ++i) {
        total_threads = per_nc[i].total_thread_num;

        if (total_threads == 0)
            continue;

        for (j = 0; j < total_threads; ++j) {
            tls = &per_nc[i].max_tls_context[j];

            for (k = 0; k < SPLIT_MAP_NUM; ++k)
                tls->split_rp[k] = INVALID_RECORD_POINTER;

            tls->chunk_reclaim = INVALID_RECORD_POINTER;
            tls->page_reclaim = INVALID_RECORD_POINTER;

            tls->seg_pool.pool = 0;
            tls->record_pool.pool = 0;
            
            tls->numa_seg_pool.pool = 0;
            tls->numa_extra_seg.pool = 0;
            tls->numa_record_pool.pool = 0;
        }
    }

    for (i = 0; i < n; ++i) {
        count = 0;

        total_threads = per_nc[i].total_thread_num;

        if (total_threads == 0)
            continue;

        mt = per_nc[i].max_work_thread_num;

        r_size = RECORD_PAGE_SIZE * (SPLIT_MAP_NUM + 2) * total_threads;
        r_page = (struct record_page *)alloc_node_page(r_size, i);

        if (unlikely(r_page == (struct record_page *)MAP_FAILED)) {
            release_all_tls_context();
            return -1;
        }

        //memset(r_page, 0, r_size);

        for (j = 0; j < total_threads; ++j) {
            tls = &per_nc[i].max_tls_context[j];

            tls->epoch = MAX_LONG_INTEGER;

            tls->seg_prefault_hint = tls->seg_prefault_count = 0;
            
            for (k = 0; k < SPLIT_MAP_NUM; ++k) {
                r_page[count].counter = 0;
                r_page[count].next = NULL;
                tls->split_rp[k] = make_record_pointer(&r_page[count++], 0);
                tls->split_rp_tail[k] = tls->split_rp[k];
            }

            r_page[count].carve = (void *)&(r_page[count].r_ent[0]);
            tls->chunk_reclaim = make_record_pointer(&r_page[count++], 0);
            tls->chunk_reclaim_tail = tls->chunk_reclaim;

            r_page[count].carve = (void *)&(r_page[count].r_ent[0]);
            tls->page_reclaim = make_record_pointer(&r_page[count++], 0);
            tls->page_reclaim_tail = tls->page_reclaim;

            tls->seg_backup = NULL;

            tls->seg_prefault = alloc_node_page(EH_ALLOC_POOL_SIZE, i);

            if (unlikely(tls->seg_prefault == MAP_FAILED))
                goto prepare_all_tls_context_failed;

            tls->seg_pool.page_num = 0;
            tls->seg_pool.base_page = DIV_2((uintptr_t)tls->seg_prefault, PAGE_SHIFT);

            r_pool_base = alloc_node_page(RECORD_POOL_SIZE, i);

            if (unlikely(r_pool_base == MAP_FAILED))
                goto prepare_all_tls_context_failed;
            
            tls->record_pool.page_num = 0;
            tls->record_pool.base_page = DIV_2((uintptr_t)r_pool_base, PAGE_SHIFT);

            tls->thread_split_context.state = FEW_SPLITS;

            if (j < mt) {
                tls->thread_split_context.thread = true;
                tls->thread_split_context.entry.target = INVALID_EH_SPLIT_TARGET;
                tls->numa_seg_pool.page_num = MAX_EH_SEG_FOR_OTHER_POOL;

                r_pool_base = alloc_node_page(RECORD_POOL_SIZE, i);

                if (unlikely(r_pool_base == MAP_FAILED))
                    goto prepare_all_tls_context_failed;
            
                tls->numa_record_pool.page_num = 0;
                tls->numa_record_pool.base_page = DIV_2((uintptr_t)r_pool_base, PAGE_SHIFT);
            } else {
                tls->urgent_consume = tls->total_consume = 0.0;
                tls->low_splits = tls->urgent_splits = tls->total_splits = 0;
            }

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
    unsigned long epoch;
    int id;

    node_id = nid;
    tls_context_id = tid;

    per_node_context = &node_context.all_node_context[nid];
    tls_context_array = per_node_context->max_tls_context;
    tls_context = tls_context_of_tid(tid);

    if (tid >= per_node_context->max_work_thread_num) {
        tls_context->sample_time = sys_time_us();
        epoch = 0;
    } else {
        id = (tid / THREADS_PER_SPLIT_THREAD) + per_node_context->max_work_thread_num;
        epoch = READ_ONCE(tls_context_of_tid(id)->epoch);

        if (epoch == MAX_LONG_INTEGER)
            epoch = 0;
    }

    release_fence();
    WRITE_ONCE(tls_context->epoch, epoch);

    memory_fence();
}


struct eh_split_entry *new_split_record(SPLIT_PRIORITY prio) {
    RECORD_POINTER new_pointer, *record_ptr;
    struct record_page *page, *new_page;
    int ent_num;

    switch (prio) {
    case LOW_PRIO:
        record_ptr = &tls_context->split_rp_tail[LOW_MAP];//record_ptr = &tls_context->lp_split_tail;
        break;
    case HIGH_PRIO:
        record_ptr = &tls_context->split_rp_tail[HIGH_MAP];//record_ptr = &tls_context->hp_split_tail;
        break;
    case URGENT_PRIO:
        record_ptr = &tls_context->split_rp_tail[URGENT_MAP];//record_ptr = &tls_context->urgent_split_tail;
        break;
    case IDLE_PRIO:
        record_ptr = &tls_context->split_rp_tail[IDLE_MAP];//record_ptr = &tls_context->idle_split_tail;
        break;
    case INCOMPLETE_PRIO:
        record_ptr = &tls_context->split_rp_tail[INCOMPLETE_MAP];
        break;
    case THREAD_PRIO:
        return tls_split_entry();
    default:
        return NULL;
    }

    page = page_of_record_pointer(*record_ptr);
    ent_num = ent_num_of_record_pointer(*record_ptr);

    if (likely(ent_num != SPLIT_ENT_PER_RECORD_PAGE - 1))
        new_pointer = make_record_pointer(page, ent_num + 1);
    else {
        new_page = alloc_record_page();

        if (unlikely((void *)new_page == MAP_FAILED))
            return (struct eh_split_entry *)MAP_FAILED;

        page->next = new_page;

        new_page->next = NULL;
        new_page->counter = page->counter + SPLIT_ENT_PER_RECORD_PAGE;

        new_pointer = make_record_pointer(new_page, 0);
    }

    release_fence();
    WRITE_ONCE(*record_ptr, new_pointer);

    return &page->s_ent[ent_num];
}

struct eh_split_entry *new_other_split_record(
                            SPLIT_PRIORITY prio, 
                            int other_nid) {
    RECORD_POINTER rp, new_rp, *record_ptr;
    struct per_node_context *per_nc;
    struct tls_context *tls;
    struct record_page *page, *new_page;
    struct eh_split_entry *ret_ent;
    int ent_num, tid;

    per_nc = &node_context.all_node_context[other_nid];
    tid = tid_itself() % per_nc->max_work_thread_num;
    tls = &per_nc->max_tls_context[tid];

    record_ptr = ((prio == URGENT_PRIO) ? 
                    &tls->split_rp_tail[URGENT_NUMA_MAP] : 
                    &tls->split_rp_tail[NUMA_MAP]);

new_other_split_record_again :
    rp = READ_ONCE(*record_ptr);

    page = page_of_record_pointer(rp);
    ent_num = ent_num_of_record_pointer(rp);

    ret_ent = &page->s_ent[ent_num];

    if (likely(ent_num != SPLIT_ENT_PER_RECORD_PAGE - 1)) {
        new_rp = make_record_pointer(page, ent_num + 1);

        if (unlikely(!cas_bool(record_ptr, rp, new_rp)))
            goto new_other_split_record_again;
    } else {
        new_page = READ_ONCE(page->next);

        if (new_page) {
            new_rp = make_record_pointer(new_page, 0);
            cas(record_ptr, rp, new_rp);

            goto new_other_split_record_again;
        }

        new_page = alloc_other_record_page(other_nid);

        if (unlikely((void *)new_page == MAP_FAILED))
            return (struct eh_split_entry *)MAP_FAILED;

        new_page->next = NULL;
        new_page->counter = page->counter + SPLIT_ENT_PER_RECORD_PAGE;

        page = cas(&page->next, NULL, new_page);

        if (unlikely(page != NULL)) {
            free_node_page(new_page, RECORD_PAGE_SIZE);

            new_rp = make_record_pointer(page, 0);
            cas(record_ptr, rp, new_rp);

            goto new_other_split_record_again;
        }

        new_rp = make_record_pointer(new_page, 0);
        cas(record_ptr, rp, new_rp);
    }

    return ret_ent;
}


int reclaim_chunk(void *addr) {
    RECORD_POINTER r, record_ptr;
    struct record_page *page, *new_page;
    int ent_num;

    record_ptr = tls_context->chunk_reclaim_tail;
    page = page_of_record_pointer(record_ptr);
    ent_num = ent_num_of_record_pointer(record_ptr);

    if (likely(ent_num != RECLAIM_ENT_PER_RECORD_PAGE - 1))
        r = make_record_pointer(page, ent_num + 1);
    else {
        new_page = alloc_record_page();

        if (unlikely((void *)new_page == MAP_FAILED))
            return -1;

        page->next = new_page;
        r = make_record_pointer(new_page, 0);
    }

    page->r_ent[ent_num].addr = (uintptr_t)addr;

    release_fence();
    WRITE_ONCE(tls_context->chunk_reclaim_tail, r);

    return 0;
}

int reclaim_page(void *addr, int shift) {
    RECORD_POINTER r, record_ptr;
    struct record_page *page, *new_page;
    int ent_num;

    record_ptr = tls_context->page_reclaim_tail;
    page = page_of_record_pointer(record_ptr);
    ent_num = ent_num_of_record_pointer(record_ptr);

    if (likely(ent_num != RECLAIM_ENT_PER_RECORD_PAGE - 1))
        r = make_record_pointer(page, ent_num + 1);
    else {
        new_page = alloc_record_page();

        if (unlikely((void *)new_page == MAP_FAILED))
            return -1;

        page->next = new_page;
        r = make_record_pointer(new_page, 0);
    }

    page->r_ent[ent_num].addr = make_page_reclaim_ent(addr, shift);

    release_fence();
    WRITE_ONCE(tls_context->page_reclaim_tail, r);

    return 0;
}
