#include "per_thread.h"

__thread int thread_id;
__thread struct tls_context *tls_context;
struct tls_context tls_context_array[THREAD_NUM + BACKGROUNG_THREAD_NUM];

int prepare_all_tls_context() {
    struct tls_context *tls;
    struct record_page *r_page;
    int i, count = 0;

    r_page = (struct record_page *)malloc_prefault_page_aligned(
            RECORD_PAGE_SIZE * (THREAD_NUM * 2 + BACKGROUNG_THREAD_NUM * 3));

    if (unlikely((void *)r_page == MAP_FAILED))
        return -1;

    for (i = 0; i < THREAD_NUM + BACKGROUNG_THREAD_NUM; ++i) {
        tls = &tls_context_array[i];

        tls->epoch = MAX_LONG_INTEGER;
        tls->lp_split_tail = tls->hp_split_tail = INVALID_RECORD_POINTER;
        tls->page_reclaim_tail = tls->chunk_reclaim_tail = INVALID_RECORD_POINTER;
        tls->seg_alloc = tls->seg_alloc_end = NULL;

        tls->hp_split = make_record_pointer(&r_page[count++], 0);
        tls->lp_split = INVALID_RECORD_POINTER;

        r_page[count].carve = (void *)&(r_page[count].r_ent[0]);
        tls->chunk_reclaim = make_record_pointer(&r_page[count++], 0);

        if (i < THREAD_NUM)
            tls->page_reclaim = INVALID_RECORD_POINTER;
        else {
            r_page[count].carve = (void *)&(r_page[count].r_ent[0]);
            tls->page_reclaim = make_record_pointer(&r_page[count++], 0);
        }

        tls->seg_prefault = malloc_page_aligned(EH_ALLOC_POOL_SIZE);

        if (unlikely(tls->seg_prefault == MAP_FAILED)) {
            free_page_aligned(r_page, 
                RECORD_PAGE_SIZE * (THREAD_NUM * 2 + BACKGROUNG_THREAD_NUM * 3));
            
            while (i--)
                free_page_aligned(tls_context_of_tid(i)->seg_prefault, 
                                                    EH_ALLOC_POOL_SIZE);

            return -1;
        }
    }

    memory_fence();
    return 0;
}

void init_tls_context(int tid) {
    thread_id = tid;
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

    release_fence();
    WRITE_ONCE(tls_context->epoch, 0);

    memory_fence();
}

struct eh_split_entry *append_split_record(
                    RECORD_POINTER *new_pointer,
                    struct eh_split_context *split, 
                    int incomplete, int high_prio) {
    RECORD_POINTER record_ptr = (high_prio ? 
                tls_context->hp_split_tail : tls_context->lp_split_tail);
    struct eh_split_entry *ret_ent;
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

    ret_ent = &page->s_ent[ent_num];

    if (unlikely(incomplete))
        init_eh_split_incomplete_entry(ret_ent, split);
    else
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