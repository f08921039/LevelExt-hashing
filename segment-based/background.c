#include "background.h"
#include "ext_hash.h"


struct tls_context tls_context_array[THREAD_NUM];

/*int delay_to_rcpage(struct epoched_reclaim_context *reclaim_ctx, void *reclaim_ent) {
    struct reclaim_page *page, *tail = reclaim_ctx->rclist_tail;

    if (tail) {
        uintptr_t index = tail->index;

        if (likely(index != RECLAIM_PAGE_ENT_NUM)) {
            tail->entry[index++] = reclaim_ent;
            tail->index = index;
            return 0;
        }
    }

    page = (struct reclaim_page *)
                malloc_prefault_page_aligned(RECLAIM_PAGE_SIZE);

    if (unlikely((void *)page == MAP_FAILED))
        return -1;

    page->entry[0] = reclaim_ent;
    page->index = set_reclaim_page_index(1, NULL);

    reclaim_ctx->rclist_tail = page;

    if (tail)
        tail->index = set_reclaim_page_index(RECLAIM_PAGE_ENT_NUM, page);
    else
        reclaim_ctx->rclist_head = page;

    return 0;
}*/


static void hook_tls_reclaim(struct tls_context *context) {
    struct epoched_reclaim_context *reclaim;
    struct reclaim_page *rcpage;
    enum tls_reclaim_type type;

    for (type = CHUNK; type < TLS_RECLAIM_TYPE; ++type) {
        reclaim = &bg_context.reclaim[type];
        rcpage = READ_ONCE(context->rclist_tail[type]);
        
	if (rcpage) {
            struct reclaim_page *first_rcpage;

            if (!reclaim->rclist_tail)
                reclaim->rclist_tail = rcpage;
        
            release_fence();
            WRITE_ONCE(context->rclist_tail[type], NULL);

            rcpage->index = set_reclaim_page_index(RECLAIM_PAGE_ENT_NUM, reclaim->rclist_head);
        
            first_rcpage = READ_ONCE(context->rclist_head[type]);

            while (1) {
                rcpage = cas(&context->rclist_head[type], first_rcpage, NULL);

                if (likely(rcpage == first_rcpage))
                    break;

                first_rcpage = rcpage;
            }
        
            reclaim->rclist_head = first_rcpage;
        }
    }
}


__attribute__((always_inline))
static void free_reclaim_chunk(struct reclaim_page *rclist) {
    while (rclist) {
        struct reclaim_page *next = (struct reclaim_page *)next_reclaim_page(rclist->index);
        int i, idx = ent_num_of_reclaim_page(rclist->index);

        for (i = 0; i < idx; ++i)
            free(rclist->entry[i]);

        free_page_aligned(rclist, RECLAIM_PAGE_SIZE);
        rclist = next;
    }
}

__attribute__((always_inline))
static void free_reclaim_page(struct reclaim_page *rclist) {
    while (rclist) {
        struct reclaim_page *next = (struct reclaim_page *)next_reclaim_page(rclist->index);
        int i, idx = ent_num_of_reclaim_page(rclist->index);

        for (i = 0; i < idx; ++i) {
            void *addr = (void *)((uintptr_t)rclist->entry[i] & PAGE_MASK); 
            size_t size = 1UL << ((uintptr_t)rclist->entry[i] & ~PAGE_MASK);
            free_page_aligned(addr, size);
        }

        free_page_aligned(rclist, RECLAIM_PAGE_SIZE);
        rclist = next;
    }
}

__attribute__((always_inline))
static void update_reclaim_contex(u64 min_epoch) {
    struct epoched_reclaim_context *chunk_reclaim, *page_reclaim;
    struct reclaim_page *ptail, *ctail;

    chunk_reclaim = &bg_context.reclaim[CHUNK];
    page_reclaim = &bg_context.reclaim[PAGE];

    if (min_epoch > bg_context.epoch) {
        free_reclaim_page(page_reclaim->wait_rclist);
        free_reclaim_chunk(chunk_reclaim->wait_rclist);
        chunk_reclaim->wait_rclist = page_reclaim->wait_rclist = NULL;
    }

    ctail = chunk_reclaim->rclist_tail;
    ptail = page_reclaim->rclist_tail;

    if (ctail) {
        ctail->index = set_reclaim_page_index(ctail->index, chunk_reclaim->wait_rclist);
        chunk_reclaim->wait_rclist = chunk_reclaim->rclist_head;
        chunk_reclaim->rclist_head = chunk_reclaim->rclist_tail = NULL;
    }

    if (ptail) {
        ptail->index = set_reclaim_page_index(ptail->index, page_reclaim->wait_rclist);
        page_reclaim->wait_rclist = page_reclaim->rclist_head;
        page_reclaim->rclist_head = page_reclaim->rclist_tail = NULL;
    }
}


__attribute__((always_inline))
static void scan_tls_context() {
    u64 max_epoch = 0, min_epoch = MAX_LONG_INTEGER;
    int i;

    for (i = 0; i < THREAD_NUM; ++i) {
        struct tls_context *tls = &tls_context_array[i];
        u64 ep;

        ep = READ_ONCE(tls->epoch);
        acquire_fence();

        if (ep < min_epoch)
            min_epoch = ep;


        hook_tls_reclaim(tls);

        //don't need memory_fence, implied by prior lock
        ep = READ_ONCE(tls->epoch);
        acquire_fence();

        if (ep > max_epoch)
            max_epoch = ep; 
    }

    update_reclaim_contex(min_epoch);

    bg_context.epoch = max_epoch;//*epoch = max_epoch;
}

/*
__attribute__((always_inline))
static int alloc_eh_two_segment_aligned(int seg2_num, 
                        struct eh_segment **seg_addr, 
                        struct background_context *bg_context) {
    struct free_page_header *p_head;
	void *addr = malloc_prefault_page_aligned((seg2_num + 1) << EH_TWO_SEGMENT_SIZE_BIT);
    size_t n;

	if (unlikely(addr == MAP_FAILED))
		return 0;

	*seg_addr = (struct eh_segment *)(((uintptr_t)addr + EH_TWO_SEGMENT_SIZE - 1) & EH_TWO_SEGMENT_SIZE_MASK);

	if ((void *)(*seg_addr) == addr)
		return seg2_num + 1;

    n = ((uintptr_t)*seg_addr) - (uintptr_t)addr;
	p_head = (struct free_page_header *)addr;
    p_head->next = bg_context->free_page;
    p_head->size = n;

    p_head = (struct free_page_header *)&((*seg_addr)[seg2_num << 1]);
    p_head->next = (struct free_page_header *)addr;
    p_head->size = EH_TWO_SEGMENT_SIZE - n;

    bg_context->free_page = p_head;

	return seg2_num;
}*/



void *background_task(void *id) {
    u64 t, time_limit;
    int split_num;

    thread_id = (int)id;

    while (1) {
        time_limit = sys_time_us() + BACKGROUND_PERIOD;

        scan_tls_context();

        t = sys_time_us();

        if (t < time_limit)
            usleep(time_limit - t);
    }

}
