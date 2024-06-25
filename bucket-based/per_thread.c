#include "per_thread.h"

__thread int thread_id;
__thread struct tls_context *tls_context;



int reclaim_kv_to_rcpage(struct kv *kv) {
    struct reclaim_page *head, *page = tls_context->kv_rcpage;
    u64 index = page->index;

    page->entry[index] = kv;

    if (likely(++index != RECLAIM_PAGE_ENT_NUM))
        page->index = set_reclaim_page_index(index, NULL);
    else {
        tls_context->kv_rcpage = (struct reclaim_page *)
                        malloc_prefault_page_aligned(RECLAIM_PAGE_SIZE);

        if (unlikely((void *)tls_context->kv_rcpage == MAP_FAILED)) {
            tls_context->kv_rcpage = page;
            return -1;
        }

        tls_context->kv_rcpage->index = 0;

        head = READ_ONCE(tls_context->kv_rclist_head);
        page->index = set_reclaim_page_index(index, head);

        if (!head || unlikely(!cas_bool(&tls_context->kv_rclist_head, head, page))) {
            release_fence();
            WRITE_ONCE(tls_context->kv_rclist_head, page);
            release_fence();
            WRITE_ONCE(tls_context->kv_rclist_tail, page);
        }
    }

    return 0;
}