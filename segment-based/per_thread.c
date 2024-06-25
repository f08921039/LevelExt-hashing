#include "per_thread.h"

__thread int thread_id;
__thread struct tls_context *tls_context;



int reclaim_to_rcpage(void *ent, enum tls_reclaim_type type) {
    struct reclaim_page *head, *page = tls_context->rcpage[type];
    u64 index = page->index;

    page->entry[index] = ent;

    if (likely(++index != RECLAIM_PAGE_ENT_NUM))
        page->index = set_reclaim_page_index(index, NULL);
    else {
        tls_context->rcpage[type] = (struct reclaim_page *)
                        malloc_prefault_page_aligned(RECLAIM_PAGE_SIZE);

        if (unlikely((void *)tls_context->rcpage[type] == MAP_FAILED)) {
            tls_context->rcpage[type] = page;
            return -1;
        }

        tls_context->rcpage[type]->index = 0;

        head = READ_ONCE(tls_context->rclist_head[type]);
        page->index = set_reclaim_page_index(index, head);

        if (!head || unlikely(!cas_bool(&tls_context->rclist_head[type], head, page))) {
            release_fence();
            WRITE_ONCE(tls_context->rclist_head[type], page);
            release_fence();
            WRITE_ONCE(tls_context->rclist_tail[type], page);
        }
    }

    return 0;
}
