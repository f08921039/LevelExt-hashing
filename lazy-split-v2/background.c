#include "background.h"
#include "eh.h"


#if BACKGROUNG_THREAD_NUM > 1
    #define TLS_SCAN_START(id)  (((id) - THREAD_NUM) * PER_THREAD_OF_BACKGROUND)
    #define TLS_SCAN_END(id)  (((id) == BACKGROUNG_THREAD_NUM - 1) ?    \
            (THREAD_NUM : (TLS_SCAN_START(id) + PER_THREAD_OF_BACKGROUND)))
#else
    #define TLS_SCAN_START(id)  0
    #define TLS_SCAN_END(id)    THREAD_NUM
#endif

void free_reclaim_area(
            struct tls_context *tls, 
            RECORD_POINTER tail, 
            int page_type) {
    RECORD_POINTER *head_ptr;
    struct reclaim_entry *ent, *carve;
    struct record_page *h_page, *t_page, *page;
    int h_ent_num, t_ent_num;

    head_ptr = (page_type ? &tls->page_reclaim : &tls->chunk_reclaim);

    h_page = (struct record_page *)page_of_record_pointer(*head_ptr);
    h_ent_num = ent_num_of_record_pointer(*head_ptr);

    t_page = (struct record_page *)page_of_record_pointer(tail);
    t_ent_num = ent_num_of_record_pointer(tail);

    carve = (struct reclaim_entry *)h_page->carve;

    ent = &h_page->r_ent[h_ent_num];

    while (ent != carve) {
        if (!page_type)
            free((void *)ent->addr);
        else
            free_page_aligned((void *)page_reclaim_addr(ent->addr), 
                            page_reclaim_size(ent->addr));

        if (h_ent_num != RECLAIM_ENT_PER_RECORD_PAGE - 1)
            h_ent_num += 1;
        else {
            h_ent_num = 0;
            page = h_page->next;
            free_page_aligned(h_page, RECORD_PAGE_SIZE);
            h_page = page;
        }

        ent = &h_page->r_ent[h_ent_num];
    }

    h_page->carve = (void *)&t_page->r_ent[t_ent_num];

    *head_ptr = make_record_pointer(h_page, h_ent_num);
}

__attribute__((always_inline))
static void free_reclaim_page(
            struct tls_context *tls, 
            RECORD_POINTER tail) {
    free_reclaim_area(tls, tail, 1);
}

__attribute__((always_inline))
static void free_reclaim_chunk(
            struct tls_context *tls,  
            RECORD_POINTER tail) {
    free_reclaim_area(tls, tail, 0);
}

#if BACKGROUNG_THREAD_NUM > 1
void free_reclaim_memory() {
    RECORD_POINTER chunk_rp[PER_THREAD_OF_BACKGROUND];
    u64 epoch, max_epoch = 0, min_epoch = MAX_LONG_INTEGER;
    struct tls_context *tls;
    int i, background_range, background_range_end;
    
    background_range = TLS_SCAN_START(thread_id);
    background_range_end = TLS_SCAN_END(thread_id);

    for (i = 0; i < THREAD_NUM + BACKGROUNG_THREAD_NUM; ++i) {
        tls = tls_context_of_tid(i);
        
        if (tls == tls_context_itself())
            continue;

        if (i >= background_range && i < background_range_end) {
            chunk_rp[i - background_range] = 
                    READ_ONCE(tls->chunk_reclaim_tail);
            acquire_fence();
        }

        epoch = READ_ONCE(tls->epoch);

        if (epoch == MAX_LONG_INTEGER)
            continue;

        if (epoch > max_epoch)
            max_epoch = epoch;

        if (epoch < min_epoch)
            min_epoch = epoch;
    }

    if (unlikely(min_epoch == MAX_LONG_INTEGER))
        return;

    tls = tls_context_itself();

    if (unlikely(tls->epoch == min_epoch)) {
        release_fence();
        WRITE_ONCE(tls->epoch, min_epoch + thread_id - THREAD_NUM);
    } else if (tls->epoch < min_epoch) {
        free_reclaim_page(tls, tls->page_reclaim_tail);
        free_reclaim_chunk(tls, tls->chunk_reclaim_tail);

        for (i = background_range; i < background_range_end; ++i) {
            if (chunk_rp[i - background_range] != INVALID_RECORD_POINTER) {
                tls = tls_context_of_tid(i);
                free_reclaim_chunk(tls, chunk_rp[i - background_range]);
            }
        }

        release_fence();
        tls = tls_context_itself();
        WRITE_ONCE(tls->epoch, max_epoch);
    }
}
#else
void free_reclaim_memory() {
    RECORD_POINTER chunk_rp[PER_THREAD_OF_BACKGROUND];
    u64 epoch, max_epoch = 0, min_epoch = MAX_LONG_INTEGER;
    struct tls_context *tls;
    int i;

    for (i = 0; i < THREAD_NUM; ++i) {
        tls = tls_context_of_tid(i);

        chunk_rp[i] = READ_ONCE(tls->chunk_reclaim_tail);
        acquire_fence();

        epoch = READ_ONCE(tls->epoch);

        if (epoch == MAX_LONG_INTEGER)
            continue;

        if (epoch > max_epoch)
            max_epoch = epoch;

        if (epoch < min_epoch)
            min_epoch = epoch;
    }

    tls = tls_context_itself();

    if (likely(min_epoch != MAX_LONG_INTEGER) && 
                        tls->epoch < min_epoch) {
        free_reclaim_page(tls, tls->page_reclaim_tail);
        free_reclaim_chunk(tls, tls->chunk_reclaim_tail);

        for (i = 0; i < THREAD_NUM; ++i) {
            if (chunk_rp[i] != INVALID_RECORD_POINTER) {
                tls = tls_context_of_tid(i);
                free_reclaim_chunk(tls, chunk_rp[i]);
            }
        }

        tls = tls_context_itself();
        tls->epoch = max_epoch;
    }
}    
#endif

static void process_hp_split_entry() {
    struct tls_context *tls;
    RECORD_POINTER tail, head;
    struct eh_split_entry *ent, *tail_ent;
    struct record_page *h_page, *t_page, *page;
    int h_ent_num, t_ent_num;
    int i;

    for (i = TLS_SCAN_START(thread_id); i < TLS_SCAN_END(thread_id); ++i) {
        tls = tls_context_of_tid(i);
        tail = READ_ONCE(tls->hp_split_tail);
        head = tls->hp_split;

        if (tail == INVALID_RECORD_POINTER)
            continue;
    
    eh_hp_split_for_each_thread :
        t_page = (struct record_page *)page_of_record_pointer(tail);
        t_ent_num = ent_num_of_record_pointer(tail);

        h_page = (struct record_page *)page_of_record_pointer(head);
        h_ent_num = ent_num_of_record_pointer(head);

        tail_ent = &t_page->s_ent[t_ent_num];
        ent = &h_page->s_ent[h_ent_num];

        while (ent != tail_ent) {
            if (unlikely(eh_split(ent, 1) == -1)) {
                //to do : handle memeory insufficient problem
            }

            if (h_ent_num != SPLIT_ENT_PER_RECORD_PAGE - 1)
                h_ent_num += 1;
            else {
                h_ent_num = 0;
                page = h_page->next;
                free_page_aligned(h_page, RECORD_PAGE_SIZE);
                h_page = page;
            }

            ent = &h_page->s_ent[h_ent_num];
        }

        tls->hp_split = tail;
    }

    if (tls != tls_context_itself()) {
        tls = tls_context_itself();
        tail = tls->hp_split_tail;
        head = tls->hp_split;

        if (tail != INVALID_RECORD_POINTER)
            goto eh_hp_split_for_each_thread;
    }
}

static int process_lp_split_entry(u64 time_limit) {
    struct tls_context *tls;
    RECORD_POINTER tail, *head_ptr;
    struct eh_split_entry *ent, *tail_ent;
    struct record_page *h_page, *t_page, *page;
    int h_ent_num, t_ent_num;
    int i, hp, repeat;

re_process_lp_split_entry :
    repeat = 0;

    for (i = TLS_SCAN_START(thread_id); i < TLS_SCAN_END(thread_id); ++i) {
        tls = tls_context_of_tid(i);
        tail = READ_ONCE(tls->lp_split_tail);

        if (tail == INVALID_RECORD_POINTER)
            continue;

        head_ptr = &tls->lp_split;
        hp = 0;

    eh_split_for_each_thread :
        t_page = (struct record_page *)page_of_record_pointer(tail);
        t_ent_num = ent_num_of_record_pointer(tail);

        h_page = (struct record_page *)page_of_record_pointer(*head_ptr);
        h_ent_num = ent_num_of_record_pointer(*head_ptr);

        tail_ent = &t_page->s_ent[t_ent_num];
        ent = &h_page->s_ent[h_ent_num];

        while (ent != tail_ent) {
            if (unlikely(eh_split(ent, hp) == -1)) {
                //to do : handle memeory insufficient problem
            }

            if (h_ent_num == SPLIT_ENT_PER_RECORD_PAGE - 1) {
                page = h_page->next;
                free_page_aligned(h_page, RECORD_PAGE_SIZE);

                tail = make_record_pointer(page, 0);
                break;
            }

            h_ent_num += 1;
            ent = &h_page->s_ent[h_ent_num];
        }

        *head_ptr = tail;

        if (time_limit < sys_time_us())
            return 1;

        if (ent != tail_ent && hp == 0) {
            tail = READ_ONCE(tls->hp_split_tail);

            repeat = 1;

            if (tail != tls->hp_split) {
                hp = 1;
                head_ptr = &tls->hp_split;
                goto eh_split_for_each_thread;
            }
        }
    }

    if (tls != tls_context_itself()) {
        tls = tls_context_itself();
        tail = tls->lp_split_tail;

        if (tail != INVALID_RECORD_POINTER) {
            head_ptr = &tls->lp_split;
            hp = 0;
            goto eh_split_for_each_thread;
        }
    }

    if (repeat)
        goto re_process_lp_split_entry;

    return 0;
}

static void prefault_memory() {
    int i;

    for (i = TLS_SCAN_START(thread_id); i < TLS_SCAN_END(thread_id); ++i)
        prefault_eh_seg(i);
}


void *background_task(void *id) {
    u64 time_limit, t;

    init_tls_context(THREAD_NUM + (int)id);

    while (1) {
        time_limit = sys_time_us() + BACKGROUND_PERIOD;

        free_reclaim_memory();

        process_hp_split_entry();

        if (process_lp_split_entry(time_limit))
            continue;

        prefault_memory();

        t = sys_time_us();

        if (t < time_limit)
            usleep(time_limit - t);
    }

}