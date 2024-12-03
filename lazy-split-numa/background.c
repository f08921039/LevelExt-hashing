#include "background.h"
#include "eh.h"

enum split_type {
    HP_SPLIT = 0,
    NUMA_SPLIT,  
    LP_SPLIT, 
    SPLIT_TYPE_NUM
};

__attribute__((always_inline))
static void update_epoch_for_split(
                    struct tls_context *tls, 
                    struct tls_context *self) {
    u64 epoch;

    epoch = READ_ONCE(tls->epoch);

    if (epoch != MAX_LONG_INTEGER) {
        epoch += 100;

        if (epoch > self->epoch) {
            release_fence();
            WRITE_ONCE(self->epoch, epoch);
        }
    }
}

__attribute__((always_inline))
static int is_valid_nuam_split_entry(struct eh_split_entry *ent) {
    uintptr_t target;

    target = READ_ONCE(ent->target);

    if (unlikely(target == INITIAL_EH_SPLIT_TARGET))
        return 0;

    return 1;
}

static int process_split_entry(
                            int tid_begin, 
                            int tid_end, 
                            u64 time_limit) {
    struct tls_context *tls, *self;
    RECORD_POINTER tail, head, *split_tail, *split_head;
    struct eh_split_entry *ent, *tail_ent;
    struct record_page *h_page, *t_page, *page;
    enum split_type s_type, old_s_type;
    int h_ent_num, t_ent_num, i, accum, repeat, prio;

    self = tls_context_itself();
    old_s_type = SPLIT_TYPE_NUM;

process_split_entry_again :
    repeat = 0;

    for (s_type = HP_SPLIT; s_type < SPLIT_TYPE_NUM; ++s_type) {
        for (i = tid_begin; i < tid_end; ++i) {
            tls = tls_context_of_tid(i);

        process_split_entry_self :
            if (s_type == NUMA_SPLIT) {
                prio = -1;
                split_tail = &tls->numa_split_tail;
                split_head = &tls->numa_split;
                accum = (old_s_type == SPLIT_TYPE_NUM) ? 32768 : 512;
            } else if (s_type == HP_SPLIT) {
                prio = 1;
                split_tail = &tls->hp_split_tail;
                split_head = &tls->hp_split;
                accum = (old_s_type == SPLIT_TYPE_NUM) ? 32768 : 512;
            } else {
                prio = 0;
                split_tail = &tls->lp_split_tail;
                split_head = &tls->lp_split;
                accum = 512;
            }

            tail = READ_ONCE(*split_tail);
            head = *split_head;
    
            t_page = (struct record_page *)page_of_record_pointer(tail);
            t_ent_num = ent_num_of_record_pointer(tail);

            h_page = (struct record_page *)page_of_record_pointer(head);
            h_ent_num = ent_num_of_record_pointer(head);

            tail_ent = &t_page->s_ent[t_ent_num];
            ent = &h_page->s_ent[h_ent_num];

            while (ent != tail_ent) {
                if (s_type == NUMA_SPLIT && !is_valid_nuam_split_entry(ent))
                    break;

                if (unlikely(eh_split(ent, prio) == -1)) {
                    //to do : handle memeory insufficient problem
                }

                if (h_ent_num != SPLIT_ENT_PER_RECORD_PAGE - 1)
                    h_ent_num += 1;
                else {
                    h_ent_num = 0;
                    page = h_page->next;
                    reclaim_page(h_page, RECORD_PAGE_SHIFT);
                    h_page = page;
                }

                ent = &h_page->s_ent[h_ent_num];

                if (unlikely(--accum == 0))
                    break;
            }

            *split_head = make_record_pointer(h_page, h_ent_num);

            update_epoch_for_split(tls, self);
            
            if (unlikely(old_s_type != SPLIT_TYPE_NUM)) {
                s_type = old_s_type;
                old_s_type = SPLIT_TYPE_NUM;
            }

            if (unlikely(accum == 0)) {
                if (unlikely(s_type != HP_SPLIT && time_limit < sys_time_us()))
                    return 1;

                repeat = 1;
                
                if (s_type == LP_SPLIT) {
                    old_s_type = LP_SPLIT;
                    s_type = HP_SPLIT;
                    goto process_split_entry_self;
                }
            }
        }

        if (s_type != NUMA_SPLIT && tls != self) {
            tls = self;
            goto process_split_entry_self;
        }

        if (s_type != HP_SPLIT && time_limit < sys_time_us())
            return 1;
            
        if (repeat && s_type == NUMA_SPLIT)
            goto process_split_entry_again;
    }

    if (repeat)
        goto process_split_entry_again;

    return 0;
}

/*static void process_numa_split_entry(int tid_begin, int tid_end) {
    struct tls_context *tls, *self;
    RECORD_POINTER tail, head;
    struct eh_split_entry *ent, *tail_ent;
    struct record_page *h_page, *t_page, *page;
    int h_ent_num, t_ent_num;
    int i;

    self = tls_context_itself();

    for (i = tid_begin; i < tid_end; ++i) {
        tls = tls_context_of_tid(i);
        tail = READ_ONCE(tls->numa_split_tail);
        head = tls->numa_split;
    
        t_page = (struct record_page *)page_of_record_pointer(tail);
        t_ent_num = ent_num_of_record_pointer(tail);

        h_page = (struct record_page *)page_of_record_pointer(head);
        h_ent_num = ent_num_of_record_pointer(head);

        tail_ent = &t_page->s_ent[t_ent_num];
        ent = &h_page->s_ent[h_ent_num];

        while (ent != tail_ent) {
            if (!is_valid_nuam_split_entry(ent))
                break;

            if (unlikely(eh_split(ent, -1) == -1)) {
                //to do : handle memeory insufficient problem
            }

            if (h_ent_num != SPLIT_ENT_PER_RECORD_PAGE - 1)
                h_ent_num += 1;
            else {
                h_ent_num = 0;
                page = h_page->next;
                reclaim_page(h_page, RECORD_PAGE_SHIFT);
                h_page = page;
            }

            ent = &h_page->s_ent[h_ent_num];
        }

        tls->numa_split = make_record_pointer(h_page, h_ent_num);

        update_epoch_for_split(tls, self);
    }
}


static void process_hp_split_entry(int tid_begin, int tid_end) {
    struct tls_context *tls, *self;
    RECORD_POINTER tail, head;
    struct eh_split_entry *ent, *tail_ent;
    struct record_page *h_page, *t_page, *page;
    int h_ent_num, t_ent_num;
    int i;

    self = tls_context_itself();

    for (i = tid_begin; i < tid_end; ++i) {
        tls = tls_context_of_tid(i);
        tail = READ_ONCE(tls->hp_split_tail);
        head = tls->hp_split;
    
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

        update_epoch_for_split(tls, self);
    }

    if (tls != self) {
        tls = self;
        tail = tls->hp_split_tail;
        head = tls->hp_split;

        goto eh_hp_split_for_each_thread;
    }
}

static int process_lp_split_entry(
                            int tid_begin, 
                            int tid_end, 
                            u64 time_limit) {
    struct tls_context *tls, *self;
    RECORD_POINTER tail, *head_ptr;
    struct eh_split_entry *ent, *tail_ent;
    struct record_page *h_page, *t_page, *page;
    int h_ent_num, t_ent_num;
    int i, hp, repeat;

    self = tls_context_itself();

re_process_lp_split_entry :
    repeat = 0;

    for (i = tid_begin; i < tid_end; ++i) {
        tls = tls_context_of_tid(i);
        tail = READ_ONCE(tls->lp_split_tail);

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

        update_epoch_for_split(tls, self);

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

    if (tls != self) {
        tls = self;
        tail = tls->lp_split_tail;

        head_ptr = &tls->lp_split;
        hp = 0;
        goto eh_split_for_each_thread;
    }

    if (repeat)
        goto re_process_lp_split_entry;

    return 0;
}*/


void *split_task(void *parameter) {
    union split_task_parameter task_parameter;
    struct split_task_input input;
    struct per_node_context *per_nc;
    u64 time_limit, t;
    int tid_begin, tid_end;

    task_parameter.parameter = parameter;
    input = task_parameter.input;

    per_nc = &node_context.all_node_context[input.nid];

    init_tls_context(input.nid, per_nc->max_work_thread_num + input.tid);
    
    tid_begin = input.tid * THREADS_PER_SPLIT_THREAD;
    tid_end = tid_begin + THREADS_PER_SPLIT_THREAD;

    if (tid_end > per_nc->max_work_thread_num)
        tid_end = per_nc->max_work_thread_num;


    while (1) {
        time_limit = sys_time_us() + BACKGROUND_SPLIT_PERIOD;

        /*process_numa_split_entry(tid_begin, tid_end);

        process_hp_split_entry(tid_begin, tid_end);

        if (process_lp_split_entry(tid_begin, tid_end, time_limit))
            continue;*/
        if (process_split_entry(tid_begin, tid_end, time_limit))
            continue;

        t = sys_time_us();

        if (t < time_limit)
            usleep(time_limit - t);
    }

}

void collect_all_thread_info() {
    struct per_node_context *per_nc;
    struct tls_context *tls, *tls_array;
    RECORD_POINTER rp, *chunk_rp, *page_rp;
    u64 ep;
    int t, mt, n, nodes, total_threads;

    nodes = node_context.node_num;

    for (n = 0; n < nodes; ++n) {
        per_nc = &node_context.all_node_context[n];

        total_threads = per_nc->total_thread_num;
        mt = per_nc->max_work_thread_num;

        if (total_threads == 0)
            continue;

        tls_array = per_nc->max_tls_context;

        chunk_rp = per_nc->chunk_rp;
        page_rp = per_nc->page_rp;

        for (t = 0; t < total_threads; ++t) {
            tls = &tls_array[t];

            if (t >= mt) {
                rp = READ_ONCE(tls->page_reclaim_tail);
                WRITE_ONCE(page_rp[t - mt], rp);
            }

            rp = READ_ONCE(tls->chunk_reclaim_tail);
            WRITE_ONCE(chunk_rp[t], rp);

            acquire_fence();

            ep = READ_ONCE(tls->epoch);

            if (ep == MAX_LONG_INTEGER)
                continue;

            if (ep > node_context.max_epoch)
                node_context.max_epoch = ep;

            if (ep < node_context.min_epoch)
                node_context.min_epoch = ep;
        }
    }
}

__attribute__((always_inline))
static void prefault_memory() {
    struct per_node_context *per_nc;
    int t, total_threads, work_threads;

    per_nc = &node_context.all_node_context[node_id];
    total_threads = per_nc->total_thread_num;
    work_threads = per_nc->max_work_thread_num;

    if (total_threads > 0) {
        for (t = 0; t < total_threads; ++t)
            prefault_eh_seg(t);
        
        for (t = 0; t < work_threads; ++t)
            prefault_other_eh_seg(t);
    }
}

void free_reclaim_area(
            struct tls_context *tls, 
            RECORD_POINTER tail,  
            int page_type) {
    RECORD_POINTER *head_ptr;
    struct reclaim_entry *ent, *carve;
    struct record_page *h_page, *t_page, *page;
    u64 t;
    int h_ent_num, t_ent_num, reclaim_count;

    head_ptr = (page_type ? &tls->page_reclaim : &tls->chunk_reclaim);

    h_page = (struct record_page *)page_of_record_pointer(*head_ptr);
    h_ent_num = ent_num_of_record_pointer(*head_ptr);

    t_page = (struct record_page *)page_of_record_pointer(tail);
    t_ent_num = ent_num_of_record_pointer(tail);

    carve = (struct reclaim_entry *)h_page->carve;

    ent = &h_page->r_ent[h_ent_num];
    
    reclaim_count = 0;

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
        
        if (++reclaim_count == 256) {
            reclaim_count = 0;
            prefault_memory();
        }
    }

    h_page->carve = (void *)&t_page->r_ent[t_ent_num];

    *head_ptr = make_record_pointer(h_page, h_ent_num);
}

__attribute__((always_inline))
static void free_all_reclaim_page() {
    struct per_node_context *per_nc;
    struct tls_context *tls;
    RECORD_POINTER rp, *page_rp;
    int t, mt, total_threads;


    per_nc = &node_context.all_node_context[node_id];

    total_threads = per_nc->total_thread_num;
    mt = per_nc->max_work_thread_num;

    if (total_threads > 0) {
        page_rp = per_nc->page_rp;

        for (t = mt; t < total_threads; ++t) {
            tls = tls_context_of_tid(t);
            rp = READ_ONCE(page_rp[t - mt]);
            
            if (likely(rp != INVALID_RECORD_POINTER))
              free_reclaim_area(tls, rp, 1);
        }
    }
}

__attribute__((always_inline))
static void free_all_reclaim_chunk() {
    struct per_node_context *per_nc;
    struct tls_context *tls;
    RECORD_POINTER rp, *chunk_rp;
    int t, total_threads;

    per_nc = &node_context.all_node_context[node_id];

    total_threads = per_nc->total_thread_num;

    if (total_threads > 0) {
        chunk_rp = per_nc->chunk_rp;

        for (t = 0; t < total_threads; ++t) {
            tls = tls_context_of_tid(t);
            rp = READ_ONCE(chunk_rp[t]);
            
            if (likely(rp != INVALID_RECORD_POINTER))
              free_reclaim_area(tls, rp, 0);
        }
    }
}

__attribute__((always_inline))
int check_gc_version() {
    struct per_node_context *per_nc;
    int nodes, n;

    nodes = node_context.node_num;

    for (n = 0; n < nodes; ++n) {
        per_nc = &node_context.all_node_context[n];

        if (per_nc->total_thread_num == 0)
            continue;

        if (node_context.gc_version != READ_ONCE(per_nc->gc_version))
            return -1;
    }

    return 0;
}


void *gc_task(void *parameter) {
    u64 time_limit, t;
    struct per_node_context *per_nc;

    node_id = (int)parameter;
    per_nc = &node_context.all_node_context[node_id];
    tls_context_array = per_nc->max_tls_context;
    
    if (per_nc->gc_main) {
        node_context.epoch = 0;
        node_context.max_epoch = 0;
        node_context.min_epoch = MAX_LONG_INTEGER;
    }


    while (1) {
        time_limit = sys_time_us() + BACKGROUND_GC_PERIOD;

        if (per_nc->gc_main) {
            node_context.min_epoch = MAX_LONG_INTEGER;
            collect_all_thread_info();

            if (unlikely(node_context.min_epoch == MAX_LONG_INTEGER)) {
                usleep(BACKGROUND_GC_PERIOD);
                continue;
            }
        }

        prefault_memory();

        if (per_nc->gc_main && 
                node_context.epoch < node_context.min_epoch
                                    && check_gc_version() == 0) {
            node_context.epoch = node_context.max_epoch;

            release_fence();
            WRITE_ONCE(node_context.gc_version, node_context.gc_version + 1);
        }

        if (per_nc->gc_version != READ_ONCE(node_context.gc_version)) {
            free_all_reclaim_page();
            free_all_reclaim_chunk();

            release_fence();
            WRITE_ONCE(per_nc->gc_version, node_context.gc_version);
        }
        
        t = sys_time_us();

        if (t < time_limit)
            usleep(time_limit - t);
    }
}
