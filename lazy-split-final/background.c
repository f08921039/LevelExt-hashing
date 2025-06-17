#include "background.h"
#include "eh.h"

#define SPLIT_HEAVY_RATIO   (1.0 / 8)
#define SPLIT_WAIT_TIME_LIMIT   100.0

#define SPLIT_RR_RECORD_ENTS   256

#define SPLIT_RECHECK_LOOP_ENTS 128

#define SPLIT_CONSUME_HISTORY_RATIO 0.25
#define SPLIT_CONSUME_COUNTING_THREHOLD 32


#define SPLIT_EXP_SMOOTH_CONSUME_RATE(old, new) \
            ((old) * SPLIT_CONSUME_HISTORY_RATIO + (new) * (1.0 - SPLIT_CONSUME_HISTORY_RATIO))

typedef enum {
	URGENT_NUMA_ORDER = 0,
	URGENT_ORDER = 1, 
	HIGH_ORDER = 2, 
	NUMA_ORDER = 3, 
	LOW_ORDER = 4, 
	IDLE_ORDER = 5, 
	SPLIT_ORDER_NUM = 6
} SPLIT_ORDER;

static const SPLIT_MAP split_map[SPLIT_MAP_NUM] = 
                        {URGENT_NUMA_MAP, URGENT_MAP, HIGH_MAP, 
                                    NUMA_MAP, LOW_MAP, IDLE_MAP};

__attribute__((always_inline))
static void update_epoch_for_split(
                    struct tls_context *tls, 
                    struct tls_context *self) {
    unsigned long epoch;

    epoch = READ_ONCE(tls->epoch);

    if (epoch != MAX_LONG_INTEGER) {
        epoch += 100;

        if (epoch > self->epoch) {
            release_fence();
            WRITE_ONCE(self->epoch, epoch);
        }
    }
}


static bool is_unurgent_split_enough(
                        int tid_begin, 
                        int tid_end, 
                        double threhold) {
    struct tls_context *tls, *self;
    RECORD_POINTER tail, head, *split_tail, *split_head;
    struct record_page *h_page, *t_page;
    struct eh_split_entry *ent, *tail_ent;
    unsigned long counts;
    int h_ent_num, t_ent_num, t, ret;
    SPLIT_ORDER order;
    SPLIT_MAP map;

    self = tls_context_itself();

    counts = 0;

    for (t = tid_begin; t < tid_end + 1; ++t) {
        tls = ((t != tid_end) ? tls_context_of_tid(t) : self);

        for (order = HIGH_ORDER; order < SPLIT_ORDER_NUM - 1; ++order) {
            map = split_map[order];

            split_head = &tls->split_rp[map];
            split_tail = &tls->split_rp_tail[map];

            head = *split_head;
            tail = READ_ONCE(*split_tail);

            t_page = page_of_record_pointer(tail);
            t_ent_num = ent_num_of_record_pointer(tail);

            h_page = page_of_record_pointer(head);
            h_ent_num = ent_num_of_record_pointer(head);

            tail_ent = &t_page->s_ent[t_ent_num];
            ent = &h_page->s_ent[h_ent_num];

            while (ent != tail_ent) {
                ret = check_eh_split_entry(ent);

                if (unlikely(ret == 1))
                    break;

                if (likely(ret == 0))
                    counts += 1;

                if (h_ent_num != SPLIT_ENT_PER_RECORD_PAGE - 1)
                    h_ent_num += 1;
                else {
                    h_ent_num = 0;
                    h_page = h_page->next;

                    if ((double)counts > threhold)
                        return true;
                }

                ent = &h_page->s_ent[h_ent_num];
            }
        
            if ((double)counts > threhold)
                return true;
        }
    }

    return false;
}

static void update_split_state(
                    int tid_begin, 
                    int tid_end) {
    struct tls_context *tls, *self;
    RECORD_POINTER tail, *split_tail;
    unsigned long urgent_splits, low_splits, total_splits;
    unsigned long splits_1, splits_2, splits_3;
    struct record_page *t_page;
    u64 cur_time, approx_seg, time_diff;
    int g_depth, t_ent_num, t;
    double segs, ratio_split, time_split;
    SPLIT_STATE state;
    SPLIT_ORDER order;
    SPLIT_MAP map;

    urgent_splits = low_splits = total_splits = 0;

    self = tls_context_itself();

    for (t = tid_begin; t < tid_end + 1; ++t) {
        if (t != tid_end) {
            tls = tls_context_of_tid(t);
            order = URGENT_NUMA_ORDER;
        } else {
            tls = self;
            order = URGENT_ORDER;
        }

        for (; order < SPLIT_ORDER_NUM - 1; ++order) {
            map = split_map[order];

            split_tail = &tls->split_rp_tail[map];
            tail = READ_ONCE(*split_tail);

            t_page = page_of_record_pointer(tail);
            t_ent_num = t_page->counter + ent_num_of_record_pointer(tail);

            total_splits += t_ent_num;

            if (order < HIGH_ORDER)
                urgent_splits += t_ent_num;
            else if (order > HIGH_ORDER)
                low_splits += t_ent_num;
        }
    }

    cur_time = sys_time_us();

    time_diff = cur_time - self->sample_time;
    splits_1 = urgent_splits - self->urgent_splits;
    splits_2 = total_splits - self->total_splits;
    splits_3 = low_splits - self->low_splits;

    self->sample_time = cur_time;
    self->urgent_splits = urgent_splits;
    self->low_splits = low_splits;
    self->total_splits = total_splits;

    g_depth = READ_ONCE(per_node_context->global_depth);

    approx_seg = EXP_2(g_depth - 1);
    approx_seg += (approx_seg >> 1);
    segs = approx_seg * SPLIT_HEAVY_RATIO;
    segs /= (node_context.actual_node_num * per_node_context->split_thread_num);

    if (self->urgent_consume != 0) {
        ratio_split = ((double)time_diff * self->urgent_consume) * (segs / (1.0 + segs));
        time_split = (double)time_diff * (self->urgent_consume - 1.0 / SPLIT_WAIT_TIME_LIMIT);

        if (ratio_split > time_split)
            ratio_split = time_split;

        if ((double)splits_1 > ratio_split) {
            state = MANY_SPLITS;
            goto update_worker_split_state;
        }
    }

    if (self->total_consume != 0) {
        ratio_split = ((double)time_diff * self->total_consume) * (segs / (1.0 + segs));
        time_split = (double)time_diff * (self->total_consume - 1.0 / SPLIT_WAIT_TIME_LIMIT);

        if (ratio_split > time_split)
            ratio_split = time_split;

        if ((double)splits_2 > ratio_split) {
            if (splits_1 > ratio_split || 
                    splits_3 > ratio_split || 
                    is_unurgent_split_enough(tid_begin, 
                            tid_end, ratio_split - splits_1)) {
                state = MODERATE_SPLITS;
                goto update_worker_split_state;
            }
        }
    }

    state = FEW_SPLITS;

update_worker_split_state :
    if (self->thread_split_context.state != state) {
        self->thread_split_context.state = state;

        for (t = tid_begin; t < tid_end; ++t) {
            tls = tls_context_of_tid(t);
            WRITE_ONCE(tls->thread_split_context.state, state);
        }
    }
}

__attribute__((always_inline))
static SPLIT_ORDER check_higher_prio_split(
                struct tls_context *tls, 
                SPLIT_ORDER order) {
    RECORD_POINTER tail, head, *split_tail, *split_head;
    struct record_page *h_page, *t_page;
    int h_ent_num, t_ent_num;
    SPLIT_MAP map;
    SPLIT_ORDER check_order;

    for (check_order = URGENT_NUMA_ORDER; check_order < order; ++check_order) {
        map = split_map[check_order];

        split_tail = &tls->split_rp_tail[map];
        split_head = &tls->split_rp[map];

        tail = READ_ONCE(*split_tail);
        head = *split_head;

        t_page = page_of_record_pointer(tail);
        t_ent_num = ent_num_of_record_pointer(tail);

        h_page = page_of_record_pointer(head);
        h_ent_num = ent_num_of_record_pointer(head);

        if (t_ent_num > h_ent_num + SPLIT_RECHECK_LOOP_ENTS || 
            (t_page != h_page && h_page->next != t_page) || 
            (h_page->next == t_page && t_ent_num + SPLIT_ENT_PER_RECORD_PAGE > 
                                            h_ent_num + SPLIT_RECHECK_LOOP_ENTS))
            break;
    }

    return check_order;
}

static int process_split_entry(
                int tid_begin, 
                int tid_end, 
                unsigned long start_time, 
                unsigned long time_limit) {
    struct tls_context *tls, *self;
    RECORD_POINTER tail, head, *split_tail, *split_head;
    struct eh_split_context split_context;
    struct eh_split_entry *ent, *tail_ent;
    struct record_page *h_page, *t_page, *page;
    unsigned long cur_time;
    double consume_rate;
    SPLIT_ORDER order, check_order;
    int h_ent_num, t_ent_num, t, counts, accum, ret;
    bool repeat;
    SPLIT_MAP map;

    self = tls_context_itself();

    counts = 0;
    order = URGENT_NUMA_ORDER;

    while (order < SPLIT_ORDER_NUM) {
        repeat = false;

        for (t = tid_begin; t < tid_end + 1; ++t) {
            tls = ((t != tid_end) ? tls_context_of_tid(t) : self);

            map = split_map[order];

            split_tail = &tls->split_rp_tail[map];
            split_head = &tls->split_rp[map];

            tail = READ_ONCE(*split_tail);
            head = *split_head;
    
            t_page = page_of_record_pointer(tail);
            t_ent_num = ent_num_of_record_pointer(tail);

            h_page = page_of_record_pointer(head);
            h_ent_num = ent_num_of_record_pointer(head);

            tail_ent = &t_page->s_ent[t_ent_num];
            ent = &h_page->s_ent[h_ent_num];

            accum = 0;

            while (ent != tail_ent) {
                ret = analyze_eh_split_entry(ent, &split_context);

                if (unlikely(ret == 1))
                    break;

                if (ret == 0) {
                    if (split_context.type != INCOMPLETE_SPLIT)
                        accum += 1;

                    ret = eh_split(&split_context);

                    if (unlikely(ret == -1)) {
                        //to dooooooooo handle memory lack
                    }
                }

                if (h_ent_num != SPLIT_ENT_PER_RECORD_PAGE - 1)
                    h_ent_num += 1;
                else {
                    h_ent_num = 0;
                    page = h_page->next;
                    reclaim_page(h_page, RECORD_PAGE_SHIFT);
                    h_page = page;

                    counts += accum;

                    if (/*tls != self && */accum > SPLIT_RR_RECORD_ENTS) {
                        repeat = true;
                        break;
                    }
                }

                ent = &h_page->s_ent[h_ent_num];
            }

            *split_head = make_record_pointer(h_page, h_ent_num);

            update_epoch_for_split(tls, self);

            check_order = check_higher_prio_split(tls, order);

            if (unlikely(order != check_order))
                break;
        }

        cur_time = sys_time_us();

        if (time_limit < cur_time || unlikely(order != check_order)) {
            if (counts > SPLIT_CONSUME_COUNTING_THREHOLD) {
                consume_rate = (double)counts / (double)(cur_time - start_time);

                if (order <= URGENT_ORDER)
                    self->urgent_consume = self->urgent_consume == 0 ? 
                                consume_rate : SPLIT_EXP_SMOOTH_CONSUME_RATE(self->urgent_consume, consume_rate);

                if (order <= LOW_ORDER)
                    self->total_consume = self->total_consume == 0 ? 
                                consume_rate : SPLIT_EXP_SMOOTH_CONSUME_RATE(self->total_consume, consume_rate);
            }

            if (time_limit < cur_time)
                return 1;

            counts = 0;
            start_time = cur_time;
            order = check_order;
            continue;
        }

        if (order == URGENT_ORDER && counts > SPLIT_CONSUME_COUNTING_THREHOLD) {
            consume_rate = ((double)counts) / (double)(cur_time - start_time);
            self->urgent_consume = self->urgent_consume == 0 ? consume_rate : 
                        SPLIT_EXP_SMOOTH_CONSUME_RATE(self->urgent_consume, consume_rate);
        } else if (order == LOW_ORDER && counts > SPLIT_CONSUME_COUNTING_THREHOLD) {
            consume_rate = ((double)counts) / (double)(cur_time - start_time);
            self->total_consume = self->total_consume == 0 ? consume_rate : 
                        SPLIT_EXP_SMOOTH_CONSUME_RATE(self->total_consume, consume_rate);
        }


        if (!repeat)
            ++order;        
    }

    return 0;
}


void *split_task(void *parameter) {
    union split_task_parameter task_parameter;
    struct split_task_input input;
    struct per_node_context *per_nc;
    unsigned long time_limit, time;
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
        update_split_state(tid_begin, tid_end);

        time = sys_time_us();
        time_limit = time + BACKGROUND_SPLIT_PERIOD;

        if (process_split_entry(tid_begin, tid_end, time, time_limit))
            continue;

        time = sys_time_us();

        if (time < time_limit)
            sleep_us(time_limit - time);//usleep(time_limit - t);
    }

}

void collect_all_thread_info() {
    struct per_node_context *per_nc;
    struct tls_context *tls, *tls_array;
    RECORD_POINTER rp, *chunk_rp, *page_rp;
    unsigned long ep;
    int t, n, nodes, total_threads;

    nodes = node_context.node_num;

    for (n = 0; n < nodes; ++n) {
        per_nc = &node_context.all_node_context[n];

        total_threads = per_nc->total_thread_num;

        if (total_threads == 0)
            continue;

        tls_array = per_nc->max_tls_context;

        chunk_rp = per_nc->chunk_rp;
        page_rp = per_nc->page_rp;

        for (t = 0; t < total_threads; ++t) {
            tls = &tls_array[t];

            rp = READ_ONCE(tls->page_reclaim_tail);
            WRITE_ONCE(page_rp[t], rp);

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
static int prefault_memory() {
    int t, total_threads, ret;

    ret = 0;

    total_threads = per_node_context->total_thread_num;

    for (t = 0; t < total_threads; ++t)
        if (prefault_eh_seg(t))
            ret = 1;

    return ret;
}

void free_reclaim_area(
            struct tls_context *tls, 
            RECORD_POINTER tail,  
            bool page_type) {
    RECORD_POINTER *head_ptr;
    struct reclaim_entry *ent, *carve;
    struct record_page *h_page, *t_page, *page;
    int h_ent_num, t_ent_num, reclaim_count;

    head_ptr = (page_type ? &tls->page_reclaim : &tls->chunk_reclaim);

    h_page = page_of_record_pointer(*head_ptr);
    h_ent_num = ent_num_of_record_pointer(*head_ptr);

    t_page = page_of_record_pointer(tail);
    t_ent_num = ent_num_of_record_pointer(tail);

    carve = (struct reclaim_entry *)h_page->carve;

    ent = &h_page->r_ent[h_ent_num];
    
    reclaim_count = 0;

    while (ent != carve) {
        if (!page_type)
            free((void *)ent->addr);
        else
            free_page_aligned(page_reclaim_addr(ent), 
                                page_reclaim_size(ent));

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
    struct tls_context *tls;
    RECORD_POINTER rp, *page_rp;
    int t, total_threads;

    total_threads = per_node_context->total_thread_num;

    if (total_threads > 0) {
        page_rp = per_node_context->page_rp;

        for (t = 0; t < total_threads; ++t) {
            tls = tls_context_of_tid(t);
            rp = READ_ONCE(page_rp[t]);
            
            if (likely(rp != INVALID_RECORD_POINTER))
                free_reclaim_area(tls, rp, true);
        }
    }
}

__attribute__((always_inline))
static void free_all_reclaim_chunk() {
    struct tls_context *tls;
    RECORD_POINTER rp, *chunk_rp;
    int t, total_threads;

    total_threads = per_node_context->total_thread_num;

    if (total_threads > 0) {
        chunk_rp = per_node_context->chunk_rp;

        for (t = 0; t < total_threads; ++t) {
            tls = tls_context_of_tid(t);
            rp = READ_ONCE(chunk_rp[t]);
            
            if (likely(rp != INVALID_RECORD_POINTER))
                free_reclaim_area(tls, rp, false);
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
    int repeat;

    node_id = (int)parameter;
    per_node_context = &node_context.all_node_context[node_id];
    tls_context_array = per_node_context->max_tls_context;
    
    if (per_node_context->gc_main) {
        node_context.epoch = 0;
        node_context.max_epoch = 0;
        node_context.min_epoch = MAX_LONG_INTEGER;
    }


    while (1) {
        time_limit = sys_time_us() + BACKGROUND_GC_PERIOD;

        if (per_node_context->gc_main) {
            node_context.min_epoch = MAX_LONG_INTEGER;
            collect_all_thread_info();

            if (unlikely(node_context.min_epoch == MAX_LONG_INTEGER)) {
                sleep_us(BACKGROUND_GC_PERIOD);//usleep(BACKGROUND_GC_PERIOD);
                continue;
            }
        }

        prefault_memory();

        if (per_node_context->gc_main && 
                node_context.epoch < node_context.min_epoch
                                    && check_gc_version() == 0) {
            node_context.epoch = node_context.max_epoch;

            release_fence();
            WRITE_ONCE(node_context.gc_version, node_context.gc_version + 1);
        }

        if (per_node_context->gc_version != READ_ONCE(node_context.gc_version)) {
            free_all_reclaim_page();
            free_all_reclaim_chunk();

            release_fence();
            WRITE_ONCE(per_node_context->gc_version, node_context.gc_version);
        }

        while (1) {
            repeat = prefault_memory();

            t = sys_time_us();

            if (t > time_limit)
                break;

            if (repeat == 0) {
                sleep_us(time_limit - t);//usleep(time_limit - t);
                break;
            }
        }
    }
}
