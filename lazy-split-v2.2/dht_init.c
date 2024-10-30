#include "dht.h"
#include "kv.h"

#include "per_thread.h"
#include "background.h"

#include "eh.h"

#define NODE_LIMIT  2048


extern struct node_context node_context;

static volatile int thread_waiting;
static volatile int thread_ready;

static void *dht_work_thread(void *paramater) {
    struct thread_paramater *t_paramater = (struct thread_paramater *)paramater;

    init_tls_context(t_paramater->node_id, t_paramater->thread_id);

    atomic_add(&thread_ready, 1);

    while (thread_waiting != thread_ready) {}

    return t_paramater->callback_fuction(t_paramater->paramater);
}

void dht_release_structure() {
    struct per_node_context *per_nc;
    size_t size;
    int i, nodes;

    nodes = node_context.node_num;
    per_nc = node_context.all_node_context;

    if (per_nc == NULL)
        return;

    for (i = 0; i < nodes; ++i) {
        if (per_nc[i].max_tls_context) {
            size = PAGE_MASK & (PAGE_SIZE - 1 + 
                    per_nc[i].total_thread_num * sizeof(struct tls_context));
            free_node_page(per_nc[i].max_tls_context, size);
        }
        
        if (per_nc[i].thread_paramater)
            free(per_nc[i].thread_paramater);

        if (per_nc[i].chunk_rp)
            free(per_nc[i].chunk_rp);

        if (per_nc[i].page_rp)
            free(per_nc[i].page_rp);
    }

    free(per_nc);
}


int dht_init_structure(struct dht_node_context *nc) {
    struct per_node_context *per_nc;
    int nodes = nc->nodes;
    int *max_node_thread = nc->max_node_thread;
    size_t size;
    int i, t, st;

    if (nodes > NODE_LIMIT || nodes > max_node_num() || nodes == 0)
        return -1;

    size = nodes * sizeof(struct per_node_context);
    if (unlikely(malloc_aligned(&per_nc, CACHE_LINE_SIZE, size) != 0))
        return -1;

    node_context.node_num = nodes;
    node_context.gc_version = 0;
    node_context.all_node_context = per_nc;

    for (i = 0; i < nodes; ++i) {
        per_nc[i].max_work_thread_num = 0;
        per_nc[i].work_thread_num = 0;
        per_nc[i].split_thread_num = 0;
        per_nc[i].total_thread_num = 0;
        per_nc[i].gc_enable = 0;
        per_nc[i].gc_main = 0;
        per_nc[i].gc_version = 0;
        per_nc[i].max_tls_context = NULL;
        per_nc[i].thread_paramater = NULL;
        per_nc[i].chunk_rp = per_nc[i].page_rp = NULL;
    }

    thread_waiting = thread_ready = 0;

    //to doooooooooooooo: find the most appropraite nodes

    for (i = 0; i < nodes; ++i) {
        t = max_node_thread[i];

        if (t == 0)
            continue;

        per_nc[i].max_work_thread_num = t;

        st = t / THREADS_PER_SPLIT_THREAD;
        st += (t % THREADS_PER_SPLIT_THREAD != 0);

        size = t * sizeof(struct thread_paramater);
        per_nc[i].thread_paramater = (struct thread_paramater *)malloc(size);

        if (unlikely(per_nc[i].thread_paramater == NULL))
            goto dht_init_structure_failed;

        t += st;
        per_nc[i].total_thread_num = t;

        size = (t * sizeof(struct tls_context) + PAGE_SIZE - 1) & PAGE_MASK;
        per_nc[i].max_tls_context = (struct tls_context *)
                                            alloc_node_page(size, i);

        if (unlikely(per_nc[i].max_tls_context == NULL))
            goto dht_init_structure_failed;

        size = t * sizeof(RECORD_POINTER);
        per_nc[i].chunk_rp = (RECORD_POINTER *)malloc(size);

        if (unlikely(per_nc[i].chunk_rp == NULL))
            goto dht_init_structure_failed;

        size = st * sizeof(RECORD_POINTER);
        per_nc[i].page_rp = (RECORD_POINTER *)malloc(size);

        if (unlikely(per_nc[i].page_rp == NULL))
            goto dht_init_structure_failed;
    }

    if (unlikely(init_eh_structure(nodes)))
        goto dht_init_structure_failed;

    if (unlikely(prepare_all_tls_context())) {
        release_eh_structure();
        goto dht_init_structure_failed;
    }

    return 0;

dht_init_structure_failed :
    dht_release_structure();
    return -1;
}

void dht_terminate_thread() {
    struct per_node_context *per_nc;
    struct thread_paramater *t_paramater;
    int nodes, mt, st, wt, total_thread, cpus, t, n;

    nodes = node_context.node_num;

    for (n = 0; n < nodes; ++n) {
        per_nc = &node_context.all_node_context[n];

        total_thread = per_nc->total_thread_num;

        if (total_thread == 0)
            continue;

        if (per_nc->gc_enable)
            terminate_the_thread(per_nc->gc_pthread);

        mt = per_nc->max_work_thread_num;
        st = per_nc->split_thread_num;
        wt = per_nc->work_thread_num;

        for (t = 0; t < st; ++t) {
            t_paramater = &per_nc->thread_paramater[t * THREADS_PER_SPLIT_THREAD];
            terminate_the_thread(t_paramater->split_pthread_id);
        }

        for (t = 0; t < wt; ++t) {
            t_paramater = &per_nc->thread_paramater[t];
            terminate_the_thread(t_paramater->work_pthread_id);
        }
    }
}

int dht_create_thread(struct dht_node_context *nc) {
    BITMASK *cpu_bitmask;
    int *cpu_array;
    struct per_node_context *per_nc;
    struct thread_paramater *t_paramater;
    pthread_t split_pthread_id;
    int *node_threads = nc->node_thread;
    struct dht_work_function **work_func = nc->node_func;
    union split_task_parameter split_parameter;
    int gc_main, nodes, mt, st, total_thread, cpus, t, t1, t2, w, n, ret;

    cpu_array = alloc_cpu_array();

    if (unlikely(cpu_array == NULL))
        goto dht_create_thread_failed;

    cpu_bitmask = alloc_cpu_bitmask();

    if (unlikely(cpu_bitmask == NULL)) {
        free_cpu_array(cpu_array);
        goto dht_create_thread_failed;
    }

    nodes = node_context.node_num;
    gc_main = 0;

    for (n = 0; n < nodes; ++n) {
        per_nc = &node_context.all_node_context[n];

        if (node_threads[n] > per_nc->max_work_thread_num)
            goto dht_create_thread_failed_front;

        atomic_add(&thread_waiting, node_threads[n]);
    }

    for (n = 0; n < nodes; ++n) {
        per_nc = &node_context.all_node_context[n];

        total_thread = per_nc->total_thread_num;

        if (total_thread == 0)
            continue;

        //clear_bitmask(cpu_bitmask);
        get_node_cpuid(n, cpu_bitmask);
        cpus = cpuid_to_array(cpu_bitmask, cpu_array);

        if (unlikely(cpus == 0))
            goto dht_create_thread_failed_front;

        if (gc_main == 0) {
            gc_main = 1;
            per_nc->gc_main = 1;
        }

        ret = create_grouping_thread(&per_nc->gc_pthread, 
                            &gc_task, (void *)n, cpu_bitmask);

        if (unlikely(ret))
            goto dht_create_thread_failed_front;

        per_nc->gc_enable = 1;

        mt = per_nc->max_work_thread_num;
        st = total_thread - mt;

        split_parameter.input.nid = n;

        for (t = 0; t < st; ++t) {
            split_parameter.input.tid = t;

            ret = create_grouping_thread(&split_pthread_id, &split_task, 
                                    split_parameter.parameter, cpu_bitmask);

            if (unlikely(ret))
                goto dht_create_thread_failed_front;

            per_nc->split_thread_num += 1;

            t1 = t * THREADS_PER_SPLIT_THREAD;
            t2 = (t == st - 1) ? mt : t1 + THREADS_PER_SPLIT_THREAD;

            for (w = t1; w < t2; ++w) {
                t_paramater = &per_nc->thread_paramater[w];

                t_paramater->node_id = n;
                t_paramater->thread_id = w;
                t_paramater->split_pthread_id = split_pthread_id;

                if (w < node_threads[n]) {
                    t_paramater->callback_fuction = 
                                work_func[n][w].start_routine;
                    t_paramater->paramater = work_func[n][w].arg;

                    ret = create_binding_thread(
                            &t_paramater->work_pthread_id, &dht_work_thread, 
                                            t_paramater, cpu_array[w % cpus]);

                    if (unlikely(ret))
                        goto dht_create_thread_failed_front;

                    per_nc->work_thread_num += 1;
                }
            }
        }
    }

    free_cpu_array(cpu_array);
    free_cpu_bitmask(cpu_bitmask);

    return 0;

dht_create_thread_failed_front :
    dht_terminate_thread();
    free_cpu_array(cpu_array);
    free_cpu_bitmask(cpu_bitmask);

dht_create_thread_failed :
    release_eh_structure();
    release_all_tls_context();
    dht_release_structure();
    return -1;
}

int dht_add_thread(
            int node_id,
            struct dht_work_function *func) {
    BITMASK *cpu_bitmask;
    int *cpu_array;
    struct per_node_context *per_nc;
    struct thread_paramater *t_paramater;
    int w, cpus, ret;

    per_nc = &node_context.all_node_context[node_id];

    w = per_nc->work_thread_num;

    if (per_nc->max_work_thread_num == w)
        return -1;

    cpu_array = alloc_cpu_array();

    if (unlikely(cpu_array == NULL))
        return -1;

    cpu_bitmask = alloc_cpu_bitmask();

    if (unlikely(cpu_bitmask == NULL)) {
        free_cpu_array(cpu_array);
        return -1;
    }

    get_node_cpuid(node_id, cpu_bitmask);
    cpus = cpuid_to_array(cpu_bitmask, cpu_array);

    t_paramater = &per_nc->thread_paramater[w];

    t_paramater->callback_fuction = func->start_routine;
    t_paramater->paramater = func->arg;

    atomic_add(&thread_waiting, 1);

    ret = create_binding_thread(&t_paramater->work_pthread_id, 
                    &dht_work_thread, t_paramater, cpu_array[w % cpus]);

    if (unlikely(ret)) {
        atomic_sub(&thread_waiting, 1);
        dht_terminate_thread();
        free_cpu_array(cpu_array);
        free_cpu_bitmask(cpu_bitmask);
        return -1;
    }

    per_nc->work_thread_num = w + 1;

    free_cpu_array(cpu_array);
    free_cpu_bitmask(cpu_bitmask);

    return 0;
}
