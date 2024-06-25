#include "dht_init.h"

#include "kv.h"

#include "ext_hash.h"


extern u64 eh_contex;
extern struct tls_context tls_context_array[THREAD_NUM];
extern struct background_context bg_contex;

extern __thread int thread_id;

struct thread_paramater thread_paramater_array[THREAD_NUM];
pthread_t background_pthread;
int active_thread_num = 0;
int cpu_num = 0;


static void *work_thread_function(void *paramater) {
    struct thread_paramater *t_paramater = (struct thread_paramater *)paramater;

    thread_id = t_paramater->tid;
    tls_context = &tls_context_array[thread_id];
    return t_paramater->callback_fuction(t_paramater->paramater);
}

static int init_eh_table() {
    struct eh_dir_entry *dir_array;
    struct eh_segment *seg_array;
    u64 ent1, ent2;
    int all_seg_size, i, j;

    all_seg_size = 3 * (EH_SEGMENT_SIZE << (INITIAL_EH_L_DEPTH - 1));

    dir_array = (struct eh_dir_entry *)
                    malloc_prefault_page_aligned(INITIAL_EH_DIR_SIZE);

    if (unlikely((void *)dir_array == MAP_FAILED))
        return -1;

    seg_array = (struct eh_segment *)malloc_prefault_page_aligned(all_seg_size);

    if (unlikely((void *)seg_array == MAP_FAILED)) {
        free_page_aligned(dir_array, INITIAL_EH_DIR_SIZE);
        return -1;
    }
	
    memset(seg_array, 0, all_seg_size);

    eh_contex = set_eh_dir_depth(dir_array, INITIAL_EH_G_DEPTH);

    for (i = 0; i < (1 << INITIAL_EH_L_DEPTH); ++i) {
        if ((i & 1) == 0)
            ent2 = (uintptr_t)(seg_array++);

        ent1 = (((uintptr_t)(seg_array++)) | INITIAL_EH_L_DEPTH);

        for (j = 0; j < (1 << (INITIAL_EH_G_DEPTH - INITIAL_EH_L_DEPTH)); ++j) {
            dir_array->ent1 = ent1;
            dir_array->ent2 = ent2;
            ++dir_array;
        }
    }

    return 0;
}

static int init_tls_contex() {
    struct tls_context *tls;
    struct reclaim_page *kv_page;
    int i;

    for (i = 0; i < THREAD_NUM; ++i) {
        tls = &tls_context_array[i];
        tls->epoch = 0;
        // to dooooooooooooo tls->count
        tls->rclist_head[PAGE] = tls->rclist_tail[PAGE] = 
		tls->rclist_head[CHUNK] = tls->rclist_tail[CHUNK] = NULL;

        kv_page = (struct reclaim_page *)malloc_prefault_page_aligned(RECLAIM_PAGE_SIZE << 1);

        if (unlikely((void *)kv_page == MAP_FAILED)) {
            tls->rcpage[PAGE] = tls->rcpage[CHUNK] = NULL;

            while (i--) {
                tls = &tls_context_array[i];
                free_page_aligned(tls->rcpage[CHUNK], RECLAIM_PAGE_SIZE << 1);
                tls->rcpage[PAGE] = tls->rcpage[CHUNK] = NULL;
            }
            
            return -1;
        }

        tls->rcpage[PAGE] = tls->rcpage[CHUNK] = kv_page;
    }

    return 0;
}

static void init_background_contex() {
    bg_context.epoch = MAX_LONG_INTEGER;
        //to doooooooooooooo bg->
    bg_context.reclaim[CHUNK].rclist_head = bg_context.reclaim[CHUNK].rclist_tail
                        = bg_context.reclaim[CHUNK].wait_rclist = NULL;
    bg_context.reclaim[PAGE].rclist_head = bg_context.reclaim[PAGE].rclist_tail
                        = bg_context.reclaim[PAGE].wait_rclist = NULL;
}



int dht_init_structure() {
    if (unlikely(init_eh_table()))
        return -1;

    if (unlikely(init_tls_contex())) {
        //to dooooooooooooo free directory and segment memory
        return -1;
    }

    init_background_contex();

    return 0;
}

//to dooooooooooooo assert cpus, threads must be larger than 0
int dht_create_thread(void *(**start_routine)(void *),
                                void **restrict arg, 
                                int threads, int cpus) {
    struct thread_paramater *t_paramater;
    int ret, i, all_cpu, c_num = 0;
    
    if (active_thread_num || threads > THREAD_NUM)
        return -1;

    all_cpu = (threads + 1 > cpus);

    active_thread_num = threads;
    cpu_num = cpus;

    if (all_cpu)
        ret = create_default_thread(&background_pthread,
                    		&background_task, 0);
    else
        ret = create_binding_thread(&background_pthread, 
        			&background_task, (void *)i, cpus - 1);

    if (unlikely(ret)) {
        terminate_the_thread(background_pthread);
        return ret;
    }
    


    for (i = 0; i < threads; ++i) {
        t_paramater = &thread_paramater_array[i];
        t_paramater->tid = i;
        t_paramater->cpu_id = c_num;
        t_paramater->callback_fuction = start_routine[i];
        t_paramater->paramater = arg[i];

        ret = create_binding_thread(&t_paramater->pthread_id, &work_thread_function, t_paramater, c_num);

        if (unlikely(ret)) {
            while (i--)
                terminate_the_thread(thread_paramater_array[i].pthread_id);

            return ret;
        }

        if (++c_num == cpus)
            c_num = 0;
    }

    return 0;
}

//to dooooo assert threads must be larger than 0
//retrun success-added thread num
int dht_add_thread(void *(**start_routine)(void *),
                    void **restrict arg, int threads) {
    struct thread_paramater *t_paramater;
    int ret, i, c_num, free_cpu;

    if (active_thread_num + threads > THREAD_NUM)
        return 0;    

    free_cpu = cpu_num - (active_thread_num + 1);

    c_num = active_thread_num % cpu_num;

    if (free_cpu < threads)
        unbind_thread(background_pthread, cpu_num);
    
    for (i = 0; i < threads; ++i) {
        t_paramater = &thread_paramater_array[active_thread_num];
        t_paramater->tid = active_thread_num;
        t_paramater->cpu_id = c_num;
        t_paramater->callback_fuction = start_routine[i];
        t_paramater->paramater = arg[i];

        ret = create_binding_thread(&t_paramater->pthread_id, &work_thread_function, t_paramater, c_num);

        if (unlikely(ret))
            return i;

        ++active_thread_num;

        if (++c_num == cpu_num)
            c_num = 0;
    }

    return threads;
}
