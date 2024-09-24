#include "dht_init.h"
#include "kv.h"

#include "eh.h"


struct thread_paramater thread_paramater_array[THREAD_NUM];
pthread_t background_pthread[BACKGROUNG_THREAD_NUM];
int active_thread_num = 0;
int cpu_num = 0;


static void *work_thread_function(void *paramater) {
    struct thread_paramater *t_paramater = (struct thread_paramater *)paramater;

    init_tls_context(t_paramater->tid);
    return t_paramater->callback_fuction(t_paramater->paramater);
}


int dht_init_structure() {

    if (unlikely(init_eh_structure()))
        return -1;

    if (unlikely(prepare_all_tls_context())) {
        //to dooooooooooooo free directory and segment memory
        return -1;
    }

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

    all_cpu = (threads + BACKGROUNG_THREAD_NUM > cpus);

    active_thread_num = threads;
    cpu_num = cpus;

    for (i = 0; i < BACKGROUNG_THREAD_NUM; ++i) {
        if (all_cpu)
            ret = create_default_thread(&background_pthread[i],
                    		        &background_task, (void *)i);
        else
            ret = create_binding_thread(&background_pthread[i], 
                            &background_task, (void *)i, cpus - 1 - i);

        if (unlikely(ret)) {
            while (i--)
                terminate_the_thread(background_pthread[i]);

            return ret;
        }

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

    free_cpu = cpu_num - (active_thread_num + BACKGROUNG_THREAD_NUM);

    c_num = active_thread_num % cpu_num;

    if (free_cpu < threads)
        for (i = 0; i < BACKGROUNG_THREAD_NUM; ++i)
            unbind_thread(background_pthread[i], cpu_num);
    
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
