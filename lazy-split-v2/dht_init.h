#ifndef __DHT_INIT_H
#define __DHT_INIT_H

#include "compiler.h"
#include "per_thread.h"
#include "background.h"


#ifdef  __cplusplus
extern  "C" {
#endif


struct thread_paramater {
    int tid;
    int cpu_id;
    void *(*callback_fuction)(void *);
    union {
        pthread_t pthread_id;
        u64 padding;
    };
    void *paramater;
};


extern struct thread_paramater thread_paramater_array[THREAD_NUM];
extern pthread_t background_pthread[BACKGROUNG_THREAD_NUM];
extern int active_thread_num;
extern int cpu_num;

static inline pthread_t get_pthread_id(int tid) {
    return thread_paramater_array[tid].pthread_id;
}

static inline int get_cpuid(int tid) {
    return thread_paramater_array[tid].cpu_id;
}

static inline int get_active_thread_num() {
    return active_thread_num;
}

static inline int get_cpu_num() {
    return cpu_num;
}


int dht_init_structure();

//to dooooooooooooo assert cpus, threads must be larger than 0
int dht_create_thread(void *(**start_routine)(void *),
                                void **restrict arg, 
                                int threads, int cpus);

//to dooooo assert threads must be larger than 0
//retrun success-added thread num
int dht_add_thread(void *(**start_routine)(void *),
                    void **restrict arg, int threads);


#ifdef  __cplusplus
}
#endif  //__cplusplus

#endif  //__DHT_INIT_H
