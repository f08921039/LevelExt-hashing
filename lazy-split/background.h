#ifndef __BACKGROUND_H
#define __BACKGROUND_H


#include "compiler.h"
#include "per_thread.h"


#define PER_THREAD_OF_BACKGROUND    (THREAD_NUM / BACKGROUNG_THREAD_NUM)

#define BACKGROUND_PERIOD   1000

#ifdef  __cplusplus
extern  "C" {
#endif

extern __thread int thread_id;
extern __thread struct tls_context *tls_context;
extern struct tls_context tls_context_array[THREAD_NUM + BACKGROUNG_THREAD_NUM];

void free_reclaim_memory();
void *background_task(void *id);

#ifdef  __cplusplus
}
#endif  //__cplusplus

#endif  //__BACKGROUND_H
