#ifndef __BACKGROUND_H
#define __BACKGROUND_H


#include "compiler.h"
#include "per_thread.h"


#define BACKGROUND_SPLIT_PERIOD   1000
#define BACKGROUND_GC_PERIOD    2000

#ifdef  __cplusplus
extern  "C" {
#endif

extern struct node_context node_context;
extern __thread struct tls_context *tls_context_array;
extern __thread int node_id;

struct split_task_input {
    int nid;
    int tid;
};

union split_task_parameter {
    void *parameter;
    struct split_task_input input;
};


void *split_task(void *parameter);
void *gc_task(void *parameter);

#ifdef  __cplusplus
}
#endif  //__cplusplus

#endif  //__BACKGROUND_H
