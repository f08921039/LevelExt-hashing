#ifndef __EH_H
#define __EH_H

#include "compiler.h"
#include "kv.h"

#include "eh_dir.h"
#include "eh_context.h"
#include "eh_seg.h"
#include "eh_alloc.h"
#include "eh_rehash.h"


#ifdef  __cplusplus
extern  "C" {
#endif


/*static inline int eh_put_kv(struct kv *kv, u64 hashed_key) {
    EH_CONTEXT *contex = get_eh_context(hashed_key);
    EH_CONTEXT c_val = READ_ONCE(*contex);
    struct eh_dir *dir = head_of_eh_dir(c_val);
    int g_depth = eh_depth(c_val);
    
    return put_eh_dir_kv(dir, kv, hashed_key, g_depth);
}

static inline int eh_get_kv(struct kv *kv, u64 hashed_key) {
    EH_CONTEXT *contex = get_eh_context(hashed_key);
    EH_CONTEXT c_val = READ_ONCE(*contex);
    struct eh_dir *dir = head_of_eh_dir(c_val);
    int g_depth = eh_depth(c_val);
    
    return get_eh_dir_kv(dir, kv, hashed_key, g_depth);
}*/

static inline int eh_put_kv(struct kv *kv, u64 hashed_key) {
    return eh_dir_kv_operation(kv, hashed_key, put_eh_seg_kv);
}

static inline int eh_get_kv(struct kv *kv, u64 hashed_key) {
    return eh_dir_kv_operation(kv, hashed_key, get_eh_seg_kv);
}

static inline int eh_delete_kv(struct kv *kv, u64 hashed_key) {
    return eh_dir_kv_operation(kv, hashed_key, delete_eh_seg_kv);
}


int eh_split(struct eh_split_entry *split_ent, int high_prio);

void prefault_eh_seg(int tid);

int init_eh_structure(int nodes);
void release_eh_structure();


#ifdef  __cplusplus
}
#endif	//__cplusplus

#endif	//__EH_H
