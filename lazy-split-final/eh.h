#ifndef __EH_H
#define __EH_H

#include "compiler.h"
#include "kv.h"

#include "eh_dir.h"
#include "eh_context.h"
#include "eh_seg.h"
#include "eh_alloc.h"
#include "eh_rehash.h"
#include "eh_record.h"


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

static inline 
int eh_insert_kv(
        KEY_ITEM key, 
        VALUE_ITEM val, 
        u64 hashed_key) {
    struct kv *kv;
    struct eh_dir *dir;
    struct eh_segment *seg;
    struct eh_bucket *bucket;
	EH_DIR_HEADER header;
    struct eh_seg_context seg_context;
    int bucket_id, l_depth, op_node, ret;
    
    dir = get_eh_dir_entry(hashed_key);
    prefech_r0(dir);

    kv = alloc_kv();

    if (unlikely(!kv))
        return -1;

    init_kv(kv, key, val, hashed_key);

    header = READ_ONCE(dir->header);

    seg = eh_dir_low_seg(header);
    l_depth = eh_dir_depth(header);
    op_node = eh_dir_node(header);

    bucket_id = eh_seg_bucket_idx(hashed_key, l_depth);
    bucket = &seg->bucket[bucket_id];
    prefetch_eh_bucket_head(bucket);

    seg_context.cur_seg = seg;
    seg_context.depth = l_depth;
    seg_context.node_id = op_node;

    ret = insert_eh_seg_kv(kv, key, hashed_key, bucket, &seg_context);

    if (ret)
        dealloc_kv(kv);

    return ret;
}

static inline 
int eh_update_kv(
        KEY_ITEM key, 
        VALUE_ITEM val, 
        u64 hashed_key) {
    struct kv *kv;
    struct eh_dir *dir;
    struct eh_segment *seg;
    struct eh_bucket *bucket;
	EH_DIR_HEADER header;
    int bucket_id, l_depth, ret;
    
    dir = get_eh_dir_entry(hashed_key);
    prefech_r0(dir);

    kv = alloc_kv();

    if (unlikely(!kv))
        return -1;

    init_kv(kv, key, val, hashed_key);

    header = READ_ONCE(dir->header);

    seg = eh_dir_low_seg(header);
    l_depth = eh_dir_depth(header);

    bucket_id = eh_seg_bucket_idx(hashed_key, l_depth);
    bucket = &seg->bucket[bucket_id];
    prefetch_eh_bucket_head(bucket);

    ret = update_eh_seg_kv(kv, key, hashed_key, bucket, l_depth);

    if (ret)
        dealloc_kv(kv);

    return ret;
}


static inline 
struct kv *eh_lookup_kv(
        KEY_ITEM key, 
        u64 hashed_key) {
    struct eh_dir *dir;
    struct eh_segment *seg;
    struct eh_bucket *bucket;
	EH_DIR_HEADER header;
    int bucket_id, l_depth;
    
    dir = get_eh_dir_entry(hashed_key);

    header = READ_ONCE(dir->header);

    seg = eh_dir_low_seg(header);
    l_depth = eh_dir_depth(header);

    bucket_id = eh_seg_bucket_idx(hashed_key, l_depth);
    bucket = &seg->bucket[bucket_id];
    prefetch_eh_bucket_head(bucket);

    return lookup_eh_seg_kv(key, hashed_key, bucket, l_depth);
}


static inline 
int eh_delete_kv(
        KEY_ITEM key, 
        u64 hashed_key) {
    struct eh_dir *dir;
    struct eh_segment *seg;
    struct eh_bucket *bucket;
	EH_DIR_HEADER header;
    int bucket_id, l_depth;
    
    dir = get_eh_dir_entry(hashed_key);

    header = READ_ONCE(dir->header);

    seg = eh_dir_low_seg(header);
    l_depth = eh_dir_depth(header);

    bucket_id = eh_seg_bucket_idx(hashed_key, l_depth);
    bucket = &seg->bucket[bucket_id];
    prefetch_eh_bucket_head(bucket);

    return delete_eh_seg_kv(key, hashed_key, bucket, l_depth);
}


int eh_split(struct eh_split_context *split);

/*void *alloc_eh_seg(int num);
void *alloc_other_eh_seg(int num, int nid);*/
struct record_page *alloc_record_page();
struct record_page *alloc_other_record_page(int nid);
int prefault_eh_seg(int tid);

int init_eh_structure(int nodes, int *node_map);
void release_eh_structure();


#ifdef  __cplusplus
}
#endif	//__cplusplus

#endif	//__EH_H
