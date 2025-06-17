#ifndef __EXT_CONTEXT_H
#define __EXT_CONTEXT_H

#include "compiler.h"
#include "kv.h"

typedef u64 EH_CONTEXT;


#define EH_DEPTH_BITS	6
#define EH_DEPTH_MAX	(~MASK(EH_DEPTH_BITS))

#define EH_GROUP_BITS	3
#define EH_GROUP_NUM	SHIFT(EH_GROUP_BITS)



#define INITIAL_EH_DIR_PAGE_BITS_PER_GROUP  0
#define INITIAL_EH_SEG_BITS_PER_GROUP   1

#define INITIAL_EH_DIR_PAGE_BITS   \
            (EH_GROUP_BITS + INITIAL_EH_DIR_PAGE_BITS_PER_GROUP)
#define INITIAL_EH_SEG_BITS    (EH_GROUP_BITS + INITIAL_EH_SEG_BITS_PER_GROUP)


#define INITIAL_EH_G_DEPTH  (INITIAL_EH_DIR_PAGE_BITS + PAGE_SHIFT - EH_DIR_HEADER_SIZE_BITS)

#define INITIAL_EH_DIR_ENT_PER_GROUP    EXP_2(INITIAL_EH_G_DEPTH - EH_GROUP_BITS)


#ifdef  __cplusplus
extern  "C" {
#endif


/*EH_DIR_CONTEX [63~48:flag 47~8:dir_addr 5~0:g_depth]*/

#define EH_CONTEXT_DEPTH_START_BIT    0
#define EH_CONTEXT_DEPTH_END_BIT    (EH_DEPTH_BITS - 1)

#define EH_CONTEXT_DIR_START_BIT  (EH_CONTEXT_DEPTH_END_BIT + 1)
#define EH_CONTEXT_DIR_END_BIT  (VALID_POINTER_BITS - 1)

#define EH_CONTEXT_UPDATE_BIT   VALID_POINTER_BITS

#define EH_CONTEXT_DIR_MASK INTERVAL(EH_CONTEXT_DIR_START_BIT, EH_CONTEXT_DIR_END_BIT)


extern EH_CONTEXT eh_group[EH_GROUP_NUM] __attribute__ ((aligned (CACHE_LINE_SIZE)));

struct eh_dir;

static inline
unsigned long eh_group_index(u64 hashed_key) {
    return hashed_key >> (PREHASH_KEY_BITS - EH_GROUP_BITS);
}

static inline 
EH_CONTEXT *get_eh_context(u64 hashed_key) {
    return &eh_group[eh_group_index(hashed_key)];
}

static inline 
int eh_depth(EH_CONTEXT context) {
    return INTERVAL_OF(context, 
            EH_CONTEXT_DEPTH_START_BIT, EH_CONTEXT_DEPTH_END_BIT);
}

static inline 
int get_eh_depth(u64 hashed_key) {
	EH_CONTEXT c_val, *contex;

	contex = get_eh_context(hashed_key);
	c_val = READ_ONCE(*contex);

	return eh_depth(c_val);
}

static inline 
struct eh_dir *extract_eh_dir(EH_CONTEXT context) {
    return (struct eh_dir *)(context & EH_CONTEXT_DIR_MASK);
}

static inline 
bool eh_context_updating(EH_CONTEXT context) {
    return !!(context & SHIFT(EH_CONTEXT_UPDATE_BIT));
}

static inline 
EH_CONTEXT set_eh_dir_context(struct eh_dir *dir, int depth) {
    return ((uintptr_t)dir) | 
                SHIFT_OF(depth, EH_CONTEXT_DEPTH_START_BIT);
}

static inline 
EH_CONTEXT set_eh_context_updating(EH_CONTEXT context) {
    return context | SHIFT(EH_CONTEXT_UPDATE_BIT);
}


int init_eh_structure(int nodes, int *node_map);
void release_eh_structure();

#ifdef  __cplusplus
}
#endif	//__cplusplus

#endif	//__EXT_CONTEXT_H
