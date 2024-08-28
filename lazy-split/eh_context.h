#ifndef __EXT_CONTEXT_H
#define __EXT_CONTEXT_H

#include "compiler.h"
#include "kv.h"

typedef u64 EH_CONTEXT;


#define EH_DEPTH_BITS	6
#define EH_DEPTH_MAX	(~MASK(EH_DEPTH_BITS))

#define EH_GROUP_BITS	3
#define EH_GROUP_NUM	SHIFT(EH_GROUP_BITS)


#ifdef  __cplusplus
extern  "C" {
#endif

//#define EH_GROUP_MASK	MASK(EH_KV_ITEM_BIT - EH_GROUP_BIT)
#define eh_group_index(hashed_key)	((hashed_key) >> (PREHASH_KEY_BITS - EH_GROUP_BITS))


/*EH_DIR_CONTEX [63~48:flag 47~8:dir_addr 5~0:g_depth]*/

#define EH_CONTEXT_DEPTH_START_BIT    0
#define EH_CONTEXT_DEPTH_END_BIT    (EH_DEPTH_BITS - 1)

#define EH_CONTEXT_DIR_START_BIT  (EH_CONTEXT_DEPTH_END_BIT + 1)
#define EH_CONTEXT_DIR_END_BIT  (VALID_POINTER_BITS - 1)

#define EH_CONTEXT_UPDATE_BIT   VALID_POINTER_BITS

#define eh_depth(contex)	(((contex) &    \
        INTERVAL(EH_CONTEXT_DEPTH_START_BIT, EH_CONTEXT_DEPTH_END_BIT)) \
                                        >> EH_CONTEXT_DEPTH_START_BIT)

#define extract_eh_dir(contex)	((contex) & \
            INTERVAL(EH_CONTEXT_DIR_START_BIT, EH_CONTEXT_DIR_END_BIT))
#define eh_context_updating(context)    \
                            ((context) & SHIFT(EH_CONTEXT_UPDATE_BIT))
#define set_eh_dir_context(dir_addr, depth)	(((uintptr_t)(dir_addr)) |  \
                                SHIFT_OF(depth, EH_CONTEXT_DEPTH_START_BIT))
#define set_eh_context_updating(context)    \
                                ((context) | SHIFT(EH_CONTEXT_UPDATE_BIT))


extern EH_CONTEXT eh_group[EH_GROUP_NUM] __attribute__ ((aligned (CACHE_LINE_SIZE)));


static inline EH_CONTEXT *get_eh_context(u64 hashed_key) {
    return &eh_group[eh_group_index(hashed_key)];
}

int init_eh_structure();


#ifdef  __cplusplus
}
#endif	//__cplusplus

#endif	//__EXT_CONTEXT_H
