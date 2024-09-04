#ifndef __EH_REHASH_H
#define __EH_REHASH_H


#include "compiler.h"
#include "kv.h"

#include "eh_dir.h"
#include "eh_seg.h"
#include "eh_context.h"


#ifdef  __cplusplus
extern  "C" {
#endif


#define INVALID_EH_SPLIT_TARGET    0UL


#define EH_SPLIT_TARGET_DEPTH_BIT_START	0
#define EH_SPLIT_TARGET_DEPTH_BIT_END	(EH_DEPTH_BITS - 1)

#define EH_SPLIT_TARGET_RECHECK_BIT    (EH_SPLIT_TARGET_DEPTH_BIT_END + 1)
#define EH_SPLIT_TARGET_INCOMPLETE_BIT    (EH_SPLIT_TARGET_DEPTH_BIT_END + 2)

#define EH_SPLIT_TARGET_PREFIX_19_BIT	(PAGE_SHIFT - 1)
#define EH_SPLIT_TARGET_PREFIX_16_BIT	(EH_SPLIT_TARGET_PREFIX_19_BIT - 3)

#define EH_SPLIT_TARGET_SEG_BIT_START	PAGE_SHIFT
#define EH_SPLIT_TARGET_SEG_BIT_END	(VALID_POINTER_BITS - 1)

#define EH_SPLIT_TARGET_PREFIX_48_BIT	VALID_POINTER_BITS
#define EH_SPLIT_TARGET_PREFIX_63_BIT	(EH_SPLIT_TARGET_PREFIX_48_BIT + 15)


#define EH_SPLIT_DEST_PREFIX_20_BIT	0
#define EH_SPLIT_DEST_PREFIX_31_BIT	(PAGE_SHIFT - 1)

#define EH_SPLIT_DEST_SEG_BIT_START	PAGE_SHIFT
#define EH_SPLIT_DEST_SEG_BIT_END	(VALID_POINTER_BITS - 1)

#define EH_SPLIT_DEST_PREFIX_32_BIT	VALID_POINTER_BITS
#define EH_SPLIT_DEST_PREFIX_47_BIT	(EH_SPLIT_DEST_PREFIX_32_BIT + 15)


#define eh_split_target_need_recheck(target)	\
			(!!((target) & SHIFT(EH_SPLIT_TARGET_RECHECK_BIT)))
#define eh_split_incomplete_target(target)	\
			(!!((target) & SHIFT(EH_SPLIT_TARGET_INCOMPLETE_BIT)))
#define eh_split_target_depth(target)   ((target) &	\
			INTERVAL(EH_SPLIT_TARGET_DEPTH_BIT_START, EH_SPLIT_TARGET_DEPTH_BIT_END))
#define eh_split_target_seg(target)	((target) &	\
			INTERVAL(EH_SPLIT_TARGET_SEG_BIT_START, EH_SPLIT_TARGET_SEG_BIT_END))
#define eh_split_dest_seg(dest)	((dest) &	\
			INTERVAL(EH_SPLIT_DEST_SEG_BIT_START, EH_SPLIT_DEST_SEG_BIT_END))

#define eh_split_range_prefix(ent, bit1, bit2, td)	\
	((((ent) & INTERVAL(EH_SPLIT_##td##_PREFIX_##bit1##_BIT, EH_SPLIT_##td##_PREFIX_##bit2##_BIT))	\
		>> EH_SPLIT_##td##_PREFIX_##bit1##_BIT) << bit1)

#define shift_prefix_for_eh_split(hashed_key, bit1, bit2, td)	\
	((PREHASH_KEY(hashed_key, bit1, bit2) >> bit1) << EH_SPLIT_##td##_PREFIX_##bit1##_BIT)



#define eh_split_prefix(target, dest, depth)	\
				((eh_split_range_prefix(target, 48, 63, TARGET) |	\
					eh_split_range_prefix(dest, 32, 47, DEST) |	\
					eh_split_range_prefix(dest, 20, 31, DEST) |	\
					eh_split_range_prefix(target, 16, 19, TARGET))	\
					& MASK(PREHASH_KEY_BITS - depth))

#define make_eh_split_target_entry(target, hashed_key, depth, recheck, incomplete)    \
                    (((uintptr_t)(target)) |	\
						shift_prefix_for_eh_split(hashed_key, 48, 63, TARGET) |	\
						shift_prefix_for_eh_split(hashed_key, 16, 19, TARGET)	|	\
                    	SHIFT_OF(recheck, EH_SPLIT_TARGET_RECHECK_BIT) |	\
						SHIFT_OF(incomplete, EH_SPLIT_TARGET_INCOMPLETE_BIT) |	\
						SHIFT_OF(depth, EH_SPLIT_TARGET_DEPTH_BIT_START))

#define make_eh_split_dest_entry(dest, hashed_key) (((uintptr_t)(dest)) \
                    | shift_prefix_for_eh_split(hashed_key, 32, 47, DEST)  \
                    | shift_prefix_for_eh_split(hashed_key, 20, 31, DEST))


//target [63~48:hashed_key_63_48 47~12:rehashed_segment 11~8:hashed_key_19~16 7~6:flags 5~0:depth]
//destination [63~48:hashed_key_47_32 47~12:new_four_segment 11~0:hashed_key_31~20]

struct eh_split_entry {
    uintptr_t target;
    uintptr_t destination;
};

struct eh_split_context {
	struct eh_segment *target_seg;
	struct eh_four_segment *dest_seg;
	u64 hashed_key;
	int depth;
	short recheck;
	short incomplete;
};


static inline void init_eh_split_entry(
                    struct eh_split_entry *ent,
					struct eh_split_context *split) {
    ent->target = make_eh_split_target_entry(
							split->target_seg, split->hashed_key, 
							split->depth, split->recheck, split->incomplete);
    ent->destination = make_eh_split_dest_entry(
								split->dest_seg, split->hashed_key);
}

struct eh_two_segment *add_eh_new_segment(
				struct eh_split_context *split,
				struct eh_two_segment *seg,
				struct eh_bucket *bucket,
				struct kv *kv, int high_prio);

int eh_split(struct eh_split_entry *split_ent, int high_prio);


#ifdef  __cplusplus
}
#endif	//__cplusplus

#endif	//__EH_REHASH_H
