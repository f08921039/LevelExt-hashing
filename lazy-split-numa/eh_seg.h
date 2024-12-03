#ifndef __EH_SEG_H
#define __EH_SEG_H

//#define LARGE_EH_SEGMENT	1
//#define WIDE_EH_BUCKET_INDEX_BIT	1

#include "compiler.h"
#include "kv.h"

#include "eh_context.h"


#define EH_SEGMENT_SIZE	32768
#define EH_SEGMENT_SIZE_BITS	15


#define EH_BUCKET_INDEX_BIT	7



#define EH_TWO_SEGMENT_SIZE	MUL_2(EH_SEGMENT_SIZE, 1)
#define EH_TWO_SEGMENT_SIZE_BITS	(EH_SEGMENT_SIZE_BITS + 1)

#define EH_FOUR_SEGMENT_SIZE	MUL_2(EH_SEGMENT_SIZE, 2)
#define EH_FOUR_SEGMENT_SIZE_BITS	(EH_SEGMENT_SIZE_BITS + 2)
//#define EH_FOUR_SEGMENT_MASK	MASK(EH_FOUR_SEGMENT_SIZE_BIT)

typedef u64 EH_BUCKET_SLOT;

typedef EH_BUCKET_SLOT EH_BUCKET_HEADER;


#define EH_BUCKET_SLOT_SIZE	sizeof(EH_BUCKET_SLOT)
#define EH_BUCKET_SLOT_BIT	MUL_2(EH_BUCKET_SLOT_SIZE, 3)
#define EH_BUCKET_HEADER_SIZE	sizeof(EH_BUCKET_HEADER)

#define EH_BUCKET_NUM	MUL_2(1, EH_BUCKET_INDEX_BIT)
#define EH_BUCKET_SIZE DIV_2(EH_SEGMENT_SIZE, EH_BUCKET_INDEX_BIT)
#define EH_BUCKET_SIZE_BIT	(EH_SEGMENT_SIZE_BITS - EH_BUCKET_INDEX_BIT)


#define EH_SLOT_NUM_PER_BUCKET	((EH_BUCKET_SIZE - EH_BUCKET_HEADER_SIZE) / EH_BUCKET_SLOT_SIZE)
#define EH_PER_BUCKET_CACHELINE	DIV_2(EH_BUCKET_SIZE, CACHE_LINE_SHIFT)
#define EH_SLOT_NUM_PER_CACHELINE	(CACHE_LINE_SIZE / EH_BUCKET_SLOT_SIZE)



#ifdef  __cplusplus
extern  "C" {
#endif

#define EH_DEFAULT_ADV_SPLIT(nodes)	MAX(0, EH_SLOT_NUM_PER_BUCKET - MUL_2(nodes, 1) + 1)

#define eh_seg_bucket_idx(hashed_key, depth)	\
		(SHIFT_OF(hashed_key, depth) >> (PREHASH_KEY_BITS - EH_BUCKET_INDEX_BIT))
#define eh_seg2_bucket_idx(hashed_key, depth)	\
		(SHIFT_OF(hashed_key, depth) >> (PREHASH_KEY_BITS - (EH_BUCKET_INDEX_BIT + 1)))
#define eh_seg4_bucket_idx(hashed_key, depth)	\
		(SHIFT_OF(hashed_key, depth) >> (PREHASH_KEY_BITS - (EH_BUCKET_INDEX_BIT + 2)))

#define eh_seg_id_in_seg2(hashed_key, depth)	(SHIFT_OF(hashed_key, depth) >> (PREHASH_KEY_BITS - 1))
#define eh_seg2_id_in_seg4(hashed_key, depth)	eh_seg_id_in_seg2(hashed_key, depth)



#define EH_LOW_SEG_BIT	0
#define EH_BUCKET_SPLIT_BIT	1
//#define EH_BUCKET_MIGRATING_BIT	2
#define EH_BUCKET_STAY_BIT	2
#define EH_EXTRA_SEG_BIT	3

#define EH_NEXT_SEG_BIT_START	4
#define EH_NEXT_SEG_BIT_END	(VALID_POINTER_BITS - 1)

#define INITIAL_EH_BUCKET_HEADER	0UL
#define INITIAL_EH_BUCKET_HP_HEADER	MASK(VALID_POINTER_BITS)


/*#define EH_LOW_SEG_BIT	VALID_POINTER_BITS
#define EH_SPLIT_HIGH_PRIO_BIT	(VALID_POINTER_BITS + 1)
#define EH_LEFT_TWO_SEG_BIT	(VALID_POINTER_BITS + 2)
#define EH_BUCKET_SPLIT_BIT	(VALID_POINTER_BITS + 3)
#define EH_BUCKET_MIGRATING_BIT	(VALID_POINTER_BITS + 4)*/


#define eh_seg_low(header)	((header) & SHIFT(EH_LOW_SEG_BIT))
#define eh_seg_extra(header)	((header) & SHIFT(EH_EXTRA_SEG_BIT))
#define eh_seg_splited(header)	((header) & SHIFT(EH_BUCKET_SPLIT_BIT))
#define eh_bucket_stayed(header)	((header) & SHIFT(EH_BUCKET_STAY_BIT))
#define eh_next_high_seg(header)	\
		(header & INTERVAL(EH_NEXT_SEG_BIT_START, EH_NEXT_SEG_BIT_END))
#define eh_lp_split_entry(header)	eh_next_high_seg(header)

#define set_eh_seg_low(next_seg)	(((uintptr_t)(next_seg)) |	\
											SHIFT(EH_LOW_SEG_BIT))
#define set_eh_seg_extra(header)	((header) | SHIFT(EH_EXTRA_SEG_BIT))
#define set_eh_seg_splited(header)	((header) | SHIFT(EH_BUCKET_SPLIT_BIT))
#define set_eh_split_entry(split_ent)	((uintptr_t)(split_ent))

#define set_eh_bucket_stayed(header)	((header) | SHIFT(EH_BUCKET_STAY_BIT))
#define clean_eh_bucket_stayed(header)	((header) & ~SHIFT(EH_BUCKET_STAY_BIT))

/*#define set_eh_split_entry_left(split_ent)	\
						(((uintptr_t)(split_ent)) |	SHIFT(EH_LEFT_TWO_SEG_BIT))*/


#define hashed_key_fingerprint18(hashed_key, l_depth)	\
		(SHIFT_OF(hashed_key, (l_depth) + EH_BUCKET_INDEX_BIT) >> (PREHASH_KEY_BITS - 18))
#define hashed_key_fingerprint16(hashed_key, l_depth)	\
		(SHIFT_OF(hashed_key, (l_depth) + EH_BUCKET_INDEX_BIT) >> (PREHASH_KEY_BITS - 16))

/*eh_slot [63~48:fingerprint16 47~3:kv_addr 2:ext_finger_bit 1~0:ext_finger2]*/

#define EH_SLOT_EXT_FING_START	0
#define EH_SLOT_EXT_FING_END	(EH_SLOT_EXT_FING_START + 1)

#define EH_SLOT_EXT_BIT	2

#define EH_SLOT_KV_ADDR_BIT_START	3
#define EH_SLOT_KV_ADDR_BIT_END	(VALID_POINTER_BITS - 1)

#define EH_SLOT_FING_BIT_START	VALID_POINTER_BITS
#define EH_SLOT_FING_BIT_END	(EH_SLOT_FING_BIT_START + 15)

#define EH_SLOT_EXT_FING_MASK	\
		INTERVAL(EH_SLOT_EXT_FING_START, EH_SLOT_EXT_FING_END)
#define EH_SLOT_EXT_MASK	(EH_SLOT_EXT_FING_MASK | SHIFT(EH_SLOT_EXT_BIT))
#define EH_SLOT_DELETE_STAT	SHIFT(EH_SLOT_EXT_FING_START)
#define EH_SLOT_INVALID_STAT	SHIFT(EH_SLOT_EXT_FING_END)

#define FREE_EH_SLOT	0x0000000000000000
#define END_EH_SLOT	(FREE_EH_SLOT |	\
			INTERVAL(EH_SLOT_FING_BIT_START, EH_SLOT_FING_BIT_END))
#define INVALID_DELETED_EH_SLOT	EH_SLOT_INVALID_STAT
//#define INVALID_EH_SLOT	EH_SLOT_DELETE_STAT

#define eh_slot_free(slot)	((slot) == FREE_EH_SLOT)
#define eh_slot_end(slot)	((slot) == END_EH_SLOT)
#define eh_slot_invalid(slot)	(((slot) & EH_SLOT_EXT_MASK) == EH_SLOT_INVALID_STAT)
#define eh_slot_deleted(slot)	(((slot) & EH_SLOT_EXT_MASK) == EH_SLOT_DELETE_STAT)
#define eh_slot_invalid_deleted(slot)	((slot) == INVALID_DELETED_EH_SLOT)
#define eh_slot_fingerprint16(slot)	((slot) >> EH_SLOT_FING_BIT_START)
#define eh_slot_fingerprint2(slot)	((slot) >> (EH_SLOT_FING_BIT_START + 14))
#define eh_slot_ext(slot)	((slot) & SHIFT(EH_SLOT_EXT_BIT))
#define eh_slot_ext_fing(slot)	\
			(((slot) & EH_SLOT_EXT_FING_MASK) >> EH_SLOT_EXT_FING_START)
#define eh_slot_kv_addr(slot)	((slot) &	\
		INTERVAL(EH_SLOT_KV_ADDR_BIT_START, EH_SLOT_KV_ADDR_BIT_END))

#define delete_eh_slot(slot)	(((slot) & ~EH_SLOT_EXT_MASK) | EH_SLOT_DELETE_STAT)
#define invalidate_eh_slot(slot)	(((slot) & ~EH_SLOT_EXT_MASK) | EH_SLOT_INVALID_STAT)
#define replace_eh_slot_kv_addr(slot, kv)	(((uintptr_t)(kv)) |	\
			((slot) & ~INTERVAL(EH_SLOT_KV_ADDR_BIT_START, EH_SLOT_KV_ADDR_BIT_END)))
#define make_eh_ext_slot(fingerprint18, kv)	\
			(((uintptr_t)(kv)) | SHIFT(EH_SLOT_EXT_BIT) |	\
			((fingerprint18 & INTERVAL(0, 1)) << EH_SLOT_EXT_FING_START) |	\
			SHIFT_OF((fingerprint18) >> 2, EH_SLOT_FING_BIT_START))
#define make_eh_slot(fingerprint16, kv)	(((uintptr_t)(kv)) |	\
						SHIFT_OF(fingerprint16, EH_SLOT_FING_BIT_START))
#define shift_eh_ext_fing(slot)	(INTERVAL(0, 15) &	\
			(SHIFT_OF(eh_slot_fingerprint16(slot), 2) | eh_slot_ext_fing(slot)))


struct eh_bucket {
	EH_BUCKET_HEADER header;
	EH_BUCKET_SLOT kv[EH_SLOT_NUM_PER_BUCKET];
};

struct eh_segment {
	struct eh_bucket bucket[EH_BUCKET_NUM];
};

struct eh_two_segment {
	union {
		struct eh_bucket bucket[MUL_2(EH_BUCKET_NUM, 1)];
		struct eh_segment seg[2];
	};
};

struct eh_four_segment {
	union {
		struct eh_bucket bucket[MUL_2(EH_BUCKET_NUM, 2)];
		struct eh_two_segment two_seg[2];
		struct eh_segment seg[4];
	};
};

typedef int (*EH_SEG_OP)(struct eh_segment*, struct kv*, u64, int, int);

int put_eh_seg_kv(
		struct eh_segment *low_seg,
		struct kv *kv, 
		u64 hashed_key,
		int l_dept0h, 
		int seg_op_node);

int get_eh_seg_kv(
		struct eh_segment *low_seg,
		struct kv *kv, 
		u64 hashed_key,
		int l_depth, 
		int seg_op_node);

int delete_eh_seg_kv(
		struct eh_segment *low_seg,
		struct kv *kv, 
		u64 hashed_key,
		int l_depth, 
		int seg_op_node);


#ifdef  __cplusplus
}
#endif	//__cplusplus

#endif	//__EH_SEG_H
