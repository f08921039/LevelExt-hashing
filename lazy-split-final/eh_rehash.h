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

#define EH_SPLIT_SEG_BIT_START	PAGE_SHIFT
#define EH_SPLIT_SEG_BIT_END	(VALID_POINTER_BITS - 1)


#define EH_SPLIT_TARGET_DEPTH_BIT_START	0
#define EH_SPLIT_TARGET_DEPTH_BIT_END	(EH_DEPTH_BITS - 1)

#define EH_SPLIT_TARGET_TYPE_BIT_START    (EH_SPLIT_TARGET_DEPTH_BIT_END + 1)
#define EH_SPLIT_TARGET_TYPE_BIT_END    (EH_SPLIT_TARGET_DEPTH_BIT_END + 2)


#define EH_SPLIT_TARGET_PREFIX_19_BIT	(EH_SPLIT_SEG_BIT_START - 1)
#define EH_SPLIT_TARGET_PREFIX_16_BIT	(EH_SPLIT_TARGET_PREFIX_19_BIT - 3)

#define EH_SPLIT_TARGET_SEG_BIT_START	EH_SPLIT_SEG_BIT_START
#define EH_SPLIT_TARGET_SEG_BIT_END	EH_SPLIT_SEG_BIT_END

#define EH_SPLIT_TARGET_PREFIX_48_BIT	(EH_SPLIT_SEG_BIT_END + 1)
#define EH_SPLIT_TARGET_PREFIX_63_BIT	(EH_SPLIT_TARGET_PREFIX_48_BIT + 15)


#define EH_SPLIT_DEST_PREFIX_20_BIT	0
#define EH_SPLIT_DEST_PREFIX_31_BIT	(EH_SPLIT_SEG_BIT_START - 1)

#define EH_SPLIT_DEST_SEG_BIT_START	EH_SPLIT_SEG_BIT_START
#define EH_SPLIT_DEST_SEG_BIT_END	EH_SPLIT_SEG_BIT_END

#define EH_SPLIT_DEST_PREFIX_32_BIT	(EH_SPLIT_SEG_BIT_END + 1)
#define EH_SPLIT_DEST_PREFIX_47_BIT	(EH_SPLIT_DEST_PREFIX_32_BIT + 15)

#define EH_SPLIT_TARGET_TYPE_MASK	INTERVAL(EH_SPLIT_TARGET_TYPE_BIT_START, EH_SPLIT_TARGET_TYPE_BIT_END)

#define EH_SPLIT_TARGET_SEG_MASK	INTERVAL(EH_SPLIT_TARGET_SEG_BIT_START, EH_SPLIT_TARGET_SEG_BIT_END)
#define EH_SPLIT_DEST_SEG_MASK	INTERVAL(EH_SPLIT_DEST_SEG_BIT_START, EH_SPLIT_DEST_SEG_BIT_END)

#define INITIAL_EH_SPLIT_TARGET    0UL
#define INVALID_EH_SPLIT_TARGET    ~(EH_SPLIT_TARGET_SEG_MASK | EH_SPLIT_TARGET_TYPE_MASK)

/*
#define eh_split_target_type(target)	\
			INTERVAL_OF(target, EH_SPLIT_TARGET_TYPE_BIT_START, EH_SPLIT_TARGET_TYPE_BIT_END)
#define eh_split_target_depth(target)   ((target) &	\
			INTERVAL(EH_SPLIT_TARGET_DEPTH_BIT_START, EH_SPLIT_TARGET_DEPTH_BIT_END))
#define eh_split_target_seg(target)	((target) &	\
			INTERVAL(EH_SPLIT_TARGET_SEG_BIT_START, EH_SPLIT_TARGET_SEG_BIT_END))
#define eh_split_dest_seg(dest)	((dest) &	\
			INTERVAL(EH_SPLIT_DEST_SEG_BIT_START, EH_SPLIT_DEST_SEG_BIT_END))
*/
#define eh_split_range_prefix(ent, bit1, bit2, td)	\
	(INTERVAL_OF(ent, EH_SPLIT_##td##_PREFIX_##bit1##_BIT, EH_SPLIT_##td##_PREFIX_##bit2##_BIT) << bit1)

#define shift_prefix_for_eh_split(hashed_key, bit1, bit2, td)	\
	(INTERVAL_OF(hashed_key, bit1, bit2) << EH_SPLIT_##td##_PREFIX_##bit1##_BIT)


/*
#define eh_split_prefix(target, dest, depth)	\
				((eh_split_range_prefix(target, 48, 63, TARGET) |	\
					eh_split_range_prefix(dest, 32, 47, DEST) |	\
					eh_split_range_prefix(dest, 20, 31, DEST) |	\
					eh_split_range_prefix(target, 16, 19, TARGET))	\
					& MASK(PREHASH_KEY_BITS - depth))

#define make_eh_split_target_entry(target, hashed_key, depth, type)    \
                    (((uintptr_t)(target)) |	\
						shift_prefix_for_eh_split(hashed_key, 48, 63, TARGET) |	\
						shift_prefix_for_eh_split(hashed_key, 16, 19, TARGET)	|	\
						SHIFT_OF(type, EH_SPLIT_TARGET_TYPE_BIT_START) |	\
						SHIFT_OF(depth, EH_SPLIT_TARGET_DEPTH_BIT_START))

#define make_eh_split_dest_entry(dest, hashed_key) (((uintptr_t)(dest)) \
                    | shift_prefix_for_eh_split(hashed_key, 32, 47, DEST)  \
                    | shift_prefix_for_eh_split(hashed_key, 20, 31, DEST))
*/

//target [63~48:hashed_key_63_48 47~12:rehashed_segment 11~8:hashed_key_19~16 7~6:type 5~0:depth]
//destination [63~48:hashed_key_47_32 47~12:new_four_segment 11~0:hashed_key_31~20]

typedef enum {
	INVALID_SPLIT = 0,
	NORMAL_SPLIT, 
	URGENT_SPLIT, 
	INCOMPLETE_SPLIT
} __attribute__ ((__packed__)) SPLIT_TYPE;

typedef enum {
    FEW_SPLITS = 0, 
    MODERATE_SPLITS, 
    MANY_SPLITS
} __attribute__ ((__packed__)) SPLIT_STATE;

struct eh_split_entry {
    uintptr_t target;
    uintptr_t destination;
};

struct eh_split_context {
	union {
		struct eh_split_entry entry;
		struct {
			struct eh_segment *target_seg;
			struct eh_segment *dest_seg;
		};
	};
	struct eh_segment *inter_seg;
	u64 hashed_prefix;
	unsigned short depth;
	unsigned short bucket_id;
	SPLIT_TYPE type;
	SPLIT_STATE state;
	bool thread;
	char padding;
};

static inline
bool is_eh_split_targey_valid(uintptr_t target) {
	return !!(target & EH_SPLIT_TARGET_TYPE_MASK);
}

static inline 
struct eh_segment *eh_s_ent_target_seg(uintptr_t target) {
	return (struct eh_segment *)(target & EH_SPLIT_TARGET_SEG_MASK);
}

static inline
SPLIT_TYPE eh_split_type(struct eh_split_entry *ent) {
	uintptr_t target = ent->target;

	return (SPLIT_TYPE)INTERVAL_OF(target, 
				EH_SPLIT_TARGET_TYPE_BIT_START, EH_SPLIT_TARGET_TYPE_BIT_END);
}

static inline
int eh_split_depth(struct eh_split_entry *ent) {
	uintptr_t target = ent->target;

	return INTERVAL_OF(target, EH_SPLIT_TARGET_DEPTH_BIT_START, 
										EH_SPLIT_TARGET_DEPTH_BIT_END);
}

static inline
struct eh_segment *eh_split_target_seg(struct eh_split_entry *ent) {
	uintptr_t target = ent->target;

	return (struct eh_segment *)(target & EH_SPLIT_TARGET_SEG_MASK);
}

static inline
struct eh_segment *eh_split_dest_seg(struct eh_split_entry *ent) {
	uintptr_t dest = ent->destination;

	return (struct eh_segment *)(dest & EH_SPLIT_DEST_SEG_MASK);
}

static inline
u64 eh_split_prefix(struct eh_split_entry *ent) {
	uintptr_t target, dest;

	target = ent->target;
	dest = ent->destination;

	return eh_split_range_prefix(target, 48, 63, TARGET) |	
					eh_split_range_prefix(dest, 32, 47, DEST) |	
					eh_split_range_prefix(dest, 20, 31, DEST) |	
					eh_split_range_prefix(target, 16, 19, TARGET);
}

static inline
uintptr_t make_eh_split_target_entry(
					struct eh_segment *target_seg, 
					u64 hashed_key, 
					int depth, 
					SPLIT_TYPE type) {
	return ((uintptr_t)(target_seg)) | 
				shift_prefix_for_eh_split(hashed_key, 48, 63, TARGET) | 
				shift_prefix_for_eh_split(hashed_key, 16, 19, TARGET) | 
				SHIFT_OF(type, EH_SPLIT_TARGET_TYPE_BIT_START) | 
				SHIFT_OF(depth, EH_SPLIT_TARGET_DEPTH_BIT_START);
}

static inline
uintptr_t make_eh_split_dest_entry(
				struct eh_segment *dest_seg, 
				u64 hashed_key) {
	return ((uintptr_t)(dest_seg)) | 
				shift_prefix_for_eh_split(hashed_key, 32, 47, DEST) | 
				shift_prefix_for_eh_split(hashed_key, 20, 31, DEST);
}

static inline
struct eh_split_entry make_eh_split_entry(
						struct eh_segment *target_seg, 
						struct eh_segment *dest_seg, 
						u64 hashed_key, 
						int depth, 
						SPLIT_TYPE type) {
	struct eh_split_entry ent;

	ent.target = make_eh_split_target_entry(target_seg, hashed_key, depth, type);
	ent.destination = make_eh_split_dest_entry(dest_seg, hashed_key);

	return ent;
}


static inline 
void invalidate_eh_split_entry(struct eh_split_entry *ent) {
	release_fence();
	WRITE_ONCE(ent->target, INVALID_EH_SPLIT_TARGET);
}

static inline 
void init_eh_split_entry(
        	struct eh_split_entry *ent,
			struct eh_segment *target_seg, 
			struct eh_segment *dest_seg, 
			u64 hashed_key, 
			int depth, 
			SPLIT_TYPE type) {
	struct eh_split_entry new_ent;

	hashed_key &= MASK(PREHASH_KEY_BITS - depth);

	new_ent = make_eh_split_entry(target_seg, dest_seg, hashed_key, depth, type);

	ent->destination = new_ent.destination;

	release_fence();
	WRITE_ONCE(ent->target, new_ent.target);
}

static inline 
int upgrade_eh_split_entry( 
				struct eh_split_entry *lp_ent, 
				struct eh_segment *target_seg) {
	uintptr_t old_target, target;

	target = READ_ONCE(lp_ent->target);

	/*if (target != INVALID_EH_SPLIT_TARGET && 
						target != INITIAL_EH_SPLIT_TARGET) {*/
	if (is_eh_split_targey_valid(target)) {
		if (unlikely(target_seg != eh_s_ent_target_seg(target)))
			return -1;

		old_target = cas(&lp_ent->target, target, INVALID_EH_SPLIT_TARGET);

		if (unlikely(old_target != target))
			return -1;

		return 0;
	}

	return -1;
}

static inline 
int check_eh_split_entry(struct eh_split_entry *split_ent) {
	uintptr_t target;

	target = READ_ONCE(split_ent->target);

	if (unlikely(target == INITIAL_EH_SPLIT_TARGET))
        return 1;

	if (unlikely(target == INVALID_EH_SPLIT_TARGET))
        return -1;

	return 0;
}

static inline 
int analyze_eh_split_entry(
                struct eh_split_entry *split_ent, 
				struct eh_split_context *split) {
	struct eh_split_entry ent;

	ent.target = READ_ONCE(split_ent->target);

	if (unlikely(ent.target == INITIAL_EH_SPLIT_TARGET))
        return 1;

	if (unlikely(ent.target == INVALID_EH_SPLIT_TARGET))
		return -1;

	if (unlikely(!cas_bool(&split_ent->target, 
						ent.target, INVALID_EH_SPLIT_TARGET)))
		return -1;

	ent.destination = split_ent->destination;

	split->target_seg = eh_split_target_seg(&ent);
	split->dest_seg = eh_split_dest_seg(&ent);
	split->inter_seg = NULL;

	split->hashed_prefix = eh_split_prefix(&ent);

	split->bucket_id = 0;
	split->depth = eh_split_depth(&ent);// below 48
	split->type = eh_split_type(&ent);
	split->thread = false;

	return 0;
}

__attribute__((optimize("unroll-loops")))
static inline 
void prefetch_eh_segment_for_normal_split(
					struct eh_segment *seg_l0,
					struct eh_segment *seg_l1, 
					int id) {
	void *addr1, *addr2;
	int fetch1, fetch2, i, j, k;

	fetch2 = DIV_2(EH_PER_BUCKET_CACHELINE, 1) + DIV_2(EH_PER_BUCKET_CACHELINE, 2);

	for (j = 0; j < 2; ++j) {
		addr2 = &seg_l1->bucket[MUL_2(id, 1) + j];

		for (k = 0; k < fetch2; ++k)
			prefech_w0(addr2 + MUL_2(k, CACHE_LINE_SHIFT));
	}

	fetch1 = EH_PER_BUCKET_CACHELINE;
	addr1 = &seg_l0->bucket[id];

	for (i = 0; i < fetch1; ++i)
		prefech_w0(addr1 + MUL_2(i, CACHE_LINE_SHIFT));
}

__attribute__((optimize("unroll-loops")))
static inline 
void prefetch_part1_eh_segment_for_urgent_split(
					struct eh_segment *seg_l0,
					int id) {
	void *addr1;
	int fetch1, i;

	fetch1 = EH_PER_BUCKET_CACHELINE;
	addr1 = &seg_l0->bucket[id];

	for (i = 0; i < fetch1; ++i)
		prefech_w0(addr1 + MUL_2(i, CACHE_LINE_SHIFT));
}

__attribute__((optimize("unroll-loops")))
static inline 
void prefetch_part2_eh_segment_for_urgent_split(
					struct eh_segment *seg_l1, 
					int id) {
	void *addr2;
	int h;

	for (h = 0; h < 2; ++h) {
		addr2 = &seg_l1->bucket[MUL_2(id, 1) + h];

		prefech_w0(addr2);
	}
}

__attribute__((optimize("unroll-loops")))
static inline 
void prefetch_part3_eh_segment_for_urgent_split(
					struct eh_segment *seg_l2, 
					int id) {
	void *addr3;
	int fetch3, j, k;

	fetch3 = DIV_2(EH_PER_BUCKET_CACHELINE, 1);

	for (j = 0; j < 4; ++j) {
		addr3 = &seg_l2->bucket[MUL_2(id, 2) + j];

		for (k = 0; k < fetch3; ++k)
			prefech_w0(addr3 + MUL_2(k, CACHE_LINE_SHIFT));
	}
}


static inline 
void prefetch_eh_segment_for_urgent_split(
					struct eh_segment *seg_l0,
					struct eh_segment *seg_l1, 
					struct eh_segment *seg_l2, 
					int id) {
	prefetch_part3_eh_segment_for_urgent_split(seg_l2, id);
	prefetch_part1_eh_segment_for_urgent_split(seg_l0, id);
	prefetch_part2_eh_segment_for_urgent_split(seg_l1, id);
}

void modify_eh_split_entry(
		struct eh_split_entry *s_ent, 
		EH_BUCKET_HEADER modified_header, 
		struct eh_segment *target_seg, 
		struct eh_segment *dest_seg, 
		u64 hashed_key, int depth);

int eh_split(struct eh_split_context *split);


#ifdef  __cplusplus
}
#endif	//__cplusplus

#endif	//__EH_REHASH_H
