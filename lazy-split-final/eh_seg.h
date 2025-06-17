#ifndef __EH_SEG_H
#define __EH_SEG_H

//#define LARGE_EH_SEGMENT	1
//#define WIDE_EH_BUCKET_INDEX_BIT	1

#include "compiler.h"
#include "kv.h"

#include "eh_context.h"
//#include "eh_rehash.h"


#define EH_SEGMENT_SIZE	32768
#define EH_SEGMENT_SIZE_BITS	15


#define EH_BUCKET_INDEX_BIT	7


typedef u64 EH_BUCKET_SLOT;

typedef EH_BUCKET_SLOT EH_BUCKET_HEADER;


#define EH_BUCKET_SLOT_SIZE	sizeof(EH_BUCKET_SLOT)
#define EH_BUCKET_SLOT_BIT	MUL_2(EH_BUCKET_SLOT_SIZE, 3)
#define EH_BUCKET_HEADER_SIZE	sizeof(EH_BUCKET_HEADER)

#define EH_BUCKET_NUM	EXP_2(EH_BUCKET_INDEX_BIT)
#define EH_BUCKET_SIZE DIV_2(EH_SEGMENT_SIZE, EH_BUCKET_INDEX_BIT)
#define EH_BUCKET_SIZE_BIT	(EH_SEGMENT_SIZE_BITS - EH_BUCKET_INDEX_BIT)


#define EH_SLOT_NUM_PER_BUCKET	((EH_BUCKET_SIZE - EH_BUCKET_HEADER_SIZE) / EH_BUCKET_SLOT_SIZE)
#define EH_PER_BUCKET_CACHELINE	DIV_2(EH_BUCKET_SIZE, CACHE_LINE_SHIFT)
#define EH_SLOT_NUM_PER_CACHELINE	(CACHE_LINE_SIZE / EH_BUCKET_SLOT_SIZE)



#ifdef  __cplusplus
extern  "C" {
#endif

#define EH_LOW_SEG_BIT	0
#define EH_URGENT_SEG_BIT	1
#define EH_BUCKET_SPLIT_BIT	2
#define EH_BUCKET_STAY_BIT	3
#define EH_FOUR_SEG_BIT	4
//#define EH_EXTRA_SEG_BIT	4
//#define EH_FAKE_SEG_BIT	4
//#define EH_NEXT_SEG_JUMP_BIT	5

#define EH_NEXT_SEG_BIT_START	PAGE_SHIFT
#define EH_NEXT_SEG_BIT_END	(VALID_POINTER_BITS - 1)

#define EH_SPLIT_ENT_BIT_START	4
#define EH_SPLIT_ENT_BIT_END	(VALID_POINTER_BITS - 1)

#define EH_SPLIT_ENT_PRIO_BIT_START	VALID_POINTER_BITS
#define EH_SPLIT_ENT_PRIO_BIT_END	(EH_SPLIT_ENT_PRIO_BIT_START + 2)


#define EH_BUCKET_TOP_BIT	(POINTER_BITS - 1)
#define EH_BUCKET_PREALLOC_BIT	(POINTER_BITS - 2)

#define INITIAL_EH_BUCKET_HEADER	0UL
#define INITIAL_EH_BUCKET_TOP_HEADER	SHIFT(EH_BUCKET_TOP_BIT)
#define INITIAL_EH_BUCKET_PREALLOC_HEADER	SHIFT(EH_BUCKET_PREALLOC_BIT)



/*eh_slot [63~48:fingerprint16 47~3:kv_addr 2:ext_finger_bit 1~0:ext_finger2]*/
#define EH_SLOT_FLAG_BIT_START	0
#define EH_SLOT_FLAG_BIT_END	2

#define EH_SLOT_EXT_FING1_BIT	0
#define EH_SLOT_EXT_FING2_BIT	1

#define EH_SLOT_EXT1_BIT	1
#define EH_SLOT_EXT2_BIT	2

#define EH_SLOT_KV_ADDR_BIT_START	3
#define EH_SLOT_KV_ADDR_BIT_END	(VALID_POINTER_BITS - 1)

#define EH_SLOT_FING_BIT_START	VALID_POINTER_BITS
#define EH_SLOT_FING_BIT_END	(EH_SLOT_FING_BIT_START + 15)

#define EH_SLOT_KV_ADDR_MASK	INTERVAL(EH_SLOT_KV_ADDR_BIT_START, EH_SLOT_KV_ADDR_BIT_END)
#define EH_SLOT_FLAG_MASK	INTERVAL(EH_SLOT_FLAG_BIT_START, EH_SLOT_FLAG_BIT_END)

#define EH_SLOT_DELETE_STAT	SHIFT(EH_SLOT_FLAG_BIT_START)


#define FREE_EH_SLOT	0x0000000000000000
#define END_EH_SLOT	(FREE_EH_SLOT |	\
			INTERVAL(EH_SLOT_FING_BIT_START, EH_SLOT_FING_BIT_END))

//#define INVALID_EH_SLOT	EH_SLOT_DELETE_STAT


typedef enum {
	URGENT_PRIO = 0,
	HIGH_PRIO = 1, 
	//NUMA_PRIO = 2, 
	LOW_PRIO = 2, 
	IDLE_PRIO = 3, 
	INCOMPLETE_PRIO = 4, 
	//NOT_THREAD_PRIO = INCOMPLETE_PRIO, 
	THREAD_PRIO = 5, 
	THREAD2_PRIO = 6
} __attribute__ ((__packed__)) SPLIT_PRIORITY;


typedef enum {
    BUTTOM_SEG = 0,  
	TOP_SEG, 
	NUMA_BUTTOM_SEG, 
	NUMA_TOP_SEG
} __attribute__ ((__packed__)) SEG_POSITION;


struct eh_bucket {
	EH_BUCKET_HEADER header;
	EH_BUCKET_SLOT kv[EH_SLOT_NUM_PER_BUCKET];
};

struct eh_segment {
	struct eh_bucket bucket[EH_BUCKET_NUM];
};


struct eh_seg_context {
	struct eh_segment *cur_seg;
	struct eh_segment *buttom_seg;
	unsigned char node_id;
	unsigned char depth;
	unsigned char layer;
	SEG_POSITION pos;
};

struct eh_split_context;

static inline 
int eh_seg_bucket_idx(u64 hashed_key, int depth) {
	return (SHIFT_OF(hashed_key, depth) >> (PREHASH_KEY_BITS - EH_BUCKET_INDEX_BIT));
}

static inline 
int eh_seg2_bucket_idx(u64 hashed_key, int depth) {
	return (SHIFT_OF(hashed_key, depth) >> (PREHASH_KEY_BITS - (EH_BUCKET_INDEX_BIT + 1)));
}

static inline 
int eh_seg4_bucket_idx(u64 hashed_key, int depth) {
	return (SHIFT_OF(hashed_key, depth) >> (PREHASH_KEY_BITS - (EH_BUCKET_INDEX_BIT + 2)));
}

static inline 
int eh_seg_id_in_seg2(u64 hashed_key, int depth) {
	return (SHIFT_OF(hashed_key, depth) >> (PREHASH_KEY_BITS - 1));
}

//

static inline 
bool eh_seg_low(EH_BUCKET_HEADER header) {
	return !!(header & SHIFT(EH_LOW_SEG_BIT));
}


static inline 
int eh_seg_urgent(EH_BUCKET_HEADER header) {
	return !!(header & SHIFT(EH_URGENT_SEG_BIT));
}

static inline 
bool eh_seg_splited(EH_BUCKET_HEADER header) {
	return !!(header & SHIFT(EH_BUCKET_SPLIT_BIT));
}

static inline 
bool eh_four_seg(EH_BUCKET_HEADER header) {
	return !!(header & SHIFT(EH_FOUR_SEG_BIT));
}

static inline 
bool eh_bucket_stayed(EH_BUCKET_HEADER header) {
	return !!(header & SHIFT(EH_BUCKET_STAY_BIT));
}

static inline 
struct eh_segment *eh_next_high_seg(EH_BUCKET_HEADER header) {
	return (struct eh_segment *)
			(header & INTERVAL(EH_NEXT_SEG_BIT_START, EH_NEXT_SEG_BIT_END));
}

static inline 
struct eh_split_entry *eh_split_entry_addr(EH_BUCKET_HEADER header) {
	return (struct eh_split_entry *)
			(header & INTERVAL(EH_SPLIT_ENT_BIT_START, EH_SPLIT_ENT_BIT_END));
}

static inline 
SPLIT_PRIORITY eh_split_entry_priority(EH_BUCKET_HEADER header) {
	return (SPLIT_PRIORITY)INTERVAL_OF(header, 
								EH_SPLIT_ENT_PRIO_BIT_START, 
								EH_SPLIT_ENT_PRIO_BIT_END);
}

static inline 
int eh_bucket_initial(EH_BUCKET_HEADER header) {
	return header == INITIAL_EH_BUCKET_HEADER || 
			header == INITIAL_EH_BUCKET_TOP_HEADER || 
			header == INITIAL_EH_BUCKET_PREALLOC_HEADER;
}

static inline 
int eh_bucket_no_normal_initial(EH_BUCKET_HEADER header) {
	return header == INITIAL_EH_BUCKET_TOP_HEADER || 
			header == INITIAL_EH_BUCKET_PREALLOC_HEADER;
}

static inline 
int eh_bucket_no_prealloc_initial(EH_BUCKET_HEADER header) {
	return header == INITIAL_EH_BUCKET_HEADER || 
			header == INITIAL_EH_BUCKET_TOP_HEADER;
}

static inline 
int eh_bucket_no_top_initial(EH_BUCKET_HEADER header) {
	return header == INITIAL_EH_BUCKET_HEADER || 
			header == INITIAL_EH_BUCKET_PREALLOC_HEADER;
}

static inline 
EH_BUCKET_HEADER set_eh_seg_low(struct eh_segment *next_seg) {
	return ((uintptr_t)next_seg) | SHIFT(EH_LOW_SEG_BIT);
}

static inline 
EH_BUCKET_HEADER set_eh_four_seg(EH_BUCKET_HEADER header) {
	return header | SHIFT(EH_FOUR_SEG_BIT);
}

static inline 
EH_BUCKET_HEADER set_eh_seg_urgent(EH_BUCKET_HEADER header) {
	return header | SHIFT(EH_URGENT_SEG_BIT);
}

static inline 
EH_BUCKET_HEADER set_eh_seg_splited(EH_BUCKET_HEADER header) {
	return header | SHIFT(EH_BUCKET_SPLIT_BIT);
}

static inline 
EH_BUCKET_HEADER cancel_eh_seg_splited(EH_BUCKET_HEADER header) {
	return header & ~SHIFT(EH_BUCKET_SPLIT_BIT);
}

static inline 
EH_BUCKET_HEADER set_eh_split_entry(
					struct eh_split_entry *split_ent, 
					SPLIT_PRIORITY prio) {
	return ((uintptr_t)split_ent) |	SHIFT_OF(prio, EH_SPLIT_ENT_PRIO_BIT_START);
}

static inline 
EH_BUCKET_HEADER set_eh_bucket_stayed(EH_BUCKET_HEADER header) {
	return header | SHIFT(EH_BUCKET_STAY_BIT);
}

static inline 
EH_BUCKET_HEADER cancel_eh_bucket_stayed(EH_BUCKET_HEADER header) {
	return header & ~SHIFT(EH_BUCKET_STAY_BIT);
}

//

static inline 
u64 hashed_key_fingerprint(u64 hashed_key, int depth, int msb) {
	return SHIFT_OF(hashed_key, depth + EH_BUCKET_INDEX_BIT) >> (PREHASH_KEY_BITS - msb);
}

/*static inline 
u64 hashed_key_fingerprint18(u64 hashed_key, int depth) {
	return SHIFT_OF(hashed_key, depth + EH_BUCKET_INDEX_BIT) >> (PREHASH_KEY_BITS - 18);
}

static inline 
u64 hashed_key_fingerprint16(u64 hashed_key, int depth) {
	return SHIFT_OF(hashed_key, depth + EH_BUCKET_INDEX_BIT) >> (PREHASH_KEY_BITS - 16);
}*/


static inline 
int eh_slot_free(EH_BUCKET_SLOT slot) {
	return slot == FREE_EH_SLOT;
}

static inline 
int eh_slot_end(EH_BUCKET_SLOT slot) {
	return slot == END_EH_SLOT;
}

static inline 
int eh_slot_invalid(EH_BUCKET_SLOT slot) {
	return (slot & (EH_SLOT_KV_ADDR_MASK | EH_SLOT_FLAG_MASK)) == EH_SLOT_DELETE_STAT;
}

static inline 
int eh_slot_deleted(EH_BUCKET_SLOT slot) {
	return (slot & EH_SLOT_FLAG_MASK) == EH_SLOT_DELETE_STAT;
}


static inline 
u64 eh_slot_fingerprint(EH_BUCKET_SLOT slot, int msb) {
	return slot >> (1 + EH_SLOT_FING_BIT_END - msb);
}

static inline 
u64 eh_slot_ext_fingerprint(EH_BUCKET_SLOT slot, int ext) {
	return INTERVAL_OF(slot, EH_SLOT_EXT_FING1_BIT, EH_SLOT_EXT_FING1_BIT + ext - 1);
}

/*static inline 
u64 eh_slot_fingerprint16(EH_BUCKET_SLOT slot) {
	return slot >> EH_SLOT_FING_BIT_START;
}

static inline 
u64 eh_slot_fingerprint2(EH_BUCKET_SLOT slot) {
	return slot >> (EH_SLOT_FING_BIT_START + 14);
}*/


static inline
int eh_slot_ext(EH_BUCKET_SLOT slot) {
	if (slot & SHIFT(EH_SLOT_EXT2_BIT))
		return 2;

	if (slot & SHIFT(EH_SLOT_EXT1_BIT))
		return 1;

	return 0;
}

static inline 
struct kv *eh_slot_kv_addr(EH_BUCKET_SLOT slot) {
	return (struct kv *)(slot & EH_SLOT_KV_ADDR_MASK);
}

static inline 
EH_BUCKET_SLOT set_eh_slot_deleted(EH_BUCKET_SLOT slot) {
	return (slot & ~EH_SLOT_FLAG_MASK) | EH_SLOT_DELETE_STAT;
}

static inline 
EH_BUCKET_SLOT set_eh_slot_invalidated(EH_BUCKET_SLOT slot) {
	return (slot & ~(EH_SLOT_KV_ADDR_MASK | EH_SLOT_FLAG_MASK)) | EH_SLOT_DELETE_STAT;
}

static inline 
EH_BUCKET_SLOT set_eh_slot_replaced_kv(
						EH_BUCKET_SLOT slot, 
						struct kv *kv) {
	return ((uintptr_t)kv) | (slot & ~EH_SLOT_KV_ADDR_MASK);
}

static inline 
EH_BUCKET_SLOT make_eh_ext2_slot(u64 fingerprint18, struct kv *kv) {
	return ((uintptr_t)kv) | SHIFT(EH_SLOT_EXT2_BIT) |
			((fingerprint18 & INTERVAL(0, 1)) << EH_SLOT_EXT_FING1_BIT) |	
			SHIFT_OF(fingerprint18 >> 2, EH_SLOT_FING_BIT_START);
}

static inline 
EH_BUCKET_SLOT make_eh_ext1_slot(u64 fingerprint17, struct kv *kv) {
	return ((uintptr_t)kv) | SHIFT(EH_SLOT_EXT1_BIT) |
			((fingerprint17 & INTERVAL(0, 0)) << EH_SLOT_EXT_FING1_BIT) |	
			SHIFT_OF(fingerprint17 >> 1, EH_SLOT_FING_BIT_START);
}


static inline 
EH_BUCKET_SLOT make_eh_slot(u64 fingerprint16, struct kv *kv) {
	return ((uintptr_t)kv) | SHIFT_OF(fingerprint16, EH_SLOT_FING_BIT_START);
}


/*static inline 
EH_BUCKET_SLOT make_eh_slot_from_old(EH_BUCKET_SLOT slot, struct kv *kv, int exts, int shifts) {
	EH_BUCKET_SLOT new_slot;
	u64 fing16;

	new_slot = (uintptr_t)kv;
	fing16 = eh_slot_fingerprint(slot, 16);
	fing16 = SHIFT_OF(fing16, shifts) & INTERVAL(0, 15);

	if (shifts == 2)
		fing16 |= INTERVAL_OF(slot, EH_SLOT_EXT_FING1_BIT, EH_SLOT_EXT_FING2_BIT);
	else if (exts == 1)
		fing16 |= INTERVAL_OF(slot, EH_SLOT_EXT_FING1_BIT, EH_SLOT_EXT_FING1_BIT);
	else {
		fing16 |= INTERVAL_OF(slot, EH_SLOT_EXT_FING2_BIT, EH_SLOT_EXT_FING2_BIT);
		new_slot |= (SHIFT(EH_SLOT_EXT1_BIT) | 
						(slot & INTERVAL(EH_SLOT_EXT_FING1_BIT, EH_SLOT_EXT_FING1_BIT)));
	}

	new_slot |= fing16;

	return new_slot;
}*/

/*static inline 
u64 shift_eh_ext_fing(EH_BUCKET_SLOT slot, int exts, int shifts) { 
	return INTERVAL(0, 15) &
			(SHIFT_OF(eh_slot_fingerprint16(slot), 2) | eh_slot_ext_fing(slot));
}*/

__attribute__((optimize("unroll-loops")))
static inline 
void prefetch_eh_bucket_head(struct eh_bucket *bucket) {
	int i;

	for (i = 0; i < EH_PER_BUCKET_CACHELINE; ++i)
		prefech_r0(((void *)bucket) + MUL_2(i, CACHE_LINE_SHIFT));
}


int insert_eh_seg_kv(
		struct kv *kv, 
		KEY_ITEM key, 
		u64 hashed_key, 
		struct eh_bucket *bucket, 
		struct eh_seg_context *seg_context);

int update_eh_seg_kv(
		struct kv *kv, 
		KEY_ITEM key, 
		u64 hashed_key, 
		struct eh_bucket *bucket, 
		int l_depth);

int delete_eh_seg_kv(
		KEY_ITEM key, 
		u64 hashed_key, 
		struct eh_bucket *bucket, 
		int l_depth);

struct kv *lookup_eh_seg_kv(
		KEY_ITEM key,  
		u64 hashed_key, 
		struct eh_bucket *bucket, 
		int l_depth);


#ifdef  __cplusplus
}
#endif	//__cplusplus

#endif	//__EH_SEG_H
