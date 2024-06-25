#ifndef __EXT_HASH_H
#define __EXT_HASH_H

//#define LARGE_EH_SEGMENT	1
//#define WIDE_EH_BUCKET_INDEX_BIT	1

#include "compiler.h"
#include "per_thread.h"
#include "kv.h"


#ifdef LARGE_EH_SEGMENT
	#define EH_SEGMENT_SIZE	32768
	#define EH_SEGMENT_SIZE_BIT	15
#else
	#define EH_SEGMENT_SIZE	16384
	#define EH_SEGMENT_SIZE_BIT	14
#endif


#ifdef WIDE_EH_BUCKET_INDEX_BIT
	#define EH_BUCKET_INDEX_BIT	7
#else
	#define EH_BUCKET_INDEX_BIT	3
#endif

#define EH_SEGMENT_SIZE_MASK	~(EH_SEGMENT_SIZE - 1)

#define EH_TWO_SEGMENT_SIZE	(EH_SEGMENT_SIZE << 1)
#define EH_TWO_SEGMENT_SIZE_BIT	(EH_SEGMENT_SIZE_BIT + 1)
#define EH_TWO_SEGMENT_SIZE_MASK	~(EH_TWO_SEGMENT_SIZE - 1)

#define EH_HALF_SEGMENT_SIZE	(EH_SEGMENT_SIZE >> 1)
#define EH_HALF_SEGMENT_SIZE_MASK	~(EH_HALF_SEGMENT_SIZE - 1)

typedef u64 EH_KV_ITEM;

#define EH_KV_ITEM_SIZE	sizeof(EH_KV_ITEM)
#define EH_KV_ITEM_BIT	(EH_KV_ITEM_SIZE << 3)

#define EH_BUCKET_NUM	(1 << EH_BUCKET_INDEX_BIT)
#define EH_BUCKET_SIZE (EH_SEGMENT_SIZE >> EH_BUCKET_INDEX_BIT)
#define EH_BUCKET_SIZE_BIT	(EH_SEGMENT_SIZE_BIT - EH_BUCKET_INDEX_BIT)

//#define EH_PER_SEGMENT_KV_NUM	(EH_SEGMENT_SIZE / EH_KV_ITEM_SIZE)
#define EH_PER_BUCKET_KV_NUM	(EH_BUCKET_SIZE / EH_KV_ITEM_SIZE)
#define EH_PER_BUCKET_CACHELINE	(EH_BUCKET_SIZE >> CACHE_LINE_SHIFT)
#define PER_CACHELINE_EH_KV_ITEM	(CACHE_LINE_SIZE / EH_KV_ITEM_SIZE)


#define EH_DEPTH_BITS	8
#define EH_DEPTH_MAX	((1UL << EH_DEPTH_BITS) - 1)


#define EH_EXTENSION_STEP	1


#ifdef  __cplusplus
extern  "C" {
#endif


#define eh_dir_index(hashed_key, depth)	((hashed_key) >> (EH_KV_ITEM_BIT - (depth)))
#define eh_dir_floor_index(hashed_key, l_depth, g_depth)	(eh_dir_index(hashed_key, l_depth) << ((g_depth) - (l_depth)))

#define eh_bucket_index(hashed_key, depth)	(((hashed_key) << (depth)) >> (EH_KV_ITEM_BIT - EH_BUCKET_INDEX_BIT))

#define round_down_to_half_seg(addr)	(((uintptr_t)(addr)) & EH_HALF_SEGMENT_SIZE_MASK)





#define EH_SEG_LOCKED_SHIFT	((EH_DEPTH_BITS - 1) + 1)
#define EH_SEG_SPLITING_SHIFT	((EH_DEPTH_BITS - 1) + 2)
#define EH_SEG_MIGRATE_SHIFT	((EH_DEPTH_BITS - 1) + 3)

#define eh_seg_depth(ent1)	((ent1) & EH_DEPTH_MAX)
#define eh_seg_locked(ent1)	((ent1) & (1UL << EH_SEG_LOCKED_SHIFT))
#define eh_seg_spliting(ent1)	((ent1) & (1UL << EH_SEG_SPLITING_SHIFT))
#define eh_seg_migrate(ent1)	((ent1) & (1UL << EH_SEG_MIGRATE_SHIFT))
#define eh_seg_changed(ent1, new_ent1)	((ent1) ^ (new_ent1))
#define extract_eh_seg_addr(ent)	((ent) & (VALID_POINTER_MASK & PAGE_MASK))

#define set_eh_seg_locked(ent1)	((ent1) | (1UL << EH_SEG_LOCKED_SHIFT))
#define set_eh_seg_spliting(ent1)	((ent1) | (1UL << EH_SEG_SPLITING_SHIFT))
#define set_eh_seg_migrate(ent1)	((ent1) | (1UL << EH_SEG_MIGRATE_SHIFT))



#define EH_DIR_ENTRY_SIZE	16
#define EH_DIR_ENTRY_SIZE_SHIFT	4


#define EH_DIR_EXTEND_BIT	EH_DEPTH_BITS

#define eh_dir_depth(context)	((context) & EH_DEPTH_MAX)
#define extract_eh_dir_addr(context)	((context) & ~(EH_DEPTH_MAX | (1UL << EH_DIR_EXTEND_BIT)))
#define set_eh_dir_depth(dir_ptr, depth)	((uintptr_t)(dir_ptr) | (depth))
#define set_eh_dir_extending(context)	((context) | (1UL << EH_DIR_EXTEND_BIT))
#define eh_dir_extending(context)	((context) & (1UL << EH_DIR_EXTEND_BIT))




#define EH_ENTRY_FLAG_SHIFT	3
#define EH_ENTRY_FLAG_MASK	((1UL << EH_ENTRY_FLAG_SHIFT) - 1)
#define EH_ENTRY_SPLIT_SHIFT	0

#define make_eh_new_entry(kv, fingerprint16)	(((uintptr_t)(kv)) | ((fingerprint16) << VALID_POINTER_BITS) | (1UL << EH_ENTRY_SPLIT_SHIFT))

#define EH_END_ENTRY	0xFFFFFFFFFFFFFFFF
#define eh_entry_kv_addr(entry)	((entry) & (VALID_POINTER_MASK ^ EH_ENTRY_FLAG_MASK))
#define eh_entry_fingerprint16(entry)	((entry) >> VALID_POINTER_BITS)
#define eh_entry_valid(entry)	(entry)
#define eh_entry_split(entry)	(!((entry) & (1UL << EH_ENTRY_SPLIT_SHIFT)))
#define eh_entry_end(entry)	((entry) == EH_END_ENTRY)

#define set_eh_entry_split(entry)	((entry) & ~(1UL << EH_ENTRY_SPLIT_SHIFT))
#define clean_eh_entry_split(entry)	((entry) | (1UL << EH_ENTRY_SPLIT_SHIFT))

#define compare_eh_entry_fingerprint16(entry1, entry2)	(((entry1) ^ (entry2)) >> VALID_POINTER_BITS)


#define fh_fingerprint16(hashed_key)	((hashed_key) & ((1UL << 16) - 1))


struct eh_bucket {
	EH_KV_ITEM kv[EH_PER_BUCKET_KV_NUM];
};

struct eh_segment {
	struct eh_bucket bucket[EH_BUCKET_NUM];
};


struct eh_dir_entry {
	u64 ent1;
	u64 ent2;
};



int put_kv_eh_bucket(struct kv *kv, 
					u64 hashed_key, 
					u64 fingerprint16);

int get_kv_eh_bucket(struct kv *kv, 
					u64 hashed_key, 
					u64 fingerprint16);



static inline int put_kv(struct kv *kv, u64 hashed_key) {
	u64 fingerprint16;

	fingerprint16 = fh_fingerprint16(hashed_key);

	return put_kv_eh_bucket(kv, hashed_key, fingerprint16);
}


static inline int get_kv(struct kv *kv, u64 hashed_key) {
	u64 fingerprint16;

	fingerprint16 = fh_fingerprint16(hashed_key);

	return get_kv_eh_bucket(kv, hashed_key, fingerprint16);
}


#ifdef  __cplusplus
}
#endif	//__cplusplus

#endif	//__EXT_HASH_H
