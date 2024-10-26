#include "eh_seg.h"
#include "eh_rehash.h"
#include "per_thread.h"


#define INIT_PREFETCH_CACHELINE_OF_EH_BUCKET	EH_PER_BUCKET_CACHELINE

#define half_bucket_of_eh_seg2(bucket_id)	((bucket_id) & SHIFT((EH_BUCKET_INDEX_BIT + 1) - 1))
//#define dis_to_eh_half_bucket(hashed_key, depth)	(((hashed_key) << ((depth) + 1)) >> (PREHASH_KEY_BITS - (EH_BUCKET_INDEX_BIT - 1)))

typedef u64 EH_SLOT_RECORD;

#define EH_RECORD_DEPTH_BIT_START	0
#define EH_RECORD_DEPTH_BIT_END	(EH_DEPTH_BITS - 1)

#define EH_RECORD_BUCKET_BIT_START	EH_DEPTH_BITS
#define EH_RECORD_BUCKET_BIT_END	(VALID_POINTER_BITS - 1)

#define EH_RECORD_ID_BIT_START	VALID_POINTER_BITS
#define EH_RECORD_ID_BIT_END	(EH_RECORD_ID_BIT_START + 15)

#define INVALID_EH_SLOT_RECORD	0UL

#define eh_record_bucket(record)	((record) &	\
			INTERVAL(EH_RECORD_BUCKET_BIT_START, EH_RECORD_BUCKET_BIT_END))
#define eh_record_id(record)	((record) >> EH_RECORD_ID_BIT_START)
#define eh_record_depth(record)	((record) &	\
			INTERVAL(EH_RECORD_DEPTH_BIT_START, EH_RECORD_DEPTH_BIT_END))


#define set_eh_slot_record(bucket, slot_id, depth)	((uintptr_t)(bucket) |	\
					SHIFT_OF(slot_id, EH_RECORD_ID_BIT_START) |	\
					SHIFT_OF(depth, EH_RECORD_DEPTH_BIT_START))


__attribute__((always_inline, optimize("unroll-loops")))
static void prefetch_eh_bucket_head(struct eh_bucket *bucket) {
	int i;

	for (i = 0; i < INIT_PREFETCH_CACHELINE_OF_EH_BUCKET; ++i)
		prefech_r0(((void *)bucket) + MUL_2(i, CACHE_LINE_SHIFT));
}


__attribute__((always_inline))
static struct eh_two_segment *retrieve_eh_high_segment(
							EH_BUCKET_HEADER header) {
	if (eh_seg_low(header))
		return (struct eh_two_segment *)eh_next_high_seg(header);

	return NULL;
}


__attribute__((always_inline))
static void prefetch_eh_bucket_step(
						struct eh_bucket *bucket, 
						struct eh_two_segment *next_seg, 
						int next_id, int cacheline_id) {
	int c = cacheline_id + INIT_PREFETCH_CACHELINE_OF_EH_BUCKET;
	void *addr;
	
	if (c < EH_PER_BUCKET_CACHELINE)
		addr = ((void *)bucket) + MUL_2(c, CACHE_LINE_SHIFT);
	else if (next_seg) {
		c -= EH_PER_BUCKET_CACHELINE;
		addr = ((void *)&next_seg->bucket[next_id]) + MUL_2(c, CACHE_LINE_SHIFT);
	} else
		return;

	prefech_r0(addr);
}

__attribute__((always_inline))
static void prefetch_eh_next_bucket_head(
						struct eh_two_segment *next_seg, 
						int next_id, 
						int cacheline_id) {
	void *bucket = (void *)&next_seg->bucket[next_id];
	int c = cacheline_id + INIT_PREFETCH_CACHELINE_OF_EH_BUCKET;

	c = ((c > EH_PER_BUCKET_CACHELINE) ? (c - EH_PER_BUCKET_CACHELINE) : 0);

	for (; c < INIT_PREFETCH_CACHELINE_OF_EH_BUCKET; ++c)
		prefech_r0(bucket + MUL_2(c, CACHE_LINE_SHIFT));
}

__attribute__((always_inline))
static void record_bucket_initial_traversal(EH_SLOT_RECORD *record) {
	record[0] = record[1] = INVALID_EH_SLOT_RECORD;
}

__attribute__((always_inline))
static int check_bucket_traversal(EH_SLOT_RECORD record) {
	return (record != INVALID_EH_SLOT_RECORD);
}

__attribute__((always_inline))
static void record_bucket_traversal(
					EH_SLOT_RECORD *record,
					EH_BUCKET_SLOT *s_record,
					EH_BUCKET_SLOT new_slot,
					struct eh_bucket *bucket, 
					int slot_id, int l_depth) {
	EH_SLOT_RECORD r = set_eh_slot_record(bucket, slot_id, l_depth);

	if (record[0] == INVALID_EH_SLOT_RECORD) {
		record[0] = r;
		s_record[0] = new_slot;
	} else if (record[1] == INVALID_EH_SLOT_RECORD) {
		record[1] = r;
		s_record[1] = new_slot;
	}
}

enum eh_slot_state {
	INVALID_SLOT, 
	DELETED_SLOT, 
	REPLACED_SLOT,
	UNMATCH_SLOT
};

__attribute__((always_inline))
static enum eh_slot_state compare_replace_eh_slot(
						struct kv *kv, 
						EH_BUCKET_SLOT slot_val,
						EH_BUCKET_SLOT *slot_addr,
						EH_BUCKET_SLOT new_slot) {
	EH_BUCKET_SLOT old_slot;
	struct kv *old_kv;

	while (likely(!eh_slot_invalid_deleted(slot_val))) {
		old_kv = (struct kv *)eh_slot_kv_addr(slot_val);

		if (compare_kv_key(kv, old_kv) == 0) {
			if (unlikely(eh_slot_invalid(slot_val)))
				return INVALID_SLOT;

			old_slot = cas(slot_addr, slot_val, new_slot);

			if (likely(old_slot == slot_val)) {
				reclaim_chunk(old_kv);
				return REPLACED_SLOT;
			}

			slot_val = old_slot;
			continue;
		}

		break;
	}

	return UNMATCH_SLOT;
}

__attribute__((always_inline))
static enum eh_slot_state compare_delete_eh_slot(
							struct kv *kv, 
							EH_BUCKET_SLOT slot_val,
							EH_BUCKET_SLOT *slot_addr) {
	struct kv *old_kv;
	EH_BUCKET_SLOT old_slot;

	while (!eh_slot_invalid_deleted(slot_val)) {
		old_kv = (struct kv *)eh_slot_kv_addr(slot_val);

		if (compare_kv_key(kv, old_kv) == 0) {
			if (eh_slot_deleted(slot_val))
				return DELETED_SLOT;

			if (unlikely(eh_slot_invalid(slot_val)))
				return INVALID_SLOT;

			old_slot = cas(slot_addr, slot_val, delete_eh_slot(slot_val));

			if (likely(old_slot == slot_val))
				return REPLACED_SLOT;

			slot_val = old_slot;
		}

		break;
	}

	return UNMATCH_SLOT;
}

static enum eh_slot_state find_eh_free_slot(
					EH_SLOT_RECORD *record, 
					EH_BUCKET_SLOT *s_record,
					struct kv *kv, u64 hashed_key, 
					struct eh_bucket *bucket, 
					int depth) {
	EH_BUCKET_SLOT tmp_slot;
	struct eh_two_segment *two_seg;
	EH_BUCKET_HEADER header;
	EH_BUCKET_SLOT new_slot;
	u64 fingerprint;
	enum eh_slot_state state;
	int slot_id, bucket_id;

	*record = INVALID_EH_SLOT_RECORD;

	while (1) {
		header = READ_ONCE(bucket->header);
		two_seg = retrieve_eh_high_segment(header);

		if (!two_seg || unlikely(eh_bucket_stayed(header)))
			return UNMATCH_SLOT;

		bucket_id = eh_seg2_bucket_idx(hashed_key, depth);
		bucket = &two_seg->bucket[bucket_id];
		depth += 1;

		fingerprint = hashed_key_fingerprint18(hashed_key, depth);
		new_slot = make_eh_ext_slot(fingerprint, kv);
		fingerprint >>= 2;

		for (slot_id = 0; slot_id < EH_SLOT_NUM_PER_BUCKET; ++slot_id) {
			tmp_slot = READ_ONCE(bucket->kv[slot_id]);

			if (eh_slot_free(tmp_slot)) {
				if (!check_bucket_traversal(*record)) {
					*record = set_eh_slot_record(bucket, slot_id, depth);
					*s_record = new_slot;
				}

				break;
			}

			if (unlikely(eh_slot_end(tmp_slot)))
				break;

			if (fingerprint == eh_slot_fingerprint16(tmp_slot)) {
				state = compare_replace_eh_slot(kv, tmp_slot, 
										&bucket->kv[slot_id], new_slot);

				if (state == REPLACED_SLOT)
					return REPLACED_SLOT;

				if (state == INVALID_SLOT) {
					*record = INVALID_EH_SLOT_RECORD;
					break;
				}
			}
		}
	}
}


static int __append_eh_slot(
				EH_SLOT_RECORD *record, 
				EH_BUCKET_SLOT *s_record,
				struct kv *kv, u64 hashed_key, 
				struct eh_bucket *bucket,
				int slot_id) {
	EH_BUCKET_SLOT new_slot, tmp_slot;
	struct eh_two_segment *two_seg;
	EH_BUCKET_HEADER header;
	u64 f1, f2;
	enum eh_slot_state state;
	int bucket_id, depth, i = 0;

	depth = eh_record_depth(record[0]);

	new_slot = s_record[0];

append_eh_slot_next_segment :
	f1 = eh_slot_fingerprint16(new_slot);

	for (; slot_id < EH_SLOT_NUM_PER_BUCKET; ++slot_id) {
		tmp_slot = READ_ONCE(bucket->kv[slot_id]);
		//acquire_fence();

		if (likely(eh_slot_free(tmp_slot))) {
			tmp_slot = cas(&bucket->kv[slot_id], FREE_EH_SLOT, new_slot);

			if (likely(eh_slot_free(tmp_slot)))
				return 0;
		}

		if (unlikely(eh_slot_end(tmp_slot)))
			break;

		f2 = eh_slot_fingerprint16(tmp_slot);

		if (unlikely(f1 == f2)) {
			state = compare_replace_eh_slot(kv, tmp_slot, 
									&bucket->kv[slot_id], new_slot);

			if (state == REPLACED_SLOT)
				return 0;

			if (state == INVALID_SLOT) {
				state = find_eh_free_slot(&record[1], &s_record[1], 
										kv, hashed_key, bucket, depth);

				if (state == REPLACED_SLOT)
					return 0;

				i = 0;
				break;
			}
		}
	}


	if (i == 0) {
		i = 1;

		if (likely(check_bucket_traversal(record[1]))) {
			bucket = (struct eh_bucket *)eh_record_bucket(record[1]);
			slot_id = eh_record_id(record[1]);
			depth = eh_record_depth(record[1]);
			new_slot = s_record[1];
			goto append_eh_slot_next_segment;
		}
	}

append_eh_no_free_slot :
	header = READ_ONCE(bucket->header);
	two_seg = retrieve_eh_high_segment(header);

	if (!two_seg || unlikely(eh_bucket_stayed(header))) {
		record_bucket_initial_traversal(&record[0]);
		return -1;
	}

	slot_id = 0;
	bucket_id = eh_seg2_bucket_idx(hashed_key, depth);
	bucket = &two_seg->bucket[bucket_id];
	depth += 1;

	f1 = hashed_key_fingerprint18(hashed_key, depth);
	new_slot = make_eh_ext_slot(f1, kv);

	goto append_eh_slot_next_segment;
}

__attribute__((always_inline))
static int append_eh_slot(
			EH_SLOT_RECORD *record, 
			EH_BUCKET_SLOT *s_record,
			struct kv *kv, u64 hashed_key) {
	EH_BUCKET_SLOT *slot;
	struct eh_bucket *bucket = (struct eh_bucket *)eh_record_bucket(record[0]);
	int id = eh_record_id(record[0]);

	slot = &bucket->kv[id];

	if (likely(cas_bool(slot, FREE_EH_SLOT, s_record[0])))
		return 0;

	return __append_eh_slot(record, s_record, kv, hashed_key, bucket, id);
}



int put_eh_seg_kv(
		struct eh_segment *low_seg,
		struct kv *kv, 
		u64 hashed_key,
		int l_depth) {
    struct eh_bucket *bucket, *half_bucket;
	struct eh_two_segment *seg, *next_seg;
	EH_BUCKET_HEADER header;
	EH_BUCKET_SLOT slot, new_slot;
	u64 fingerprint;
	enum eh_slot_state state;
	EH_BUCKET_SLOT s_record[2];
	EH_SLOT_RECORD record[2];
	struct eh_split_context split;
	int half_bucket_id, bucket_id, i, cl, id;

	bucket_id = eh_seg_bucket_idx(hashed_key, l_depth);
	bucket = &low_seg->bucket[bucket_id];
	prefetch_eh_bucket_head(bucket);
	half_bucket = &low_seg->bucket[0];

	record_bucket_initial_traversal(&record[0]);

	split.hashed_key = hashed_key;
	split.incomplete = 0;

put_eh_next_seg_kv :
	fingerprint = hashed_key_fingerprint18(hashed_key, l_depth);
	new_slot = make_eh_ext_slot(fingerprint, kv);
	fingerprint = fingerprint >> 2;

	bucket_id = eh_seg2_bucket_idx(hashed_key, l_depth);
	half_bucket_id = half_bucket_of_eh_seg2(bucket_id);

	i = 1;
	cl = id = 0;

	header = READ_ONCE(bucket->header);
	next_seg = retrieve_eh_high_segment(header);

	if (unlikely(eh_seg_splited(header)))
		goto put_eh_next_seg_kv_ready;

	for (; cl < EH_PER_BUCKET_CACHELINE; ++cl) {
		for (; i < EH_SLOT_NUM_PER_CACHELINE; ++i) {
			slot = READ_ONCE(bucket->kv[id]);
			//acquire_fence();

			if (unlikely(eh_slot_end(slot)))
				goto put_eh_seg_kv_no_matched;

			if (eh_slot_free(slot)) {
				record_bucket_traversal(&record[0], &s_record[0], 
							new_slot, bucket, id, l_depth);
				goto put_eh_seg_kv_no_matched;
			}

			if (fingerprint == eh_slot_fingerprint16(slot)) {
				state = compare_replace_eh_slot(kv, slot, 
							&bucket->kv[id], new_slot);

				if (state == REPLACED_SLOT)
					return 0;

				if (unlikely(state == INVALID_SLOT)) {
					record_bucket_initial_traversal(&record[0]);
					goto put_eh_seg_kv_no_matched;
				}
			}

			id += 1;
		}

		i = 0;

		prefetch_eh_bucket_step(bucket, next_seg, bucket_id, cl);
	}

put_eh_seg_kv_no_matched :
	if (!next_seg || unlikely(eh_bucket_stayed(header))) {
		if (check_bucket_traversal(record[0]) && 
				!append_eh_slot(&record[0], &s_record[0], kv, hashed_key))
			return 0;

		next_seg = add_eh_new_segment(&split, seg, bucket, kv, 1);

		if (likely(next_seg == NULL))
			return 0;

		if (unlikely((void *)next_seg == MAP_FAILED))
			return -1;

		cl = 0;
	}

put_eh_next_seg_kv_ready :
	if (cl != EH_PER_BUCKET_CACHELINE)
		prefetch_eh_next_bucket_head(next_seg, bucket_id, cl);

	seg = next_seg;
	split.target_seg = (struct eh_segment *)half_bucket;
	split.depth = l_depth++;
	bucket = &seg->bucket[bucket_id];
	half_bucket = &seg->bucket[half_bucket_id];
	goto put_eh_next_seg_kv;
}


int get_eh_seg_kv(
		struct eh_segment *low_seg,
		struct kv *kv, 
		u64 hashed_key,
		int l_depth) {
	struct eh_bucket *bucket;
	struct eh_two_segment *next_seg;
	EH_BUCKET_HEADER header;
	EH_BUCKET_SLOT slot;
	struct kv *old_kv, *migrate_kv;
	u64 fingerprint;
	int bucket_id, i, cl, id;

	bucket_id = eh_seg_bucket_idx(hashed_key, l_depth);
	bucket = &low_seg->bucket[bucket_id];
	prefetch_eh_bucket_head(bucket);

	migrate_kv = NULL;

get_eh_next_seg_kv :
	fingerprint = hashed_key_fingerprint16(hashed_key, l_depth);

	bucket_id = eh_seg2_bucket_idx(hashed_key, l_depth);

	i = 1;
	cl = id = 0;

	header = READ_ONCE(bucket->header);
	next_seg = (struct eh_two_segment *)eh_next_high_seg(header);
	//acquire_fence();

	if (unlikely(eh_seg_splited(header)))
		goto get_eh_next_seg_kv_ready;

	for (; cl < EH_PER_BUCKET_CACHELINE; ++cl) {
		for (; i < EH_SLOT_NUM_PER_CACHELINE; ++i) {
			slot = READ_ONCE(bucket->kv[id]);
			//acquire_fence();

			if (unlikely(eh_slot_end(slot)) || eh_slot_free(slot))
				goto get_eh_seg_kv_no_matched;

			if (fingerprint == eh_slot_fingerprint16(slot) 
							&& !eh_slot_invalid_deleted(slot)) {
				old_kv = (struct kv *)eh_slot_kv_addr(slot);

				if (compare_kv_key(kv, old_kv) == 0) {
					if (eh_slot_deleted(slot))
						return -1;

					if (unlikely(eh_slot_invalid(slot))) {
						migrate_kv = old_kv;
						goto get_eh_seg_kv_no_matched;
					}

					copy_kv_val(kv, old_kv);
					return 0;
				}
			}

			id += 1;
		}

		i = 0;

		prefetch_eh_bucket_step(bucket, next_seg, bucket_id, cl);
	}

get_eh_seg_kv_no_matched :
	if (!next_seg || unlikely(eh_bucket_stayed(header))) {
		if (unlikely(migrate_kv)) {
			copy_kv_val(kv, migrate_kv);
			return 0;
		}

		return -1;
	}

get_eh_next_seg_kv_ready :
	if (cl != EH_PER_BUCKET_CACHELINE)
		prefetch_eh_next_bucket_head(next_seg, bucket_id, cl);

	l_depth += 1;
	bucket = &next_seg->bucket[bucket_id];
	
	goto get_eh_next_seg_kv;
}

int delete_eh_seg_kv(
		struct eh_segment *low_seg,
		struct kv *kv, 
		u64 hashed_key,
		int l_depth) {
	struct eh_bucket *bucket;
	struct eh_two_segment *next_seg;
	struct eh_four_segment *next_next_seg;
	EH_BUCKET_HEADER header;
	EH_BUCKET_SLOT slot;
	enum eh_slot_state state;
	u64 fingerprint;
	int bucket_id, i, cl, id, migrated;

	bucket_id = eh_seg_bucket_idx(hashed_key, l_depth);
	bucket = &low_seg->bucket[bucket_id];
	prefetch_eh_bucket_head(bucket);

	migrated = 0;

delete_eh_next_seg_kv :
	fingerprint = hashed_key_fingerprint16(hashed_key, l_depth);

	bucket_id = eh_seg2_bucket_idx(hashed_key, l_depth);

	i = 1;
	cl = id = 0;

	header = READ_ONCE(bucket->header);
	next_seg = (struct eh_two_segment *)eh_next_high_seg(header);
	//acquire_fence();

	if (unlikely(eh_seg_splited(header)))
		goto delete_eh_next_seg_kv_ready;

	for (; cl < EH_PER_BUCKET_CACHELINE; ++cl) {
		for (; i < EH_SLOT_NUM_PER_CACHELINE; ++i) {
			slot = READ_ONCE(bucket->kv[id]);
			//acquire_fence();

			if (unlikely(eh_slot_end(slot)) || eh_slot_free(slot))
				goto delete_eh_seg_kv_no_matched;

			if (fingerprint == eh_slot_fingerprint16(slot)) {
				state = compare_delete_eh_slot(kv, slot, &bucket->kv[id]);

				if (state == REPLACED_SLOT)
					return 0;

				if (state == DELETED_SLOT)
					return -1;

				if (unlikely(state == INVALID_SLOT)) {
					migrated = 1;
					goto delete_eh_seg_kv_no_matched;
				}
			}

			id += 1;
		}

		i = 0;

		prefetch_eh_bucket_step(bucket, next_seg, bucket_id, cl);
	}

delete_eh_seg_kv_no_matched :
	if (!next_seg || unlikely(eh_bucket_stayed(header))) {
		if (likely(migrated == 0))
			return -1;
		
		header = READ_ONCE(bucket->header);
		next_seg = (struct eh_two_segment *)eh_next_high_seg(header);
	}

	if (unlikely(migrated == 1)) {
		int seg2_id = eh_seg2_id_in_seg4(hashed_key, l_depth);

		header = READ_ONCE(next_seg->bucket[0].header);
		next_next_seg = (struct eh_four_segment *)eh_next_high_seg(header);

		bucket = &next_seg->bucket[bucket_id];
		next_seg = &next_next_seg->two_seg[seg2_id];

		bucket_id = eh_seg2_bucket_idx(hashed_key, ++l_depth);
							
		cl = 0;
		migrated = 0;
	}

delete_eh_next_seg_kv_ready :
	if (cl != EH_PER_BUCKET_CACHELINE)
		prefetch_eh_next_bucket_head(next_seg, bucket_id, cl);

	l_depth += 1;
	bucket = &next_seg->bucket[bucket_id];
	
	goto delete_eh_next_seg_kv;
}
