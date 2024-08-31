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
static void prefetch_eh_bucket_head(
					struct eh_bucket *bucket, 
					struct eh_segment *low_seg) {
	int i;

	for (i = 0; i < INIT_PREFETCH_CACHELINE_OF_EH_BUCKET; ++i)
		prefech_r0(((void *)bucket) + (i << CACHE_LINE_SHIFT));

	prefech_r0(&low_seg->bucket[0]);
}

__attribute__((always_inline))
static struct eh_two_segment *__retrieve_eh_high_segment(
							struct eh_bucket *bucket, 
							struct eh_bucket *half_bucket,
							EH_BUCKET_HEADER header) {
	EH_BUCKET_HEADER tmp_header;

	if (bucket != half_bucket) {
		tmp_header = READ_ONCE(half_bucket->header);

		if (eh_seg_low(tmp_header)) {
			struct eh_two_segment *seg = (struct eh_two_segment *)
											eh_next_high_seg(tmp_header);
			cas(&bucket->header, header, set_eh_seg_low(seg));
			return seg;
		}
	}

	return NULL;
}

__attribute__((always_inline))
static struct eh_two_segment *retrieve_eh_high_segment(
							struct eh_bucket *bucket, 
							struct eh_bucket *half_bucket,
							EH_BUCKET_HEADER header) {
	if (eh_seg_low(header))
		return (struct eh_two_segment *)eh_next_high_seg(header);

	return __retrieve_eh_high_segment(bucket, half_bucket, header);
}


__attribute__((always_inline))
static void prefetch_eh_bucket_step(
						struct eh_bucket *bucket, 
						struct eh_two_segment *next_seg, 
						int next_id, int next_half_id,
						int cacheline_id) {
	int c = cacheline_id + INIT_PREFETCH_CACHELINE_OF_EH_BUCKET;
	void *addr;
	
	if (c < EH_PER_BUCKET_CACHELINE)
		addr = ((void *)bucket) + (c << CACHE_LINE_SHIFT);
	else if (next_seg) {
		c -= EH_PER_BUCKET_CACHELINE;
		addr = ((void *)&next_seg->bucket[next_id]) + (c << CACHE_LINE_SHIFT);

		if (c == 0)
			prefech_r0(&next_seg->bucket[next_half_id]);
	} else
		return;

	prefech_r0(addr);
}

__attribute__((always_inline))
static void prefetch_eh_next_bucket_head(
						struct eh_two_segment *next_seg, 
						int next_id, 
						int next_half_id,
						int cacheline_id) {
	void *bucket = (void *)&next_seg->bucket[next_id];
	int c = cacheline_id + INIT_PREFETCH_CACHELINE_OF_EH_BUCKET;

	if (c > EH_PER_BUCKET_CACHELINE)
		c -= EH_PER_BUCKET_CACHELINE;
	else {
		prefech_r0(&next_seg->bucket[next_half_id]);
		c = 0;
	}

	for (; c < INIT_PREFETCH_CACHELINE_OF_EH_BUCKET; ++c)
		prefech_r0(bucket + (c << CACHE_LINE_SHIFT));
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


static EH_SLOT_RECORD find_eh_free_slot(
				struct kv *kv, u64 hashed_key, 
				struct eh_bucket *bucket, 
				int slot_id, int depth) {
	EH_BUCKET_SLOT tmp_slot;
	struct eh_bucket *half_bucket;
	struct eh_two_segment *two_seg;
	EH_BUCKET_HEADER header;
	EH_SLOT_RECORD record = INVALID_EH_SLOT_RECORD;
	struct kv *tmp_kv;
	u64 fingerprint;
	int bucket_id;

re_find_eh_free_slot :
	fingerprint = hashed_key_fingerprint16(hashed_key, depth);

	for (; slot_id < EH_SLOT_NUM_PER_BUCKET; ++slot_id) {
		tmp_slot = READ_ONCE(bucket->kv[slot_id]);
		//acquire_fence();

		if (eh_slot_free(tmp_slot)) {
			if (!check_bucket_traversal(record))
				record = set_eh_slot_record(bucket, slot_id, depth);
			break;
		}

		if (unlikely(eh_slot_end(tmp_slot)))
			break;

		if (fingerprint == eh_slot_fingerprint16(tmp_slot) 
									&& !eh_slot_invalid(tmp_slot)) {
			tmp_kv = (struct kv *)eh_slot_kv_addr(tmp_slot);

			if (compare_kv_key(kv, tmp_kv) == 0)
				return set_eh_slot_record(bucket, slot_id, depth);
		}
	}

	bucket_id = eh_seg2_bucket_idx(hashed_key, depth);
	half_bucket = &bucket[-DIV_2(bucket_id, 1)];

	header = READ_ONCE(bucket->header);

	two_seg = retrieve_eh_high_segment(bucket, half_bucket, header);

	if (!two_seg) {
		if (!check_bucket_traversal(record))
			record = set_eh_slot_record(bucket, EH_SLOT_NUM_PER_BUCKET - 1, depth);
		return record;
	}

	slot_id = 0;
	bucket = &two_seg->bucket[bucket_id];
	depth += 1;

	goto re_find_eh_free_slot;
}


static int __append_eh_slot(
					EH_SLOT_RECORD *record, 
					EH_BUCKET_SLOT *s_record,
					struct kv *kv, u64 hashed_key, 
					struct eh_bucket *bucket,
					int slot_id) {
	EH_BUCKET_SLOT new_slot, tmp_slot, old_slot;
	struct eh_bucket *half_bucket;
	struct eh_two_segment *two_seg;
	EH_BUCKET_HEADER header;
	struct kv *tmp_kv;
	u64 f1, f2;
	EH_SLOT_RECORD rec;
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
			if (unlikely(eh_slot_invalid_deleted(tmp_slot)))
				continue;

			tmp_kv = (struct kv *)eh_slot_kv_addr(tmp_slot);

			if (compare_kv_key(kv, tmp_kv) == 0)
				goto append_eh_seg_matched_kv;
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

	bucket_id = eh_seg2_bucket_idx(hashed_key, depth);
	half_bucket = &bucket[-DIV_2(bucket_id, 1)];

	header = READ_ONCE(bucket->header);
	two_seg = retrieve_eh_high_segment(bucket, half_bucket, header);

	if (!two_seg) {
		record_bucket_initial_traversal(&record[0]);
		return -1;
	}

	slot_id = 0;
	bucket = &two_seg->bucket[bucket_id];
	depth += 1;

	f1 = hashed_key_fingerprint18(hashed_key, depth);
	new_slot = make_eh_ext_slot(f1, kv);

	goto append_eh_slot_next_segment;

append_eh_seg_matched_kv :
	if (unlikely(eh_slot_invalid(tmp_slot))) {
		record_bucket_initial_traversal(&record[0]);
		rec = find_eh_free_slot(kv, hashed_key, bucket, slot_id, depth);

		bucket = (struct eh_bucket *)eh_record_bucket(rec);
		slot_id = eh_record_id(rec);
		depth = eh_record_depth(rec);

		f1 = hashed_key_fingerprint18(hashed_key, depth);
		new_slot = make_eh_ext_slot(f1, kv);

		goto append_eh_slot_next_segment;
	}

	old_slot = cas(&bucket->kv[slot_id], tmp_slot, new_slot);

	if (likely(old_slot == tmp_slot)) {
		reclaim_chunk(tmp_kv);
		return 0;
	}

	tmp_kv = (struct kv *)eh_slot_kv_addr(old_slot);
	tmp_slot = old_slot;

	goto append_eh_seg_matched_kv;
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
	EH_BUCKET_SLOT slot, new_slot, old_slot;
	struct kv *old_kv;
	u64 fingerprint;
	EH_BUCKET_SLOT s_record[2];
	EH_SLOT_RECORD record[2];
	struct eh_split_context split;
	int half_bucket_id, bucket_id, i, cl, id;

	bucket_id = eh_seg_bucket_idx(hashed_key, l_depth);
	bucket = &low_seg->bucket[bucket_id];
	prefetch_eh_bucket_head(bucket, low_seg);
	half_bucket = &low_seg->bucket[0];

	record_bucket_initial_traversal(&record[0]);

	split.hashed_key = hashed_key;
	split.incomplete = 0;

put_eh_next_seg_kv :
	fingerprint = hashed_key_fingerprint18(hashed_key, l_depth);
	new_slot = make_eh_ext_slot(fingerprint, kv);
	fingerprint = DIV_2(fingerprint, 2);

	bucket_id = eh_seg2_bucket_idx(hashed_key, l_depth);
	half_bucket_id = half_bucket_of_eh_seg2(bucket_id);

	i = 1;
	cl = id = 0;

	header = READ_ONCE(bucket->header);

	if (unlikely(eh_seg_splited(header))) {
		next_seg = (struct eh_two_segment *)eh_next_high_seg(header);
		goto put_eh_next_seg_kv_ready;
	}

	for (; cl < EH_PER_BUCKET_CACHELINE; ++cl) {
		for (; i < EH_SLOT_NUM_PER_CACHELINE; ++i) {
			slot = READ_ONCE(bucket->kv[id]);
			//acquire_fence();

			if (unlikely(eh_slot_end(slot))) {
				header = READ_ONCE(half_bucket->header);
				next_seg = (struct eh_two_segment *)eh_next_high_seg(header);
				goto put_eh_next_seg_kv_ready;
			}

			if (eh_slot_free(slot))
				goto put_eh_seg_kv_free_slot;

			if (fingerprint == eh_slot_fingerprint16(slot) 
									&& !eh_slot_invalid(slot)) {
				old_kv = (struct kv *)eh_slot_kv_addr(slot);

				if (compare_kv_key(kv, old_kv) == 0) {
				put_eh_seg_matched_kv :
					old_slot = cas(&bucket->kv[id], slot, new_slot);

					if (likely(old_slot == slot)) {
						reclaim_chunk(old_kv);
						return 0;
					}

					if (likely(!eh_slot_invalid(old_slot))) {
						old_kv = (struct kv *)eh_slot_kv_addr(old_slot);
						slot = old_slot;
						goto put_eh_seg_matched_kv;
					}
					//record[0] = record[1] = INVALID_EH_SLOT_RECORD;
				}
			}

			id += 1;
		}

		i = 0;

		if (cl == 0)
			next_seg = retrieve_eh_high_segment(bucket, half_bucket, header);

		prefetch_eh_bucket_step(bucket, next_seg, bucket_id, half_bucket_id, cl);
	}

	if (!next_seg) {
		if (unlikely(check_bucket_traversal(record[0])))
			goto put_eh_seg_kv_append_slot;

	put_eh_full_seg_kv :
		split.recheck = (split.target_seg != low_seg);
		next_seg = add_eh_new_segment(&split, seg, kv, 1);

		if (likely(next_seg == NULL))
			return 0;

		if (unlikely((void *)next_seg == MAP_FAILED))
			return -1;

		if (next_seg == &split.dest_seg->two_seg[1])
			cas(&half_bucket->header, INITIAL_EH_BUCKET_HEADER, set_eh_seg_low(next_seg));

		cl = 0;
	}

put_eh_next_seg_kv_ready :
	if (cl != EH_PER_BUCKET_CACHELINE)
		prefetch_eh_next_bucket_head(next_seg, bucket_id, half_bucket_id, cl);

	seg = next_seg;
	split.target_seg = (struct eh_segment *)half_bucket;
	split.depth = l_depth++;
	bucket = &seg->bucket[bucket_id];
	half_bucket = &seg->bucket[half_bucket_id];
	goto put_eh_next_seg_kv;

put_eh_seg_kv_free_slot :
	record_bucket_traversal(&record[0], &s_record[0], 
								new_slot, bucket, id, l_depth);

	if (cl == 0)
		next_seg = retrieve_eh_high_segment(bucket, half_bucket, header);
//printf("%x\n", (void *)next_seg);
	if (next_seg)
		goto put_eh_next_seg_kv_ready;

put_eh_seg_kv_append_slot :
	if (unlikely(append_eh_slot(&record[0], &s_record[0], kv, hashed_key)))
		goto put_eh_full_seg_kv;

	return 0;
}


int get_eh_seg_kv(
		struct eh_segment *low_seg,
		struct kv *kv, 
		u64 hashed_key,
		int l_depth) {
	struct eh_bucket *bucket, *half_bucket;
	struct eh_two_segment *next_seg;
	EH_BUCKET_HEADER header;
	EH_BUCKET_SLOT slot;
	struct kv *old_kv;
	u64 fingerprint;
	int half_bucket_id, bucket_id, i, cl, id;

	bucket_id = eh_seg_bucket_idx(hashed_key, l_depth);
	bucket = &low_seg->bucket[bucket_id];
	prefetch_eh_bucket_head(bucket, low_seg);
	half_bucket = &low_seg->bucket[0];

get_eh_next_seg_kv :
	fingerprint = hashed_key_fingerprint16(hashed_key, l_depth);

	bucket_id = eh_seg2_bucket_idx(hashed_key, l_depth);
	half_bucket_id = half_bucket_of_eh_seg2(bucket_id);

	i = 1;
	cl = id = 0;

	header = READ_ONCE(bucket->header);
	//acquire_fence();

	if (unlikely(eh_seg_splited(header))) {
		next_seg = (struct eh_two_segment *)eh_next_high_seg(header);
		goto get_eh_next_seg_kv_ready;
	}

	for (; cl < EH_PER_BUCKET_CACHELINE; ++cl) {
		for (; i < EH_SLOT_NUM_PER_CACHELINE; ++i) {
			slot = READ_ONCE(bucket->kv[id]);
			//acquire_fence();

			if (unlikely(eh_slot_end(slot))) {
				header = READ_ONCE(half_bucket->header);
				next_seg = (struct eh_two_segment *)eh_next_high_seg(header);
				goto get_eh_next_seg_kv_ready;
			}

			if (eh_slot_free(slot))
				goto get_eh_seg_kv_free_slot;

			if (fingerprint == eh_slot_fingerprint16(slot) 
									&& !eh_slot_invalid(slot)) {
				old_kv = (struct kv *)eh_slot_kv_addr(slot);

				if (compare_kv_key(kv, old_kv) == 0) {
					if (eh_slot_deleted(slot))
						return -1;
					copy_kv_val(kv, old_kv);
					return 0;
				}
			}

			id += 1;
		}

		i = 0;

		if (cl == 0)
			next_seg = retrieve_eh_high_segment(bucket, half_bucket, header);

		prefetch_eh_bucket_step(bucket, next_seg, bucket_id, half_bucket_id, cl);
	}

get_eh_seg_kv_free_slot :
	if (cl == 0)
		next_seg = retrieve_eh_high_segment(bucket, half_bucket, header);

	if (!next_seg)
		return -1;

get_eh_next_seg_kv_ready :
	if (cl != EH_PER_BUCKET_CACHELINE)
		prefetch_eh_next_bucket_head(next_seg, bucket_id, half_bucket_id, cl);

	l_depth += 1;
	bucket = &next_seg->bucket[bucket_id];
	half_bucket = &next_seg->bucket[half_bucket_id];
	
	goto get_eh_next_seg_kv;
}

int delete_eh_seg_kv(
		struct eh_segment *low_seg,
		struct kv *kv, 
		u64 hashed_key,
		int l_depth) {
	struct eh_bucket *bucket, *half_bucket;
	struct eh_two_segment *next_seg;
	EH_BUCKET_HEADER header;
	EH_BUCKET_SLOT slot;
	struct kv *old_kv;
	u64 fingerprint;
	int half_bucket_id, bucket_id, i, cl, id;

	bucket_id = eh_seg_bucket_idx(hashed_key, l_depth);
	bucket = &low_seg->bucket[bucket_id];
	prefetch_eh_bucket_head(bucket, low_seg);
	half_bucket = &low_seg->bucket[0];

delete_eh_next_seg_kv :
	fingerprint = hashed_key_fingerprint16(hashed_key, l_depth);

	bucket_id = eh_seg2_bucket_idx(hashed_key, l_depth);
	half_bucket_id = half_bucket_of_eh_seg2(bucket_id);

	i = 1;
	cl = id = 0;

	header = READ_ONCE(bucket->header);
	//acquire_fence();

	if (unlikely(eh_seg_splited(header))) {
		next_seg = (struct eh_two_segment *)eh_next_high_seg(header);
		goto delete_eh_next_seg_kv_ready;
	}

	for (; cl < EH_PER_BUCKET_CACHELINE; ++cl) {
		for (; i < EH_SLOT_NUM_PER_CACHELINE; ++i) {
			slot = READ_ONCE(bucket->kv[id]);
			//acquire_fence();

			if (unlikely(eh_slot_end(slot))) {
				header = READ_ONCE(half_bucket->header);
				next_seg = (struct eh_two_segment *)eh_next_high_seg(header);
				goto delete_eh_next_seg_kv_ready;
			}

			if (eh_slot_free(slot))
				goto delete_eh_seg_kv_free_slot;

			if (fingerprint == eh_slot_fingerprint16(slot) 
									&& !eh_slot_invalid(slot)) {
				old_kv = (struct kv *)eh_slot_kv_addr(slot);

				if (compare_kv_key(kv, old_kv) == 0)
					goto delete_eh_seg_matched_kv;
			}

			id += 1;
		}

		i = 0;

		if (cl == 0)
			next_seg = retrieve_eh_high_segment(bucket, half_bucket, header);

		prefetch_eh_bucket_step(bucket, next_seg, bucket_id, half_bucket_id, cl);
	}

delete_eh_seg_kv_free_slot :
	if (cl == 0)
		next_seg = retrieve_eh_high_segment(bucket, half_bucket, header);

	if (!next_seg)
		return -1;

delete_eh_next_seg_kv_ready :
	if (cl != EH_PER_BUCKET_CACHELINE)
		prefetch_eh_next_bucket_head(next_seg, bucket_id, half_bucket_id, cl);

	l_depth += 1;
	bucket = &next_seg->bucket[bucket_id];
	half_bucket = &next_seg->bucket[half_bucket_id];
	
	goto delete_eh_next_seg_kv;

delete_eh_seg_matched_kv :
	while (!eh_slot_deleted(slot)) {
		EH_BUCKET_SLOT tmp_slot = cas(&bucket->kv[id], slot, delete_eh_slot(slot));

		if (likely(tmp_slot == slot))
			return 0;

		slot = tmp_slot;
	}
					
	return -1;
}