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

#define EH_RECORD_LAYER_BIT_START	VALID_POINTER_BITS
#define EH_RECORD_LAYER_BIT_END	(VALID_POINTER_BITS + 5)

#define EH_RECORD_ID_BIT_START	(VALID_POINTER_BITS + 6)
#define EH_RECORD_ID_BIT_END	(VALID_POINTER_BITS + 15)

#define EH_RECORD_BUCKET_MASK	\
			INTERVAL(EH_RECORD_BUCKET_BIT_START, EH_RECORD_BUCKET_BIT_END)

#define INVALID_EH_SLOT_RECORD	0UL

#define eh_record_bucket(record)	((record) &	EH_RECORD_BUCKET_MASK)
#define eh_record_id(record)	\
			INTERVAL_OF(record, EH_RECORD_ID_BIT_START, EH_RECORD_ID_BIT_END)
#define eh_record_depth(record)	\
		INTERVAL_OF(record, EH_RECORD_DEPTH_BIT_START, EH_RECORD_DEPTH_BIT_END)
#define eh_record_layer(record)	\
		INTERVAL_OF(record, EH_RECORD_LAYER_BIT_START, EH_RECORD_LAYER_BIT_END)


#define set_eh_slot_record(bucket, slot_id, depth, layer)	\
				((uintptr_t)(bucket) |	\
				SHIFT_OF(layer, EH_RECORD_LAYER_BIT_START) |	\
				SHIFT_OF(slot_id, EH_RECORD_ID_BIT_START) |	\
				SHIFT_OF(depth, EH_RECORD_DEPTH_BIT_START))

struct eh_slot_record_pair {
	EH_SLOT_RECORD record[2];
	EH_BUCKET_SLOT slot[2];
};


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
static struct eh_two_segment *retrieve_eh_higher_segment(
						struct eh_bucket *bucket,
						struct eh_two_segment *next_seg, 
						u64 hashed_key, int depth) {
	EH_BUCKET_HEADER header;
	struct eh_four_segment *next_next_seg;
	int seg2_id;

	if (unlikely(next_seg == NULL)) {
		header = READ_ONCE(bucket->header);
		next_seg = (struct eh_two_segment *)eh_next_high_seg(header);
	}

	seg2_id = eh_seg2_id_in_seg4(hashed_key, depth);

	header = READ_ONCE(next_seg->bucket[0].header);
	next_next_seg = (struct eh_four_segment *)eh_next_high_seg(header);

	return &next_next_seg->two_seg[seg2_id];
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
static void record_bucket_initial_traversal(struct eh_slot_record_pair *rec_pair) {
	rec_pair->record[0] = rec_pair->record[1] = INVALID_EH_SLOT_RECORD;
}

__attribute__((always_inline))
static int check_bucket_traversal(EH_SLOT_RECORD record) {
	return (record != INVALID_EH_SLOT_RECORD);
}

__attribute__((always_inline))
static void record_bucket_traversal(
			struct eh_slot_record_pair *rec_pair, 
			EH_BUCKET_SLOT new_slot,
			struct eh_bucket *bucket, 
			int slot_id, int l_depth, 
			int layer) {
	EH_SLOT_RECORD r = set_eh_slot_record(bucket, slot_id, l_depth, layer);

	if (!check_bucket_traversal(rec_pair->record[0])) {
		rec_pair->record[0] = r;
		rec_pair->slot[0] = new_slot;
	} else if (!check_bucket_traversal(rec_pair->record[1])) {
		rec_pair->record[1] = r;
		rec_pair->slot[1] = new_slot;
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
				struct eh_slot_record_pair *rec_pair, 
				struct kv *kv, u64 hashed_key, 
				struct eh_bucket *bucket, 
				int depth, int layer) {
	EH_BUCKET_SLOT tmp_slot;
	struct eh_two_segment *two_seg;
	EH_BUCKET_HEADER header;
	EH_BUCKET_SLOT new_slot;
	u64 fingerprint;
	enum eh_slot_state state;
	int slot_id, bucket_id;

	rec_pair->record[1] = INVALID_EH_SLOT_RECORD;

	while (1) {
		header = READ_ONCE(bucket->header);
		two_seg = retrieve_eh_high_segment(header);

		if (!two_seg || unlikely(eh_bucket_stayed(header)))
			return UNMATCH_SLOT;

		bucket_id = eh_seg2_bucket_idx(hashed_key, depth);
		bucket = &two_seg->bucket[bucket_id];
		depth += 1;
		layer += 1;

		fingerprint = hashed_key_fingerprint18(hashed_key, depth);
		new_slot = make_eh_ext_slot(fingerprint, kv);
		fingerprint >>= 2;

		for (slot_id = 0; slot_id < EH_SLOT_NUM_PER_BUCKET; ++slot_id) {
			tmp_slot = READ_ONCE(bucket->kv[slot_id]);

			if (eh_slot_free(tmp_slot)) {
				if (!check_bucket_traversal(rec_pair->record[1])) {
					rec_pair->record[1] = set_eh_slot_record(
											bucket, slot_id, depth, layer);
					rec_pair->slot[1] = new_slot;
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
					rec_pair->record[1] = INVALID_EH_SLOT_RECORD;
					break;
				}
			}
		}
	}
}


static int __append_eh_slot(
			struct eh_slot_record_pair *rec_pair, 
			struct kv *kv, u64 hashed_key, 
			struct eh_bucket *bucket,
			int slot_id, int seg_op_node) {
	EH_BUCKET_SLOT new_slot, tmp_slot;
	struct eh_two_segment *two_seg;
	EH_BUCKET_HEADER header;
	u64 f1, f2;
	enum eh_slot_state state;
	int bucket_id, depth, layer, i = 0;

	depth = eh_record_depth(rec_pair->record[0]);
	layer = eh_record_layer(rec_pair->record[0]);

	new_slot = rec_pair->slot[0];

append_eh_slot_next_segment :
	f1 = eh_slot_fingerprint16(new_slot);

	for (; slot_id < EH_SLOT_NUM_PER_BUCKET; ++slot_id) {
		tmp_slot = READ_ONCE(bucket->kv[slot_id]);
		//acquire_fence();

		if (likely(eh_slot_free(tmp_slot))) {
			tmp_slot = cas(&bucket->kv[slot_id], FREE_EH_SLOT, new_slot);

			if (likely(eh_slot_free(tmp_slot))) {
				if (seg_op_node != tls_node_id())
					return 0;

				return EH_SLOT_NUM_PER_BUCKET * layer + slot_id;
			}
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
				state = find_eh_free_slot(rec_pair, kv, 
										hashed_key, bucket, depth, layer);

				if (state == REPLACED_SLOT)
					return 0;

				i = 0;
				break;
			}
		}
	}


	if (i == 0) {
		i = 1;

		if (likely(check_bucket_traversal(rec_pair->record[1]))) {
			bucket = (struct eh_bucket *)eh_record_bucket(rec_pair->record[1]);
			slot_id = eh_record_id(rec_pair->record[1]);
			depth = eh_record_depth(rec_pair->record[1]);
			layer = eh_record_layer(rec_pair->record[1]);
			new_slot = rec_pair->slot[1];
			goto append_eh_slot_next_segment;
		}
	}

	header = READ_ONCE(bucket->header);
	two_seg = retrieve_eh_high_segment(header);

	if (!two_seg || unlikely(eh_bucket_stayed(header))) {
		record_bucket_initial_traversal(rec_pair);
		return -1;
	}

	slot_id = 0;
	bucket_id = eh_seg2_bucket_idx(hashed_key, depth);
	bucket = &two_seg->bucket[bucket_id];
	depth += 1;
	layer += 1;

	f1 = hashed_key_fingerprint18(hashed_key, depth);
	new_slot = make_eh_ext_slot(f1, kv);

	goto append_eh_slot_next_segment;
}

__attribute__((always_inline))
static int append_eh_slot(
		struct eh_slot_record_pair *rec_pair, 
		struct kv *kv, u64 hashed_key, 
		int seg_op_node) {
	EH_BUCKET_SLOT *slot;
	struct eh_bucket *bucket;
	int id, layer;

	bucket = (struct eh_bucket *)eh_record_bucket(rec_pair->record[0]);
	id = eh_record_id(rec_pair->record[0]);

	slot = &bucket->kv[id];

	if (likely(cas_bool(slot, FREE_EH_SLOT, rec_pair->slot[0]))) {
		if (seg_op_node != tls_node_id())
			return 0;

		layer = eh_record_layer(rec_pair->record[0]);
		return EH_SLOT_NUM_PER_BUCKET * layer + id;
	}

	return __append_eh_slot(rec_pair, kv, hashed_key, bucket, id, seg_op_node);
}

__attribute__((always_inline))
static int check_append_eh_init_slot(
			struct eh_bucket *bucket, 
			EH_BUCKET_SLOT new_slot, 
			int init) {
	if (init && likely(cas_bool(&bucket->kv[0], FREE_EH_SLOT, new_slot)))
		return 1;

	return 0;
}

int put_eh_seg_kv(
		struct eh_segment *low_seg,
		struct kv *kv, u64 hashed_key,
		int l_depth, int seg_op_node) {
    struct eh_bucket *bucket, *half_bucket;
	struct eh_two_segment *seg, *next_seg;
	EH_BUCKET_HEADER header;
	EH_BUCKET_SLOT slot, new_slot;
	u64 fingerprint;
	struct eh_slot_record_pair rec_pair;
	struct eh_split_context split;
	enum eh_slot_state state;
	int init_append, adv_split, half_bucket_id, bucket_id, layer, i, cl, id;

	bucket_id = eh_seg_bucket_idx(hashed_key, l_depth);
	bucket = &low_seg->bucket[bucket_id];
	prefetch_eh_bucket_head(bucket);
	half_bucket = &low_seg->bucket[0];

	record_bucket_initial_traversal(&rec_pair);

	adv_split = tls_adv_split_count();

	split.hashed_key = hashed_key;
	split.incomplete = 0;

	layer = 0;
	init_append = 0;

put_eh_next_seg_kv :
	fingerprint = hashed_key_fingerprint18(hashed_key, l_depth);
	new_slot = make_eh_ext_slot(fingerprint, kv);
	fingerprint = fingerprint >> 2;

	init_append = check_append_eh_init_slot(bucket, new_slot, init_append);

	if (init_append == 1)
		return 0;

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
				record_bucket_traversal(&rec_pair, new_slot, 
											bucket, id, l_depth, layer);
				goto put_eh_seg_kv_no_matched;
			}

			if (fingerprint == eh_slot_fingerprint16(slot)) {
				state = compare_replace_eh_slot(kv, slot, 
							&bucket->kv[id], new_slot);

				if (state == REPLACED_SLOT)
					return 0;

				if (unlikely(state == INVALID_SLOT)) {
					record_bucket_initial_traversal(&rec_pair);
					next_seg = retrieve_eh_higher_segment(bucket, next_seg, 
										hashed_key, l_depth);
					bucket_id = eh_seg2_bucket_idx(hashed_key, ++l_depth);
					layer += 1;
					cl = 0;
					goto put_eh_next_seg_kv_ready;
				}
			}

			id += 1;
		}

		i = 0;

		if (!eh_bucket_stayed(header))
			prefetch_eh_bucket_step(bucket, next_seg, bucket_id, cl);
	}

put_eh_seg_kv_no_matched :
	if (!next_seg || unlikely(eh_bucket_stayed(header))) {
		if (check_bucket_traversal(rec_pair.record[0])) {
			id = append_eh_slot(&rec_pair, kv, hashed_key, seg_op_node);

			if (likely(id >= 0)) {
				if (next_seg || likely(id <= adv_split) || layer > 1)
					return 0;

				kv = NULL;
			}
		}

		if (next_seg)
			WRITE_ONCE(bucket->header, clean_eh_bucket_stayed(header));
		else {
			next_seg = add_eh_new_segment(&split, seg, bucket, kv, 1, seg_op_node);

			if (likely(next_seg == NULL))
				return 0;

			if (unlikely((void *)next_seg == MAP_FAILED))
				return -1;

			//cl = 0;
		}
		
		init_append = 1;
	}

put_eh_next_seg_kv_ready :
	layer += 1;
	adv_split += EH_SLOT_NUM_PER_BUCKET;

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
		struct kv *kv, u64 hashed_key,
		int l_depth, int seg_op_node) {
	struct eh_bucket *bucket;
	struct eh_two_segment *next_seg;
	EH_BUCKET_HEADER header;
	EH_BUCKET_SLOT slot;
	struct kv *old_kv;
	u64 fingerprint;
	int bucket_id, i, cl, id;

	bucket_id = eh_seg_bucket_idx(hashed_key, l_depth);
	bucket = &low_seg->bucket[bucket_id];
	prefetch_eh_bucket_head(bucket);

get_eh_next_seg_kv :
	fingerprint = hashed_key_fingerprint16(hashed_key, l_depth);

	bucket_id = eh_seg2_bucket_idx(hashed_key, l_depth);

	i = 1;
	cl = id = 0;

	header = READ_ONCE(bucket->header);
	next_seg = retrieve_eh_high_segment(header);
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
						next_seg = retrieve_eh_higher_segment(bucket, next_seg, 
										hashed_key, l_depth);
						bucket_id = eh_seg2_bucket_idx(hashed_key, ++l_depth);
						cl = 0;
						goto get_eh_next_seg_kv_ready;
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
	if (!next_seg || unlikely(eh_bucket_stayed(header)))
		return -1;

get_eh_next_seg_kv_ready :
	if (cl != EH_PER_BUCKET_CACHELINE)
		prefetch_eh_next_bucket_head(next_seg, bucket_id, cl);

	l_depth += 1;
	bucket = &next_seg->bucket[bucket_id];
	
	goto get_eh_next_seg_kv;
}

int delete_eh_seg_kv(
		struct eh_segment *low_seg,
		struct kv *kv, u64 hashed_key,
		int l_depth, int seg_op_node) {
	struct eh_bucket *bucket;
	struct eh_two_segment *next_seg;
	EH_BUCKET_HEADER header;
	EH_BUCKET_SLOT slot;
	enum eh_slot_state state;
	u64 fingerprint;
	int bucket_id, i, cl, id;

	bucket_id = eh_seg_bucket_idx(hashed_key, l_depth);
	bucket = &low_seg->bucket[bucket_id];
	prefetch_eh_bucket_head(bucket);

delete_eh_next_seg_kv :
	fingerprint = hashed_key_fingerprint16(hashed_key, l_depth);

	bucket_id = eh_seg2_bucket_idx(hashed_key, l_depth);

	i = 1;
	cl = id = 0;

	header = READ_ONCE(bucket->header);
	next_seg = retrieve_eh_high_segment(header);
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
					next_seg = retrieve_eh_higher_segment(bucket, next_seg, 
										hashed_key, l_depth);
					bucket_id = eh_seg2_bucket_idx(hashed_key, ++l_depth);
					cl = 0;
					goto delete_eh_next_seg_kv_ready;
				}
			}

			id += 1;
		}

		i = 0;

		prefetch_eh_bucket_step(bucket, next_seg, bucket_id, cl);
	}

delete_eh_seg_kv_no_matched :
	if (!next_seg || unlikely(eh_bucket_stayed(header)))
		return -1;

delete_eh_next_seg_kv_ready :
	if (cl != EH_PER_BUCKET_CACHELINE)
		prefetch_eh_next_bucket_head(next_seg, bucket_id, cl);

	l_depth += 1;
	bucket = &next_seg->bucket[bucket_id];
	
	goto delete_eh_next_seg_kv;
}
