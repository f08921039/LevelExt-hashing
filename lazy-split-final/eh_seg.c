#include "eh_seg.h"
#include "eh_rehash.h"
#include "per_thread.h"

#define EH_ADV_SPLIT_THREHOLD	DIV_2(EH_SLOT_NUM_PER_BUCKET * 7, 3)
#define EH_BOOST_SPLIT_THREHOLD	DIV_2(EH_SLOT_NUM_PER_BUCKET * 9, 3)
#define EH_EMERGENCY_SPLIT_THREHOLD	DIV_2(EH_SLOT_NUM_PER_BUCKET * 23, 4)

#define half_bucket_of_eh_seg2(bucket_id)	((bucket_id) & SHIFT((EH_BUCKET_INDEX_BIT + 1) - 1))
//#define dis_to_eh_half_bucket(hashed_key, depth)	(((hashed_key) << ((depth) + 1)) >> (PREHASH_KEY_BITS - (EH_BUCKET_INDEX_BIT - 1)))

typedef u64 EH_SLOT_RECORD;

#define EH_RECORD_DEPTH_BIT_START	0
#define EH_RECORD_DEPTH_BIT_END	(EH_RECORD_DEPTH_BIT_START + EH_DEPTH_BITS - 1)

#define EH_RECORD_BUCKET_BIT_START	EH_DEPTH_BITS
#define EH_RECORD_BUCKET_BIT_END	(VALID_POINTER_BITS - 1)

#define EH_RECORD_LAYER_BIT_START	VALID_POINTER_BITS
#define EH_RECORD_LAYER_BIT_END	(EH_RECORD_LAYER_BIT_START + EH_DEPTH_BITS - 1)

#define EH_RECORD_ID_BIT_START	(EH_RECORD_LAYER_BIT_END + 1)
#define EH_RECORD_ID_BIT_END	(POINTER_BITS - 1)

#define EH_RECORD_BUCKET_MASK	\
			INTERVAL(EH_RECORD_BUCKET_BIT_START, EH_RECORD_BUCKET_BIT_END)

#define INVALID_EH_SLOT_RECORD	0UL

#define eh_record_bucket(record)	((record) & EH_RECORD_BUCKET_MASK)
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

__attribute__((always_inline))
static struct eh_segment *retrieve_eh_high_segment(EH_BUCKET_HEADER header) {
	if (eh_seg_low(header))
		return eh_next_high_seg(header);

	return NULL;
}


__attribute__((always_inline))
static void record_bucket_initial_traversal(struct eh_slot_record_pair *rec_pair) {
	rec_pair->record[0] = rec_pair->record[1] = INVALID_EH_SLOT_RECORD;
}

__attribute__((always_inline))
static bool check_bucket_traversal(EH_SLOT_RECORD record) {
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

enum eh_op_type {
	EH_INSERT, 
	EH_LOOKUP, 
	EH_UPDATE, 
	EH_DELETE
};

enum eh_slot_state {
	INVALID_SLOT = -1, 
	DELETED_SLOT = -2, 
	EXISTED_SLOT = -3, 
	REPLACED_SLOT = -4, 
	UNMATCH_SLOT = -5, 
	LACK_SLOT = -6
};

__attribute__((always_inline))
static enum eh_slot_state compare_eh_slot(
					KEY_ITEM key,
					struct kv **slot_kv_ptr, 
					EH_BUCKET_SLOT slot_val) {
	struct kv *slot_kv;

	if (likely(!eh_slot_invalid(slot_val))) {
		slot_kv = eh_slot_kv_addr(slot_val);

		if (__compare_kv_key(slot_kv, key) == 0) {
			if (slot_kv_ptr != NULL)
				*slot_kv_ptr = slot_kv;

			if (eh_slot_deleted(slot_val))
				return DELETED_SLOT;

			return EXISTED_SLOT;
		}

		return UNMATCH_SLOT;
	}

	return INVALID_SLOT;
}

static enum eh_slot_state __check_unique_eh_slot(
					struct kv *kv, 
					KEY_ITEM key, 
					u64 hashed_key, 
					EH_BUCKET_SLOT *slot_addr, 
					struct eh_slot_record_pair *rec_pair) {
	EH_BUCKET_SLOT slot_val, new_slot_val, old_slot_val;
	EH_BUCKET_HEADER header;
	struct eh_bucket *bucket;
	struct eh_segment *next_seg;
	struct kv *old_kv;
	u64 f1, f2;
	enum eh_slot_state state;
	int depth, slot_id, bucket_id;
	bool first;

	first = true;

	bucket = (struct eh_bucket *)eh_record_bucket(rec_pair->record[0]);
	slot_id = eh_record_id(rec_pair->record[0]);
	depth = eh_record_depth(rec_pair->record[0]);

	if (kv)//update
		new_slot_val = rec_pair->slot[0];

check_unique_eh_slot_for_next_segment :
	f1 = hashed_key_fingerprint(hashed_key, depth, 16);

	for (; slot_id < EH_SLOT_NUM_PER_BUCKET; ++slot_id) {
		slot_val = READ_ONCE(bucket->kv[slot_id]);

		if (eh_slot_free(slot_val))
			return EXISTED_SLOT;

		if (eh_slot_end(slot_val))
			break;

		f2 = eh_slot_fingerprint(slot_val, 16);

		if (f1 == f2) {
			if (slot_addr == &bucket->kv[slot_id])
				return EXISTED_SLOT;

			while (1) {
				state = compare_eh_slot(key, &old_kv, slot_val);

				if (state == UNMATCH_SLOT || state == INVALID_SLOT)
					break;

				if (state == DELETED_SLOT)
					return DELETED_SLOT;

				if (kv == NULL)//delete
					new_slot_val = set_eh_slot_deleted(slot_val);

				old_slot_val = cas(&bucket->kv[slot_id], slot_val, new_slot_val);

				if (unlikely(old_slot_val != slot_val)) {
					slot_val = old_slot_val;
					continue;
				}

				if (kv)//update
					reclaim_chunk(old_kv);

				return REPLACED_SLOT;
			}
		}
	}

	if (first) {
		if (likely(check_bucket_traversal(rec_pair->record[1]))) {
			bucket = (struct eh_bucket *)eh_record_bucket(rec_pair->record[1]);
			slot_id = eh_record_id(rec_pair->record[1]);
			depth = eh_record_depth(rec_pair->record[1]);

			if (kv)//update
				new_slot_val = rec_pair->slot[1];

			first = false;
			goto check_unique_eh_slot_for_next_segment;
		}

		return EXISTED_SLOT;
	}

	header = READ_ONCE(bucket->header);
	next_seg = retrieve_eh_high_segment(header);

	if (next_seg && likely(!eh_bucket_stayed(header))) {
		slot_id = 0;
		bucket_id = eh_seg2_bucket_idx(hashed_key, depth);
		bucket = &next_seg->bucket[bucket_id];
		depth += 1;

		if (kv) {//update
			f1 = hashed_key_fingerprint(hashed_key, depth, 18);
			new_slot_val = make_eh_ext2_slot(f1, kv);
		}

		goto check_unique_eh_slot_for_next_segment;
	}

	return EXISTED_SLOT;
}

__attribute__((always_inline))
static enum eh_slot_state check_unique_eh_slot(
					struct kv *kv, 
					KEY_ITEM key, 
					u64 hashed_key, 
					EH_BUCKET_SLOT *slot_addr, 
					struct eh_slot_record_pair *rec_pair) {
	EH_BUCKET_SLOT slot_val;
	struct eh_bucket *bucket;
	int slot_id;

	if (!check_bucket_traversal(rec_pair->record[0]))
		return EXISTED_SLOT;

	bucket = (struct eh_bucket *)eh_record_bucket(rec_pair->record[0]);
	slot_id = eh_record_id(rec_pair->record[0]);

	slot_val = READ_ONCE(bucket->kv[slot_id]);

	if (likely(eh_slot_free(slot_val)))
		return EXISTED_SLOT;

	return __check_unique_eh_slot(kv, key, hashed_key, slot_addr, rec_pair);
}

__attribute__((always_inline))
static enum eh_slot_state update_eh_matched_slot(
						struct kv *kv, 
						KEY_ITEM key, 
						u64 hashed_key, 
						EH_BUCKET_SLOT slot_val, 
						EH_BUCKET_SLOT *slot_addr,
						EH_BUCKET_SLOT new_slot, 
						struct eh_slot_record_pair *rec_pair) {
	EH_BUCKET_SLOT old_slot;
	struct kv *old_kv;
	enum eh_slot_state state;

	while (1) {
		state = compare_eh_slot(key, &old_kv, slot_val);

		if (state == EXISTED_SLOT) {
			state = check_unique_eh_slot(kv, key, hashed_key, slot_addr, rec_pair);

			if (likely(state == EXISTED_SLOT)) {
				old_slot = cas(slot_addr, slot_val, new_slot);

				if (likely(old_slot == slot_val)) {
					reclaim_chunk(old_kv);
					return REPLACED_SLOT;
				}

				record_bucket_initial_traversal(rec_pair);
				slot_val = old_slot;
				continue;
			}
		}

		break;
	}

	return state;
}

__attribute__((always_inline))
static enum eh_slot_state delete_eh_matched_slot(
						KEY_ITEM key,
						u64 hashed_key, 
						EH_BUCKET_SLOT slot_val, 
						EH_BUCKET_SLOT *slot_addr,
						struct eh_slot_record_pair *rec_pair) {
	EH_BUCKET_SLOT old_slot, new_slot;
	enum eh_slot_state state;

	while (1) {
		state = compare_eh_slot(key, NULL, slot_val);

		if (state == EXISTED_SLOT) {
			state = check_unique_eh_slot(NULL, key, hashed_key, slot_addr, rec_pair);

			if (likely(state == EXISTED_SLOT)) {
				new_slot = set_eh_slot_deleted(slot_val);

				old_slot = cas(slot_addr, slot_val, new_slot);

				if (likely(old_slot == slot_val))
					return REPLACED_SLOT;

				record_bucket_initial_traversal(rec_pair);
				slot_val = old_slot;
				continue;
			}
		}

		break;
	}

	return state;
}

__attribute__((always_inline))
static enum eh_slot_state lookup_eh_matched_slot(
						KEY_ITEM key, 
						struct kv **slot_kv_ptr, 
						EH_BUCKET_SLOT slot_val) {
	return compare_eh_slot(key, slot_kv_ptr, slot_val);
}

__attribute__((always_inline))
static enum eh_slot_state insert_eh_matched_slot(
						KEY_ITEM key, 
						EH_BUCKET_SLOT slot_val, 
						EH_BUCKET_SLOT *slot_addr,
						EH_BUCKET_SLOT new_slot) {
	EH_BUCKET_SLOT old_slot;
	struct kv *old_kv;
	enum eh_slot_state state;

	while (1) {
		state = compare_eh_slot(key, &old_kv, slot_val);

		if (state == DELETED_SLOT) {
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

	return state;
}


static int __append_eh_slot(
			struct eh_slot_record_pair *rec_pair, 
			struct kv *kv, 
			KEY_ITEM key, 
			u64 hashed_key) {
	EH_BUCKET_SLOT new_slot, tmp_slot;
	struct eh_bucket *bucket;
	struct eh_segment *next_seg;
	EH_BUCKET_HEADER header;
	u64 f1, f2;
	enum eh_slot_state state;
	int slot_id, bucket_id, depth, layer;
	bool first, invalid;

retry_append_eh_slot :
	first = true;
	invalid = false;

	bucket = (struct eh_bucket *)eh_record_bucket(rec_pair->record[0]);
	slot_id = eh_record_id(rec_pair->record[0]);
	depth = eh_record_depth(rec_pair->record[0]);
	layer = eh_record_layer(rec_pair->record[0]);

	new_slot = rec_pair->slot[0];

append_eh_slot_next_segment :
	f1 = eh_slot_fingerprint(new_slot, 16);

	for (; slot_id < EH_SLOT_NUM_PER_BUCKET; ++slot_id) {
		tmp_slot = READ_ONCE(bucket->kv[slot_id]);
		//acquire_fence();

		if (likely(eh_slot_free(tmp_slot))) {
			if (unlikely(invalid)) {
				record_bucket_traversal(rec_pair, new_slot, bucket, 
				                                    slot_id, depth, layer);
				break;
			} else {
				tmp_slot = cas(&bucket->kv[slot_id], FREE_EH_SLOT, new_slot);

				if (likely(eh_slot_free(tmp_slot)))
					return EH_SLOT_NUM_PER_BUCKET * layer + slot_id;
			}
		}

		if (unlikely(eh_slot_end(tmp_slot)))
			break;

		f2 = eh_slot_fingerprint(tmp_slot, 16);

		if (unlikely(f1 == f2)) {
			state = insert_eh_matched_slot(key, tmp_slot, &bucket->kv[slot_id], new_slot);

			if (state == EXISTED_SLOT)
				return EXISTED_SLOT;

			if (state == REPLACED_SLOT)
				return 0;

			if (state == INVALID_SLOT && !invalid) {
				invalid = true;
				first = false;
				record_bucket_initial_traversal(rec_pair);
			}
		}
	}


	if (first) {
		first = false;

		if (check_bucket_traversal(rec_pair->record[1])) {
			bucket = (struct eh_bucket *)eh_record_bucket(rec_pair->record[1]);
			slot_id = eh_record_id(rec_pair->record[1]);
			depth = eh_record_depth(rec_pair->record[1]);
			layer = eh_record_layer(rec_pair->record[1]);
			new_slot = rec_pair->slot[1];
			goto append_eh_slot_next_segment;
		}
	}

	header = READ_ONCE(bucket->header);
	next_seg = retrieve_eh_high_segment(header);

	if (next_seg && likely(!eh_bucket_stayed(header))) {
		slot_id = 0;
		bucket_id = eh_seg2_bucket_idx(hashed_key, depth);
		bucket = &next_seg->bucket[bucket_id];
		depth += 1;
		layer += 1;

		f1 = hashed_key_fingerprint(hashed_key, depth, 18);
		new_slot = make_eh_ext2_slot(f1, kv);

		goto append_eh_slot_next_segment;
	}

	if (invalid && check_bucket_traversal(rec_pair->record[0]))
		goto retry_append_eh_slot;

	record_bucket_initial_traversal(rec_pair);
	return LACK_SLOT;
}

__attribute__((always_inline))
static int append_eh_slot(
			struct eh_slot_record_pair *rec_pair, 
			struct kv *kv, 
			KEY_ITEM key, 
			u64 hashed_key) {
	EH_BUCKET_SLOT *slot;
	struct eh_bucket *bucket;
	int slot_id, layer;

	bucket = (struct eh_bucket *)eh_record_bucket(rec_pair->record[0]);
	slot_id = eh_record_id(rec_pair->record[0]);

	slot = &bucket->kv[slot_id];

	if (likely(cas_bool(slot, FREE_EH_SLOT, rec_pair->slot[0]))) {
		layer = eh_record_layer(rec_pair->record[0]);
		return EH_SLOT_NUM_PER_BUCKET * layer + slot_id;
	}

	return __append_eh_slot(rec_pair, kv, key, hashed_key);
}

static void __boost_eh_split_entry(
				struct eh_seg_context *seg_context, 
				struct eh_bucket *bucket,
				EH_BUCKET_HEADER bucket_header, 
				u64 hashed_key, 
				SPLIT_PRIORITY priority, 
				bool urgent) {
	EH_BUCKET_HEADER header, new_header, old_header;
	struct eh_segment *buttom_seg, *next_seg, *seg;
	struct eh_split_entry *s_ent, *old_s_ent;
	int depth, right;
	bool head;
	SPLIT_PRIORITY old_priority;

	seg = seg_context->cur_seg;
	header = READ_ONCE(seg->bucket[0].header);

	depth = seg_context->depth;
	buttom_seg = seg_context->buttom_seg;

	head = (bucket == &seg->bucket[0]);

re_boost_eh_split_entry :
	if (unlikely(eh_bucket_no_top_initial(header)))
		return;

	if (header == INITIAL_EH_BUCKET_TOP_HEADER)
		goto finish_boost_eh_split_entry;

	if (eh_seg_low(header)) {
		right = eh_seg_id_in_seg2(hashed_key, depth - 1);
		next_seg = eh_next_high_seg(header);

		if (eh_four_seg(header))
			next_seg += MUL_2(right, 1);
		else if (right == 1)
			return;

		header = set_eh_seg_low(next_seg);
		header = set_eh_bucket_stayed(header);
		goto finish_boost_eh_split_entry;
	}

	old_s_ent = eh_split_entry_addr(header);
	old_priority = eh_split_entry_priority(header);

	if (unlikely(old_priority == THREAD_PRIO))
		goto finish_boost_eh_split_entry;

	if ((priority == LOW_PRIO && old_priority != IDLE_PRIO) || 
		(priority == HIGH_PRIO && old_priority == HIGH_PRIO) || 
										priority == IDLE_PRIO) {
		if (!urgent || eh_seg_urgent(header))
			goto finish_boost_eh_split_entry;
		
		s_ent = NULL; 

		new_header = set_eh_seg_urgent(header);
	} else {
		s_ent = new_split_record(priority);

		if (unlikely(s_ent == (struct eh_split_entry *)MAP_FAILED))
			return;

		new_header = set_eh_split_entry(s_ent, priority);

		if (urgent)
			new_header = set_eh_seg_urgent(new_header);
	}

	old_header = cas(&seg->bucket[0].header, header, new_header);

	if (unlikely(old_header != header)) {
	        if (s_ent)
		        invalidate_eh_split_entry(s_ent);

		header = old_header;
		goto re_boost_eh_split_entry;
	}

	header = new_header;

	if (s_ent) {
		if (unlikely(upgrade_eh_split_entry(old_s_ent, buttom_seg)))
			invalidate_eh_split_entry(s_ent);
		else {
			init_eh_split_entry(s_ent, buttom_seg, seg, hashed_key, depth - 1, NORMAL_SPLIT);

			memory_fence();
			header = READ_ONCE(seg->bucket[0].header);

			if (unlikely(new_header != header))
				modify_eh_split_entry(s_ent, header, buttom_seg, seg, hashed_key, depth - 1);
		}
	}

finish_boost_eh_split_entry :
	if (!head)
		cas(&bucket->header, bucket_header, header);
}

__attribute__((always_inline))
static int boost_eh_split_entry(
				struct eh_seg_context *seg_context, 
				struct eh_bucket *bucket,
				EH_BUCKET_HEADER bucket_header, 
				u64 hashed_key, 
				int slot_id, 
				bool same_node) {
	bool urgent;
	SPLIT_PRIORITY priority;
	SPLIT_STATE state;

	if (likely(slot_id < EH_ADV_SPLIT_THREHOLD) || 
			(!same_node && slot_id < EH_EMERGENCY_SPLIT_THREHOLD) ||
			slot_id >= MUL_2(EH_SLOT_NUM_PER_BUCKET, 1) || 
			slot_id < seg_context->layer * EH_SLOT_NUM_PER_BUCKET)
		return 0;	

	if (slot_id < EH_SLOT_NUM_PER_BUCKET)
		return 1;

	if (eh_bucket_no_normal_initial(bucket_header))
		return 0;

	priority = ((bucket_header != INITIAL_EH_BUCKET_HEADER) ? 
				eh_split_entry_priority(bucket_header) : IDLE_PRIO);

	if (unlikely(priority == THREAD_PRIO))
		return 0;

	state = tls_split_state();

	if (slot_id >= EH_EMERGENCY_SPLIT_THREHOLD) {
		if (same_node) {
			if (state != MANY_SPLITS || !is_tls_help_split())
				return 1;

			priority = HIGH_PRIO;
		} else
			priority = IDLE_PRIO;

		if (bucket_header != INITIAL_EH_BUCKET_HEADER 
							&& eh_seg_urgent(bucket_header))
			return 0;

		urgent = true;
	} else if (slot_id < EH_BOOST_SPLIT_THREHOLD || state == MANY_SPLITS) {
		if (priority != IDLE_PRIO)
			return 0;

		priority = LOW_PRIO;
		urgent = false;
	} else {
		if (priority == HIGH_PRIO)
			return 0;

		priority = HIGH_PRIO;
		urgent = false;
	}

	__boost_eh_split_entry(seg_context, bucket, bucket_header, hashed_key, priority, urgent);
	return 0;
}

struct eh_segment *add_eh_new_segment_for_buttom(
						struct eh_seg_context *seg_context, 
						struct eh_bucket *bucket,
						EH_BUCKET_SLOT slot_val, 
						u64 hashed_key) {
	EH_BUCKET_HEADER header, bucket_header, 
					new_header, old_header, next_header;
	struct eh_bucket *new_bucket;
	struct eh_split_entry *s_ent;
	struct eh_segment *seg, *next_seg;
	int bucket_id, depth, g_depth, depth_diff, nid;
	bool helping, head, check_prefault;
	SEG_POSITION pos;
	SPLIT_PRIORITY priority;
	SPLIT_STATE state;

	seg = seg_context->cur_seg;
	pos = seg_context->pos;
	depth = seg_context->depth;

	header = READ_ONCE(seg->bucket[0].header);

	head = (bucket == &seg->bucket[0]);
	check_prefault = false;

	if (pos == NUMA_BUTTOM_SEG) {
		nid = seg_context->node_id;
		priority = LOW_PRIO;
	} else {
		state = tls_split_state();
		g_depth = get_eh_depth(hashed_key);
		depth_diff = g_depth - depth;

		if (depth_diff < 3) {
			helping = (g_depth > depth) ? is_tls_help_split() : true;

			if (slot_val != FREE_EH_SLOT)
				priority = helping ? LOW_PRIO : THREAD_PRIO;
			else {
				if (state != MANY_SPLITS && !helping)
					priority = THREAD_PRIO;
				else if (state == FEW_SPLITS)
					priority = IDLE_PRIO;
				else
					goto to_prepare_eh_seg_for_buttom;

				check_prefault = true;
			}
		} else {
			switch (state) {
			case FEW_SPLITS:
				priority = HIGH_PRIO;
				break;
			case MODERATE_SPLITS:
				priority = LOW_PRIO;
				break;
			default:
				priority = IDLE_PRIO;
				break;
			}
		}
	}

	if (unlikely(eh_seg_low(header)))
		return eh_next_high_seg(header);

	if (check_prefault && !is_prefault_seg_enough(2))
		goto to_prepare_eh_seg_for_buttom;

	next_seg = (pos != NUMA_BUTTOM_SEG) ?
					alloc_eh_seg(2) : 
					alloc_other_eh_seg(2, nid);

	if (unlikely((void *)next_seg == MAP_FAILED))
		return (slot_val != FREE_EH_SLOT) ? (struct eh_segment *)MAP_FAILED : NULL;

	new_header = set_eh_seg_low(next_seg);

	if (!head || slot_val == FREE_EH_SLOT)
		new_header = set_eh_bucket_stayed(new_header);

	bucket_id = eh_seg2_bucket_idx(hashed_key, depth);
	new_bucket = &next_seg->bucket[bucket_id];

	s_ent = (pos != NUMA_BUTTOM_SEG) ? 
				new_split_record(priority) : 
				new_other_split_record(priority, nid);

	if (unlikely(s_ent == (struct eh_split_entry *)MAP_FAILED)) {
		free_page_aligned(next_seg, MUL_2(2, EH_SEGMENT_SIZE_BITS));
		return (slot_val != FREE_EH_SLOT) ? (struct eh_segment *)MAP_FAILED : NULL;
	}

	if (priority == THREAD_PRIO)
		init_tls_split_entry(seg, next_seg, hashed_key, depth, NORMAL_SPLIT);

	next_header = set_eh_split_entry(s_ent, priority);
	new_bucket->header = next_header;
	next_seg->bucket[0].header = next_header;

	if (slot_val != FREE_EH_SLOT)
		new_bucket->kv[0] = slot_val;

	while (1) {
		old_header = cas(&seg->bucket[0].header, header, new_header);

		if (likely(header == old_header)) {
			if (priority == THREAD_PRIO)
				prefetch_eh_segment_for_normal_split(seg, next_seg, 0);
			else {
				init_eh_split_entry(s_ent, seg, next_seg, hashed_key, depth, NORMAL_SPLIT);

				memory_fence();
				header = READ_ONCE(next_seg->bucket[0].header);

				if (unlikely(next_header != header))
					modify_eh_split_entry(s_ent, header, seg, next_seg, hashed_key, depth);
			}

			seg_context->cur_seg = next_seg;
			break;
		}

		header = old_header;

		if (eh_seg_low(header)) {
			invalidate_eh_split_entry(s_ent);
			free_page_aligned(next_seg, MUL_2(2, EH_SEGMENT_SIZE_BITS));
			next_seg = eh_next_high_seg(header);
			break;
		}
	}

	return next_seg;

to_prepare_eh_seg_for_buttom :
	bucket_header = READ_ONCE(bucket->header);

	if (bucket_header == INITIAL_EH_BUCKET_PREALLOC_HEADER)
		return NULL;

	while (header != INITIAL_EH_BUCKET_PREALLOC_HEADER) {
		if (eh_seg_low(header))
			return eh_next_high_seg(header);

		old_header = cas(&seg->bucket[0].header, header, 
							INITIAL_EH_BUCKET_PREALLOC_HEADER);

		if (likely(old_header == header)) {
			hint_eg_seg_prefault(2);
			header = INITIAL_EH_BUCKET_PREALLOC_HEADER;
			break;
		}

		header = old_header;
	}

	if (!head)
		cas(&bucket->header, bucket_header, header);

	return NULL;
}

struct eh_segment *add_eh_new_segment_for_top(
						struct eh_seg_context *seg_context, 
						struct eh_bucket *bucket,
						EH_BUCKET_SLOT slot_val, 
						u64 hashed_key) {
	EH_BUCKET_HEADER header, new_header, old_header, next_header;
	struct eh_bucket *new_bucket;
	struct eh_split_entry *s_ent, *old_s_ent;
	uintptr_t target_ent;
	struct eh_segment *buttom_seg, *seg, *next_seg, *target_seg, *dest_seg;
	int bucket_id, depth, g_depth, nid, right, right2;
	bool head;
	SPLIT_STATE state;
	SEG_POSITION pos;
	SPLIT_PRIORITY priority, old_priority;

	seg = seg_context->cur_seg;
	pos = seg_context->pos;
	buttom_seg = seg_context->buttom_seg;
	depth = seg_context->depth - 1;

	header = READ_ONCE(seg->bucket[0].header);

	head = (bucket == &seg->bucket[0]);
	right = eh_seg_id_in_seg2(hashed_key, depth);

	priority = URGENT_PRIO;
	
	if (pos == NUMA_TOP_SEG)
		nid = seg_context->node_id;
	else if (slot_val == FREE_EH_SLOT) {
		state = tls_split_state();

		if (state == MANY_SPLITS && !is_tls_help_split() 
		                        && is_prefault_seg_enough(4)) {
		        g_depth = get_eh_depth(hashed_key);
		        
		        if (g_depth > depth)
			        priority = THREAD_PRIO;
		}
	} 

	if (unlikely(eh_seg_low(header)))
		goto add_eh_existed_top_segment;

	if (unlikely(eh_bucket_no_top_initial(header)))
		goto add_eh_new_segment_for_top_to_buttom;

	next_seg = (pos != NUMA_TOP_SEG) ? 
						alloc_eh_seg(4) : 
						alloc_other_eh_seg(4, nid);

	if (unlikely((void *)next_seg == MAP_FAILED))
		return (slot_val != FREE_EH_SLOT) ? (struct eh_segment *)MAP_FAILED : NULL;

	new_header = set_eh_seg_low(next_seg);
	new_header = set_eh_four_seg(new_header);

	if (!head || slot_val == FREE_EH_SLOT)
		new_header = set_eh_bucket_stayed(new_header);

	bucket_id = eh_seg4_bucket_idx(hashed_key, depth);
	new_bucket = &next_seg->bucket[bucket_id];

	if (header == INITIAL_EH_BUCKET_TOP_HEADER) {
		s_ent = NULL;
		priority = URGENT_PRIO;
	} else {
		s_ent = (pos != NUMA_TOP_SEG) ? 
						new_split_record(priority) : 
						new_other_split_record(priority, nid);

		if (unlikely((void *)s_ent == MAP_FAILED)) {
			free_page_aligned(next_seg, MUL_2(4, EH_SEGMENT_SIZE_BITS));
			return (slot_val != FREE_EH_SLOT) ? (struct eh_segment *)MAP_FAILED : NULL;
		}

		old_s_ent = eh_split_entry_addr(header);
		old_priority = eh_split_entry_priority(header);

		if (unlikely(old_priority == THREAD2_PRIO)) {
			depth -= 1;
			right2 = eh_seg_id_in_seg2(hashed_key, depth);

			target_ent = READ_ONCE(old_s_ent->target);

			target_seg = eh_s_ent_target_seg(target_ent);
			dest_seg = seg - MUL_2(right2, 1);
		} else {
			target_seg = buttom_seg;
			dest_seg = next_seg;
		}
	}

	if (priority == URGENT_PRIO)
		next_header = INITIAL_EH_BUCKET_TOP_HEADER;
	else {
	        target_ent = make_eh_split_target_entry(target_seg, 0, 0, INVALID_SPLIT);
	        WRITE_ONCE(s_ent->target, target_ent);
		next_header = set_eh_split_entry(s_ent, THREAD2_PRIO);
	}

	next_seg->bucket[0].header = next_header;
	next_seg[2].bucket[0].header = next_header;

	if (slot_val != FREE_EH_SLOT)
		new_bucket->kv[0] = slot_val;

	while (1) {
		old_header = cas(&seg->bucket[0].header, header, new_header);

		if (likely(header == old_header)) {
			if (s_ent) {
				if (unlikely(old_priority == THREAD2_PRIO)) {
					seg = &dest_seg[MUL_2(right2 ^ 1, 1)];

					cas(&seg->bucket[0].header, 
							header, INITIAL_EH_BUCKET_TOP_HEADER);
				}
				
				if (unlikely(upgrade_eh_split_entry(old_s_ent, target_seg)))
					invalidate_eh_split_entry(s_ent);
				else if (priority == THREAD_PRIO) {
				        init_tls_split_entry(target_seg, dest_seg, hashed_key, depth, URGENT_SPLIT);
				        
				        memory_fence();
				        header = READ_ONCE(dest_seg->bucket[0].header);
				        
				        if (likely(header == next_header))
				            prefetch_eh_segment_for_urgent_split(target_seg, seg, dest_seg, 0);
				        else {
				            cas(&dest_seg[2].bucket[0].header, next_header, INITIAL_EH_BUCKET_TOP_HEADER);
				            
				            modify_tls_split_entry(target_seg, dest_seg);
				        }
				} else
					init_eh_split_entry(s_ent, target_seg, dest_seg, hashed_key, depth, URGENT_SPLIT);
			}

			next_seg += MUL_2(right, 1);
			seg_context->cur_seg = next_seg;
			break;
		}

		header = old_header;

		if (eh_seg_low(header)) {
		        if (s_ent)
			    invalidate_eh_split_entry(s_ent);

			free_page_aligned(next_seg, MUL_2(4, EH_SEGMENT_SIZE_BITS));
			goto add_eh_existed_top_segment;
		}

		if (unlikely(old_priority == THREAD2_PRIO)) {
			depth += 1;
			target_seg = buttom_seg;
			dest_seg = next_seg;
		}

		if (!eh_bucket_initial(header)) {
			old_s_ent = eh_split_entry_addr(header);
			old_priority = eh_split_entry_priority(header);

			if (s_ent == NULL) {
				s_ent = (pos != NUMA_TOP_SEG) ? 
						new_split_record(URGENT_PRIO) : 
						new_other_split_record(URGENT_PRIO, nid);

				if (unlikely(s_ent == (struct eh_split_entry *)MAP_FAILED)) {
					free_page_aligned(next_seg, MUL_2(4, EH_SEGMENT_SIZE_BITS));
					return (slot_val != FREE_EH_SLOT) ? (struct eh_segment *)MAP_FAILED : NULL;
				}

				target_seg = buttom_seg;
			        dest_seg = next_seg;
			}
		} else {
			if (s_ent) {
				invalidate_eh_split_entry(s_ent);
				s_ent = NULL;

				next_seg->bucket[0].header = INITIAL_EH_BUCKET_TOP_HEADER;
				next_seg[2].bucket[0].header = INITIAL_EH_BUCKET_TOP_HEADER;
			}

			if (eh_bucket_no_top_initial(header)) {
				free_page_aligned(next_seg, MUL_2(4, EH_SEGMENT_SIZE_BITS));
				goto add_eh_new_segment_for_top_to_buttom;
			}
		}
	}

	return next_seg;

add_eh_existed_top_segment :
	next_seg = eh_next_high_seg(header);

	if (eh_four_seg(header))
		return (next_seg + MUL_2(right, 1));

	if (right == 0)
		return next_seg;

add_eh_new_segment_for_top_to_buttom :
	if (slot_val != FREE_EH_SLOT) {
		seg += right;

		seg_context->pos = (pos != NUMA_TOP_SEG) ? BUTTOM_SEG : NUMA_BUTTOM_SEG;
		seg_context->cur_seg = seg;
		seg_context->layer = 0;
	}

	return NULL;
}

struct eh_segment *add_eh_new_segment(
						struct eh_seg_context *seg_context, 
						struct eh_segment *next_seg, 
						struct eh_bucket *bucket,
						EH_BUCKET_HEADER bucket_header, 
						u64 hashed_key, 
						struct kv *kv) {
	EH_BUCKET_HEADER new_header;
	EH_BUCKET_SLOT slot_val;
	u64 fingerprint;
	struct eh_segment *ret_seg, *seg;
	struct eh_bucket *new_bucket;
	int depth, bucket_id;
	bool head;
	SEG_POSITION pos;

	depth = seg_context->depth;
	pos = seg_context->pos;

	if (kv) {
		fingerprint = hashed_key_fingerprint(hashed_key, depth + 1, 18);
		slot_val = make_eh_ext2_slot(fingerprint, kv);
	} else 
		slot_val = FREE_EH_SLOT;

	if (next_seg) {
		new_header = cancel_eh_bucket_stayed(bucket_header);
		head = false;
	} else {
	        seg = seg_context->cur_seg;
	        head = (bucket == &seg->bucket[0]);

		if (pos == TOP_SEG || pos == NUMA_TOP_SEG) {
			next_seg = add_eh_new_segment_for_top(seg_context, bucket, 
													slot_val, hashed_key);
			pos = seg_context->pos;
		}

		if (pos != TOP_SEG && pos != NUMA_TOP_SEG)
			next_seg = add_eh_new_segment_for_buttom(seg_context, 
										bucket, slot_val, hashed_key);

		if (unlikely(next_seg == (struct eh_segment *)MAP_FAILED))
			return (struct eh_segment *)MAP_FAILED;

		if (next_seg == NULL)
			return NULL;

		new_header = set_eh_seg_low(next_seg);

		if (next_seg == seg_context->cur_seg) {
			ret_seg = NULL;
			goto finish_add_eh_new_segment;
		}
	}

	if (kv == NULL)
		ret_seg = NULL;
	else {
		bucket_id = eh_seg2_bucket_idx(hashed_key, depth);
		new_bucket = &next_seg->bucket[bucket_id];

		if (!cas_bool(&new_bucket->kv[0], FREE_EH_SLOT, slot_val))
			ret_seg = next_seg;
		else {
			ret_seg = NULL;

			if (pos == BUTTOM_SEG) {
			        seg_context->buttom_seg = seg_context->cur_seg;
				seg_context->cur_seg = next_seg;
				seg_context->depth = depth + 1;

				__boost_eh_split_entry(seg_context, new_bucket, 
							INITIAL_EH_BUCKET_HEADER, hashed_key, LOW_PRIO, false);
			}
		}
	}

finish_add_eh_new_segment :
	if (!head) {
		if (kv == NULL) {
			new_header = set_eh_bucket_stayed(new_header);
			cas(&bucket->header, bucket_header, new_header);
		} else {
			release_fence();
			WRITE_ONCE(bucket->header, new_header);
		}
	}

	return ret_seg;
}


__attribute__((always_inline)) 
static void eh_help_split() {
	struct eh_split_context *s_context; 
	int ret;
	
	s_context = possess_tls_split_context();

	if (s_context == NULL)
		return;

	ret = eh_split(s_context);

	if (ret == 0)
		invalidate_eh_split_entry(&s_context->entry);
	else if (ret == 1) {
		ret = dispossess_tls_split_context();

		if (unlikely(ret == -1)) {
			//to doooooooooo handle memory error and redo upgrade tls split entry
		}
	} else {
		//to dooooooooooooo handle memory error and redo memory fail handle logging
	}
}


int insert_eh_seg_kv(
			struct kv *kv, 
			KEY_ITEM key, 
			u64 hashed_key, 
			struct eh_bucket *bucket, 
			struct eh_seg_context *seg_context) {
	struct eh_segment *next_seg;
	EH_BUCKET_HEADER header;
	EH_BUCKET_SLOT slot, new_slot;
	struct eh_slot_record_pair rec_pair;
	u64 fingerprint;
	enum eh_slot_state state;
	int bucket_id, slot_id;
	bool invalid, stay, same_node;

	same_node = (seg_context->node_id == tls_node_id());
	seg_context->pos = (same_node ? BUTTOM_SEG : NUMA_BUTTOM_SEG);
	seg_context->layer = 0;
	
	record_bucket_initial_traversal(&rec_pair);

insert_eh_next_seg_kv :
	//eh_help_split();

	fingerprint = hashed_key_fingerprint(hashed_key, seg_context->depth, 18);
	new_slot = make_eh_ext2_slot(fingerprint, kv);
	fingerprint = fingerprint >> 2;

	bucket_id = eh_seg2_bucket_idx(hashed_key, seg_context->depth);

	header = READ_ONCE(bucket->header);
	next_seg = retrieve_eh_high_segment(header);

	if (next_seg) {
		if (unlikely(eh_seg_splited(header)))
			goto insert_eh_next_seg_kv_ready;

		stay = eh_bucket_stayed(header);

		if (!stay)
			prefetch_eh_bucket_head(&next_seg->bucket[bucket_id]);
	}

	invalid = false;

	for (slot_id = 0; slot_id < EH_SLOT_NUM_PER_BUCKET; ++slot_id) {
		slot = READ_ONCE(bucket->kv[slot_id]);

		if (unlikely(eh_slot_end(slot)))
			break;

		if (eh_slot_free(slot)) {
			record_bucket_traversal(&rec_pair, new_slot, bucket, slot_id, 
								seg_context->depth, seg_context->layer);
			break;
		}

		if (fingerprint == eh_slot_fingerprint(slot, 16)) {
			state = insert_eh_matched_slot(key, slot, &bucket->kv[slot_id], new_slot);

			if (state == EXISTED_SLOT)
				return 1;

			if (state == REPLACED_SLOT)
				return 0;

			if (unlikely(state == INVALID_SLOT))
				invalid = true;
		}
	}

	acquire_fence();

	if (!next_seg || unlikely(stay)) {
		if (unlikely(invalid)) {
			header = READ_ONCE(bucket->header);
			next_seg = eh_next_high_seg(header);

			goto insert_eh_next_seg_kv_ready;
		}

		if (check_bucket_traversal(rec_pair.record[0])) {
			slot_id = append_eh_slot(&rec_pair, kv, key, hashed_key);

			if (likely(slot_id >= 0)) {
				if (next_seg || boost_eh_split_entry(seg_context, bucket, 
										header, hashed_key, slot_id, same_node) == 0)
					return 0;

				kv = NULL;
			} else if (slot_id == EXISTED_SLOT)
				return 1;
		}

		next_seg = add_eh_new_segment(seg_context, next_seg, bucket, header, hashed_key, kv);

		if (likely(next_seg == NULL))
			return 0;

		if (unlikely((void *)next_seg == MAP_FAILED))
			return -1;
	}

insert_eh_next_seg_kv_ready :
	seg_context->buttom_seg = seg_context->cur_seg;

	if (seg_context->layer != 0)
		seg_context->buttom_seg += eh_seg_id_in_seg2(hashed_key, seg_context->depth - 1);

	seg_context->layer += 1;
	seg_context->depth += 1;
	seg_context->pos = (same_node ? TOP_SEG : NUMA_TOP_SEG);
	seg_context->cur_seg = next_seg;

	bucket = &next_seg->bucket[bucket_id];
	goto insert_eh_next_seg_kv;
}

int update_eh_seg_kv(
		struct kv *kv, 
		KEY_ITEM key, 
		u64 hashed_key, 
		struct eh_bucket *bucket, 
		int l_depth) {
	struct eh_segment *next_seg;
	EH_BUCKET_HEADER header;
	EH_BUCKET_SLOT slot, new_slot;
	struct eh_slot_record_pair rec_pair;
	u64 fingerprint;
	enum eh_slot_state state;
	int bucket_id, slot_id;
	bool invalid, stay;

	//to dooooooooooooooooooooooo add extra function

	record_bucket_initial_traversal(&rec_pair);

update_eh_next_seg_kv :
	fingerprint = hashed_key_fingerprint(hashed_key, l_depth, 18);
	new_slot = make_eh_ext2_slot(fingerprint, kv);
	fingerprint = fingerprint >> 2;

	bucket_id = eh_seg2_bucket_idx(hashed_key, l_depth);

	header = READ_ONCE(bucket->header);
	next_seg = retrieve_eh_high_segment(header);

	if (next_seg) {
		if (unlikely(eh_seg_splited(header)))
			goto update_eh_next_seg_kv_ready;

		stay = eh_bucket_stayed(header);

		if (!stay)
			prefetch_eh_bucket_head(&next_seg->bucket[bucket_id]);
	}

	invalid = false;

	for (slot_id = 0; slot_id < EH_SLOT_NUM_PER_BUCKET; ++slot_id) {
		slot = READ_ONCE(bucket->kv[slot_id]);

		if (unlikely(eh_slot_end(slot)))
			break;

		if (eh_slot_free(slot)) {
			record_bucket_traversal(&rec_pair, new_slot, 
										bucket, slot_id, l_depth, 0);
			break;
		}

		if (fingerprint == eh_slot_fingerprint(slot, 16)) {
			state = update_eh_matched_slot(kv, key, hashed_key, slot, 
								&bucket->kv[slot_id], new_slot, &rec_pair);

			if (state == REPLACED_SLOT)
				return 0;

			if (state == DELETED_SLOT)
				return 1;

			if (unlikely(state == INVALID_SLOT))
				invalid = true;
		}
	}

	acquire_fence();

	if (!next_seg || unlikely(stay)) {
		if (likely(!invalid))
			return 1;

		header = READ_ONCE(bucket->header);
		next_seg = eh_next_high_seg(header);
	}

update_eh_next_seg_kv_ready :
	l_depth += 1;
	bucket = &next_seg->bucket[bucket_id];
	goto update_eh_next_seg_kv;
}


int delete_eh_seg_kv(
		KEY_ITEM key, 
		u64 hashed_key, 
		struct eh_bucket *bucket, 
		int l_depth) {
	struct eh_segment *next_seg;
	EH_BUCKET_HEADER header;
	EH_BUCKET_SLOT slot;
	struct eh_slot_record_pair rec_pair;
	u64 fingerprint;
	enum eh_slot_state state;
	int bucket_id, slot_id;
	bool invalid, stay;

	//to dooooooooooooooooooooooo add extra function

	record_bucket_initial_traversal(&rec_pair);

delete_eh_next_seg_kv :
	fingerprint = hashed_key_fingerprint(hashed_key, l_depth, 16);

	bucket_id = eh_seg2_bucket_idx(hashed_key, l_depth);

	header = READ_ONCE(bucket->header);
	next_seg = retrieve_eh_high_segment(header);

	if (next_seg) {
		if (unlikely(eh_seg_splited(header)))
			goto delete_eh_next_seg_kv_ready;

		stay = eh_bucket_stayed(header);

		if (!stay)
			prefetch_eh_bucket_head(&next_seg->bucket[bucket_id]);
	}

	invalid = false;

	for (slot_id = 0; slot_id < EH_SLOT_NUM_PER_BUCKET; ++slot_id) {
		slot = READ_ONCE(bucket->kv[slot_id]);

		if (unlikely(eh_slot_end(slot)))
			break;

		if (eh_slot_free(slot)) {
			record_bucket_traversal(&rec_pair, FREE_EH_SLOT, 
										bucket, slot_id, l_depth, 0);
			break;
		}

		if (fingerprint == eh_slot_fingerprint(slot, 16)) {
			state = delete_eh_matched_slot(key, hashed_key, slot, &bucket->kv[slot_id], &rec_pair);

			if (state == REPLACED_SLOT)
				return 0;

			if (state == DELETED_SLOT)
				return 1;

			if (unlikely(state == INVALID_SLOT))
				invalid = true;
		}
	}

	acquire_fence();

	if (!next_seg || unlikely(stay)) {
		if (likely(!invalid))
			return 1;

		header = READ_ONCE(bucket->header);
		next_seg = eh_next_high_seg(header);
	}

delete_eh_next_seg_kv_ready :
	l_depth += 1;
	bucket = &next_seg->bucket[bucket_id];
	goto delete_eh_next_seg_kv;
}

struct kv *lookup_eh_seg_kv(
		KEY_ITEM key,  
		u64 hashed_key, 
		struct eh_bucket *bucket,  
		int l_depth) {
	struct eh_segment *next_seg;
	struct kv *ret_kv;
	EH_BUCKET_HEADER header;
	EH_BUCKET_SLOT slot;
	u64 fingerprint;
	enum eh_slot_state state;
	int bucket_id, slot_id;
	bool stay, invalid;

	//to dooooooooooooooooooooooo add extra function

lookup_eh_next_seg_kv :
	fingerprint = hashed_key_fingerprint(hashed_key, l_depth, 16);

	bucket_id = eh_seg2_bucket_idx(hashed_key, l_depth);

	header = READ_ONCE(bucket->header);
	next_seg = retrieve_eh_high_segment(header);

	if (next_seg) {
		if (unlikely(eh_seg_splited(header)))
			goto lookup_eh_next_seg_kv_ready;

		stay = eh_bucket_stayed(header);

		if (!stay)
			prefetch_eh_bucket_head(&next_seg->bucket[bucket_id]);
	}

	invalid = false;

	for (slot_id = 0; slot_id < EH_SLOT_NUM_PER_BUCKET; ++slot_id) {
		slot = READ_ONCE(bucket->kv[slot_id]);

		if (eh_slot_free(slot) || unlikely(eh_slot_end(slot)))
			break;

		if (fingerprint == eh_slot_fingerprint(slot, 16)) {
			state = lookup_eh_matched_slot(key, &ret_kv, slot);

			if (state == EXISTED_SLOT)
				return ret_kv;

			if (state == DELETED_SLOT)
				return NULL;

			if (unlikely(state == INVALID_SLOT))
				invalid = true;
		}
	}

	acquire_fence();

	if (!next_seg || unlikely(stay)) {
		if (likely(!invalid))
			return NULL;

		header = READ_ONCE(bucket->header);
		next_seg = eh_next_high_seg(header);
	}

lookup_eh_next_seg_kv_ready :
	l_depth += 1;
	bucket = &next_seg->bucket[bucket_id];
	goto lookup_eh_next_seg_kv;
}
