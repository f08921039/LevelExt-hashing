#include "per_thread.h"
#include "background.h"

#include "eh_rehash.h"
#include "eh_alloc.h"

void modify_eh_split_entry(
		struct eh_split_entry *s_ent, 
		EH_BUCKET_HEADER modified_header, 
		struct eh_segment *target_seg, 
		struct eh_segment *dest_seg, 
		u64 hashed_key, int depth) {
	struct eh_split_entry *new_ent;
	struct eh_segment *next_seg;
	EH_BUCKET_HEADER new_header, old_header;
	SPLIT_TYPE type;
	SPLIT_PRIORITY priority;

re_modify_eh_split_entry :
	if (eh_seg_low(modified_header)) {
		next_seg = eh_next_high_seg(modified_header);
		type = URGENT_SPLIT;
		priority = URGENT_PRIO;
	} else if (!eh_bucket_initial(modified_header)) {
		next_seg = dest_seg;
		type = NORMAL_SPLIT;
		priority = eh_split_entry_priority(modified_header);
	} else
		return;

	new_ent = new_split_record(priority);

	if (unlikely(new_ent == (struct eh_split_entry *)MAP_FAILED)) {
		if (READ_ONCE(s_ent->target) == INVALID_EH_SPLIT_TARGET)
			return;

		new_ent = s_ent;
	} else if (upgrade_eh_split_entry(s_ent, target_seg)) {
		invalidate_eh_split_entry(new_ent);
		return;
	} else
		init_eh_split_entry(new_ent, target_seg, next_seg, hashed_key, depth, type);

	if (type == NORMAL_SPLIT) {
		new_header = set_eh_split_entry(new_ent, priority);

		old_header = cas(&dest_seg->bucket[0].header, modified_header, new_header);

		if (unlikely(old_header != modified_header)) {
			modified_header = old_header;
			s_ent = new_ent;
			goto re_modify_eh_split_entry;
		}
	}
}

__attribute__((always_inline, optimize("unroll-loops")))
void find_dest_bucket_free_slot(
			struct eh_bucket *start_bucket, 
			unsigned char *dest_idx, 
			int bucket_num) {
	EH_BUCKET_SLOT slot;
	int i, j;

	for (i = 0; i < bucket_num; ++i) {
		dest_idx[i] = EH_SLOT_NUM_PER_BUCKET;

		for (j = 0; j < EH_SLOT_NUM_PER_BUCKET; ++j) {
			slot = READ_ONCE(start_bucket[i].kv[j]);

			if (eh_slot_free(slot)) {
				dest_idx[i] = j;
				break;
			}
		}
	}
}

__attribute__((always_inline))
static EH_BUCKET_SLOT clear_eh_delete_slot(
			EH_BUCKET_SLOT *slot_addr,
			EH_BUCKET_SLOT slot_val) {
	EH_BUCKET_SLOT old, invalid_slot;

	while (eh_slot_deleted(slot_val)) {
		invalid_slot = set_eh_slot_invalidated(slot_val);
		old = cas(slot_addr, slot_val, invalid_slot);

		if (likely(old == slot_val)) {
			reclaim_chunk((void *)eh_slot_kv_addr(slot_val));
			return EH_SLOT_DELETE_STAT;
		}

		slot_val = old;
	}

	return slot_val;
}

__attribute__((optimize("unroll-loops")))
static int further_append_split_record(
				struct eh_segment *seg_l1, 
				struct eh_segment *seg_l2, 
				u64 hashed_prefix, 
				int depth) {
	EH_BUCKET_HEADER header, *header_addr, new_header;
	struct eh_segment *target_seg, *dest_seg;
	struct eh_split_entry *s_ent;
	u64 new_prefix;
	int seg_id, ret;
	SPLIT_TYPE type;
	SPLIT_PRIORITY priority;

	ret = 0;

	for (seg_id = 0; seg_id < 2; ++seg_id) {
		target_seg = &seg_l1[seg_id];
		dest_seg = &seg_l2[MUL_2(seg_id, 1)];

		header_addr = &dest_seg->bucket[0].header;

	retry_further_append_split_record :
		header = READ_ONCE(*header_addr);

		if (unlikely(eh_seg_low(header))) {
			dest_seg = eh_next_high_seg(header);
			type = URGENT_SPLIT;
			priority = URGENT_PRIO;
		} else {
			type = NORMAL_SPLIT;
			priority = LOW_PRIO;
		}

		s_ent = new_split_record(priority);

		if (unlikely(s_ent == (struct eh_split_entry *)MAP_FAILED)) {
			//to dooooooooooooo record in memory fail handle logging
			ret = -1;
			continue;
		}
		
		if (type == NORMAL_SPLIT) {
			new_header = set_eh_split_entry(s_ent, LOW_PRIO);

			if (unlikely(!cas_bool(header_addr, header, new_header))) {
				invalidate_eh_split_entry(s_ent);
				goto retry_further_append_split_record;
			}
		}

		new_prefix = hashed_prefix | SHIFT_OF(seg_id, PREHASH_KEY_BITS - 1 - depth);

		init_eh_split_entry(s_ent, target_seg, dest_seg, new_prefix, depth + 1, type);

		if (type == NORMAL_SPLIT) {
			memory_fence();
			header = READ_ONCE(*header_addr);

			if (unlikely(header != new_header))
				modify_eh_split_entry(s_ent, header, target_seg, 
										dest_seg, new_prefix, depth + 1);
		}
	}

	return ret;
}

static struct eh_segment *add_eh_urgent_segment(
							struct eh_segment *top_seg,
							struct eh_segment *pair_seg) {
	EH_BUCKET_HEADER header, new_header, old_header, top_header;
	struct eh_split_entry *ent;
	struct eh_segment *new_seg;
	SPLIT_PRIORITY priority;

	header = READ_ONCE(top_seg->bucket[0].header);

	while (1) {
		if (unlikely(eh_seg_low(header)))
			return eh_next_high_seg(header);

		new_seg = alloc_eh_seg(4);

		if (unlikely((void *)new_seg == MAP_FAILED))
			return (struct eh_segment *)MAP_FAILED;

		priority = eh_split_entry_priority(header);

		if (priority == THREAD_PRIO) {
			ent = eh_split_entry_addr(header);
			top_header = set_eh_split_entry(ent, THREAD2_PRIO);
		} else 
			top_header = INITIAL_EH_BUCKET_TOP_HEADER;

		new_seg->bucket[0].header = top_header;
		new_seg->bucket[MUL_2(EH_BUCKET_NUM, 1)].header = top_header;

		new_header = set_eh_seg_low(new_seg);
		new_header = set_eh_four_seg(new_header);
		new_header = set_eh_bucket_stayed(new_header);

		old_header = cas(&top_seg->bucket[0].header, header, new_header);

		if (likely(old_header == header)) {
			if (priority == THREAD2_PRIO && pair_seg)
				cas(&pair_seg->bucket[0].header, header, INITIAL_EH_BUCKET_TOP_HEADER);

			break;
		}

		header = old_header;
		reclaim_page(new_seg, EH_SEGMENT_SIZE_BITS + 2);//free_page_aligned(new_seg, MUL_2(4, EH_SEGMENT_SIZE_BITS));
	}

	return new_seg;
}

static int __migrate_eh_slot(
		struct eh_bucket *dest_bucket,
		EH_BUCKET_SLOT *slot_addr,
		EH_BUCKET_SLOT slot_val,
		EH_BUCKET_SLOT new_slot,
		EH_BUCKET_SLOT old_slot,
		unsigned char *dest_idx) {
	EH_BUCKET_SLOT invalid_slot, delete_slot, *addr;
	struct kv *kv;
	int i;

	i = *dest_idx;

	if (eh_slot_free(old_slot)) {
		for (; i < EH_SLOT_NUM_PER_BUCKET; ++i) {
			addr = &dest_bucket->kv[i];

			if (likely(cas_bool(addr, FREE_EH_SLOT, new_slot)))
				goto migrate_eh_half_slot;
		}

		*dest_idx = EH_SLOT_NUM_PER_BUCKET;
		return -1;
	}

	addr = &dest_bucket->kv[i];

re_migrate_eh_half_slot :
	slot_val = old_slot;
	kv = eh_slot_kv_addr(old_slot);

	new_slot = set_eh_slot_replaced_kv(new_slot, kv);

	if (!eh_slot_deleted(old_slot))
		WRITE_ONCE(*addr, new_slot);
	else {
		delete_slot = set_eh_slot_deleted(new_slot);
		WRITE_ONCE(*addr, delete_slot);
	}

migrate_eh_half_slot :
	invalid_slot = set_eh_slot_invalidated(slot_val);
	old_slot = cas(slot_addr, slot_val, invalid_slot);

	if (unlikely(old_slot != slot_val))
		goto re_migrate_eh_half_slot;

	(*dest_idx) = i + 1;
	return 0;
}

__attribute__((always_inline))
static int migrate_eh_slot(
		struct eh_bucket *dest_bucket,
		EH_BUCKET_SLOT *slot_addr,
		EH_BUCKET_SLOT slot_val,
		EH_BUCKET_SLOT new_slot,
		unsigned char *dest_idx) {
	EH_BUCKET_SLOT invalid_slot, old_slot, *addr;

	addr = &dest_bucket->kv[*dest_idx];

	if (unlikely(!cas_bool(addr, FREE_EH_SLOT, new_slot))) {
		(*dest_idx) += 1;
		old_slot = FREE_EH_SLOT;
	} else {
		invalid_slot = set_eh_slot_invalidated(slot_val);
		old_slot = cas(slot_addr, slot_val, invalid_slot);

		if (likely(old_slot == slot_val)) {
			(*dest_idx) += 1;
			return 0;
		}
	}

	return __migrate_eh_slot(dest_bucket, slot_addr, slot_val, new_slot, old_slot, dest_idx);
}

__attribute__((always_inline))
static int migrate_eh_ext_slot(
		struct eh_bucket *dest_bucket,
		EH_BUCKET_SLOT *slot_addr,
		EH_BUCKET_SLOT slot_val,
		unsigned char *dest_idx, 
		int shifts) {
	EH_BUCKET_SLOT new_slot;
	u64 fingerprint;
	struct kv *kv;
	int ext, d_id, ret;

	kv = eh_slot_kv_addr(slot_val);
	ext = eh_slot_ext(slot_val);

	if (ext < shifts)
		ret = 1;
	else {
		d_id = eh_slot_fingerprint(slot_val, shifts);

		if (dest_idx[d_id] < EH_SLOT_NUM_PER_BUCKET) {
			fingerprint = eh_slot_fingerprint(slot_val, 16);
			fingerprint <<= ext;
			fingerprint |= eh_slot_ext_fingerprint(slot_val, ext);

			if (ext == shifts) {
				fingerprint &= INTERVAL(0, 15);
				new_slot = make_eh_slot(fingerprint, kv);
			} else {
				fingerprint &= INTERVAL(0, 16);
				new_slot = make_eh_ext1_slot(fingerprint, kv);
			}

			if (likely(migrate_eh_slot(&dest_bucket[d_id], slot_addr, 
									slot_val, new_slot, &dest_idx[d_id]) == 0))
				return 0;
		}

		ret = -1;
	}

	prefech_r0(kv);
	return ret;
}

__attribute__((always_inline))
static int migrate_eh_int_slot(
		struct eh_bucket *dest_bucket,
		EH_BUCKET_SLOT *slot_addr,
		EH_BUCKET_SLOT slot_val,
		unsigned char *dest_idx, 
		int depth, int shifts) {
	EH_BUCKET_SLOT new_slot;
	struct kv *kv;
	u64 fingerprint18;
	int d_id;

	d_id = eh_slot_fingerprint(slot_val, shifts);

	if (dest_idx[d_id] >= EH_SLOT_NUM_PER_BUCKET)
		return -1;

	kv = eh_slot_kv_addr(slot_val);

#ifdef DHT_INTEGER
	fingerprint18 = SHIFT_OF(get_kv_prehash64(kv), 
				depth + EH_BUCKET_INDEX_BIT + shifts);
	fingerprint18 >>= (PREHASH_KEY_BITS - 18);
#else
	fingerprint18 = (eh_slot_fingerprint(slot_val, 16) & INTERVAL(0, 15 - shifts)) << (2 + shifts);
	fingerprint18 |= 
		(SHIFT_OF(get_kv_prehash48(kv), depth + EH_BUCKET_INDEX_BIT + 16) 
							>> (PREHASH_KEY_BITS - (2 + shifts)));
#endif

	new_slot = make_eh_ext2_slot(fingerprint18, kv);

	return migrate_eh_slot(&dest_bucket[d_id], slot_addr, slot_val, new_slot, &dest_idx[d_id]);
}

static int __further_migrate_eh_slot(
			struct eh_segment *dest_seg, 
			struct eh_bucket *dest_bucket, 
			EH_BUCKET_SLOT *slot_addr,
			EH_BUCKET_SLOT slot_val,
			u64 hashed_prefix, 
			int depth) {
	struct kv *kv;
	u64 fingerprint;
	EH_BUCKET_SLOT slot, invalid_slot, delete_slot, new_slot;
	EH_BUCKET_SLOT *new_addr;
	EH_BUCKET_HEADER header;
	int slot_id, bucket_id, right;
	bool init_append;

	kv = eh_slot_kv_addr(slot_val);

#ifdef DHT_INTEGER
	hashed_prefix = get_kv_prehash64(kv);
#else
	if (depth < 16 - EH_BUCKET_INDEX_BIT) {
		fingerprint = eh_slot_fingerprint(slot_val, 16);
		hashed_prefix |= SHIFT_OF(fingerprint, 
				(PREHASH_KEY_BITS - 16 - EH_BUCKET_INDEX_BIT) - depth);
	}

	hashed_prefix &= ~LST48_KEY_MASK;
	hashed_prefix |= get_kv_prehash48(kv);
#endif

	while (1) {
		header = READ_ONCE(dest_bucket->header);

		if (eh_seg_low(header)) {
			dest_seg = eh_next_high_seg(header);

			right = eh_seg_id_in_seg2(hashed_prefix, depth + 1);
			dest_seg -= MUL_2(right, 1);

			if (unlikely(eh_bucket_stayed(header))) {
				init_append = true;
				header = cancel_eh_bucket_stayed(header);
				WRITE_ONCE(dest_bucket->header, header);
			} else 
				init_append = false;
		} else {
			right = eh_seg_id_in_seg2(hashed_prefix, depth);

			dest_seg = add_eh_urgent_segment(dest_seg + MUL_2(right, 1),
										dest_seg + MUL_2(right ^ 1, 1));

			if (unlikely(dest_seg == (struct eh_segment *)MAP_FAILED))
				return -1;

			init_append = true;

			right = eh_seg_id_in_seg2(hashed_prefix, depth + 1);
			header = set_eh_seg_low(dest_seg + MUL_2(right, 1));
			header = set_eh_four_seg(header);
			WRITE_ONCE(dest_bucket->header, header);
		}

		depth += 1;

		bucket_id = eh_seg4_bucket_idx(hashed_prefix, depth);
		dest_bucket = &dest_seg->bucket[bucket_id];

		fingerprint = hashed_key_fingerprint(hashed_prefix, depth + 2, 18);

		new_slot = make_eh_ext2_slot(fingerprint, kv);
		new_addr = &dest_bucket->kv[0];

		if (init_append && cas_bool(new_addr, FREE_EH_SLOT, new_slot))
			break;

		for (slot_id = 0; slot_id < EH_SLOT_NUM_PER_BUCKET; ++slot_id) {
			new_addr = &dest_bucket->kv[slot_id];
			slot = READ_ONCE(*new_addr);
			
			if (eh_slot_free(slot) && 
					cas_bool(new_addr, FREE_EH_SLOT, new_slot))
				break;
		}
		
		if (slot_id != EH_SLOT_NUM_PER_BUCKET)
		        break;
	}

	while (1) {
		invalid_slot = set_eh_slot_invalidated(slot_val);
		slot = cas(slot_addr, slot_val, invalid_slot);

		if (likely(slot == slot_val))
			break;

		slot_val = slot;
		kv = eh_slot_kv_addr(slot);

		new_slot = set_eh_slot_replaced_kv(new_slot, kv);

		if (!eh_slot_deleted(slot))
			WRITE_ONCE(*new_addr, new_slot);
		else {
			delete_slot = set_eh_slot_deleted(new_slot);
			WRITE_ONCE(*new_addr, delete_slot);
		}
	}

	return 0;
}

__attribute__((always_inline))
static int further_migrate_eh_slot(
			struct eh_segment *dest_seg, 
			struct eh_bucket *dest_bucket, 
			EH_BUCKET_SLOT *slot_addr,
			EH_BUCKET_SLOT slot_val, 
			u64 hashed_prefix, 
			int depth) {
	int d_id;

	d_id = eh_slot_fingerprint(slot_val, 2);

#ifndef DHT_INTEGER
	hashed_prefix |= SHIFT_OF(split->bucket_id, 
			PREHASH_KEY_BITS - EH_BUCKET_INDEX_BIT - depth);
#endif
	return __further_migrate_eh_slot(dest_seg, &dest_bucket[d_id], 
							slot_addr, slot_val, hashed_prefix, depth);
}

static int eh_bucket_migrate(
			struct eh_split_context *split, 
			struct eh_bucket *target_bucket, 
			struct eh_bucket *dest_bucket, 
			unsigned char *dest_ind, 
			int shifts) {
	EH_BUCKET_SLOT slot_val, *slot_addr;
	int s_id, undo_count, ret;
	unsigned char undo_ind[EH_SLOT_NUM_PER_BUCKET];

	//to dooooooooo assert shifts == 1 or shifts == 2

	undo_count = 0;

	for (s_id = 0; s_id < EH_SLOT_NUM_PER_BUCKET; ++s_id) {
		slot_addr = &target_bucket->kv[s_id];
		slot_val = READ_ONCE(*slot_addr);

                if (unlikely(eh_slot_free(slot_val))) {
			slot_val = cas(slot_addr, FREE_EH_SLOT, END_EH_SLOT);

			if (likely(eh_slot_free(slot_val)))
				break;
		}
		
		if (unlikely(eh_slot_end(slot_val)))
                        break;

		if (unlikely(eh_slot_invalid(slot_val)))
			continue;

		slot_val = clear_eh_delete_slot(slot_addr, slot_val);

		if (slot_val == EH_SLOT_DELETE_STAT)
			continue;

		ret = migrate_eh_ext_slot(dest_bucket, slot_addr, 
									slot_val, &dest_ind[0], shifts);

		if (ret == 0)
			continue;

		if (unlikely(ret == -1 && shifts == 1))
			return -1;

		undo_ind[undo_count++] = s_id;
	}

	for (s_id = 0; s_id < undo_count; ++s_id) {
		slot_addr = &target_bucket->kv[undo_ind[s_id]];
		slot_val = READ_ONCE(*slot_addr);
		slot_val = clear_eh_delete_slot(slot_addr, slot_val);

		if (unlikely(slot_val == EH_SLOT_DELETE_STAT))
			continue;

		if (migrate_eh_int_slot(dest_bucket, slot_addr, 
						slot_val, &dest_ind[0], split->depth, shifts) == 0)
			continue;
		
		if (shifts == 1)
			return -1;

		ret = further_migrate_eh_slot(split->dest_seg, 
							dest_bucket, slot_addr, slot_val, 
							split->hashed_prefix, split->depth);

		if (unlikely(ret == -1))
			return -1;
	}

	return 0;
}

static int eh_segment_normal_split(
				struct eh_split_context *split, 
				struct eh_dir *dir, 
				int g_depth) {
	EH_BUCKET_HEADER header, header_l0, header_l0s, header_l1;
	struct eh_segment *seg_l0, *seg_l1, *seg_l2;
	struct eh_bucket *bucket_l0, *bucket_l1;
	int b_id, b_end, ret;
	unsigned char dest_ind[2];
	bool thread_split;

	b_id = split->bucket_id;
	thread_split = split->thread;

	seg_l0 = split->target_seg;
	seg_l1 = split->dest_seg;

	if (thread_split)
		b_end = b_id + 1;
	else {
		prefetch_eh_segment_for_normal_split(seg_l0, seg_l1, b_id);
		b_end = EH_BUCKET_NUM;
	}

	bucket_l0 = &seg_l0->bucket[b_id];
	bucket_l1 = &seg_l1->bucket[MUL_2(b_id, 1)];

	header_l0 = set_eh_seg_low(seg_l1);
	header_l0s = set_eh_seg_splited(header_l0);

	while (b_id == 0 && !thread_split) {
		header_l1 = READ_ONCE(bucket_l1->header);

		if (eh_seg_urgent(header_l1))
			goto add_eh_segment_for_normal2urgent_split;

		if (unlikely(eh_seg_low(header_l1))) {
			seg_l2 = eh_next_high_seg(header_l1);
			goto eh_segment_normal2urgent_split;
		}

		if (cas_bool(&bucket_l1->header, header_l1, INITIAL_EH_BUCKET_TOP_HEADER))
			break;
	}

	for (; b_id < b_end; ++b_id) {
		if (b_id != EH_BUCKET_NUM - 1)
			prefetch_eh_segment_for_normal_split(seg_l0, seg_l1, b_id + 1);
		else if (dir)
			prefetch_eh_dir(dir, g_depth, split->depth);

		find_dest_bucket_free_slot(bucket_l1, &dest_ind[0], 2);

		WRITE_ONCE(bucket_l0->header, header_l0);

		ret = eh_bucket_migrate(split, bucket_l0, bucket_l1, &dest_ind[0], 1);

		if (unlikely(ret == -1))
			goto add_eh_segment_for_normal2urgent_split;

		release_fence();
		WRITE_ONCE(bucket_l0->header, header_l0s);

		bucket_l0 += 1;
		bucket_l1 += 2;
		split->bucket_id = b_id + 1;
	}

	if (b_id == EH_BUCKET_NUM) {
		split->type = INCOMPLETE_SPLIT;

		header_l1 = READ_ONCE(seg_l1->bucket[0].header);

		if (likely(!eh_seg_low(header_l1))) {
			header = cas(&seg_l1->bucket[0].header, header_l1, INITIAL_EH_BUCKET_HEADER);

			if (likely(header_l1 == header))
				return 0;

			header_l1 = header;
		}

		seg_l2 = eh_next_high_seg(header_l1);

		header_l1 = set_eh_seg_low(&seg_l2[2]);
		WRITE_ONCE(seg_l1[1].bucket[0].header, header_l1);

		ret = further_append_split_record(seg_l1, seg_l2, 
								split->hashed_prefix, split->depth);

		if (unlikely(ret == -1)) {
			//to dooooooooooooo record in memory fail handle logging
			return -1;
		}

		return 0;
	}

	return 1;

add_eh_segment_for_normal2urgent_split :
	seg_l2 = add_eh_urgent_segment(seg_l1, NULL);
	
	if (unlikely(seg_l2 == (struct eh_segment *)MAP_FAILED)) {
		//to dooooooooooooo record in memory fail handle logging
		return -1;
	}
	
	if (b_id > DIV_2(EH_BUCKET_NUM, 1)) {
	        header_l1 = set_eh_seg_low(&seg_l2[2]);
	        WRITE_ONCE(seg_l1[1].bucket[0].header, header_l1);
	}

eh_segment_normal2urgent_split :
	prefetch_part3_eh_segment_for_urgent_split(seg_l2, 0);

	split->dest_seg = seg_l2;
	split->inter_seg = seg_l1;
	split->type = URGENT_SPLIT;

	return 1;
}

static int eh_segment_urgent_split(
			struct eh_split_context *split, 
			struct eh_dir *dir, 
			int g_depth) {
	EH_BUCKET_HEADER header_l0, header_l0s, header_l1;
	struct eh_segment *seg_l0, *seg_l1, *seg_l2;
	struct eh_bucket *bucket_l0, *bucket_l1, *bucket_l2;
	int b_id, b_end, right, ret;
	unsigned char dest_ind[4];
	bool thread_split;

	seg_l0 = split->target_seg;
	seg_l2 = split->dest_seg;

	thread_split = split->thread;
	b_id = split->bucket_id;

	if (thread_split)
		b_end = b_id + 1;
	else {
		if (b_id == 0) {
			prefetch_part1_eh_segment_for_urgent_split(seg_l0, 0);
			prefetch_part3_eh_segment_for_urgent_split(seg_l2, 0);
		}

		b_end = EH_BUCKET_NUM;
	}
	
	bucket_l0 = &seg_l0->bucket[b_id];

	if (split->inter_seg == NULL) {
	        header_l0 = READ_ONCE(seg_l0->bucket[0].header);

		seg_l1 = eh_next_high_seg(header_l0);
		split->inter_seg = seg_l1;
		
		if (!thread_split) {
		        while (unlikely(eh_seg_splited(header_l0))) {
		                if (b_id == DIV_2(EH_BUCKET_NUM, 1)) {
			                header_l1 = set_eh_seg_low(&seg_l2[2]);
			                WRITE_ONCE(seg_l1[1].bucket[0].header, header_l1);
			        }
	                        
	                        split->bucket_id = ++b_id;
	                        bucket_l0 += 1;
	                        header_l0 = READ_ONCE(bucket_l0->header);
	                }
	
		        prefetch_part2_eh_segment_for_urgent_split(seg_l1, b_id);
		}
	} else 
		seg_l1 = split->inter_seg;

	header_l0 = set_eh_seg_low(seg_l1);
	header_l0s = set_eh_seg_splited(header_l0);

        bucket_l1 = &seg_l1->bucket[MUL_2(b_id, 1)];
	bucket_l2 = &seg_l2->bucket[MUL_2(b_id, 2)];

	right = DIV_2(b_id, EH_BUCKET_INDEX_BIT - 1);
	header_l1 = set_eh_seg_low(&seg_l2[MUL_2(right, 1)]);
	header_l1 = set_eh_four_seg(header_l1);


	for (; b_id < b_end; ++b_id) {
		if (b_id != EH_BUCKET_NUM - 1)
			prefetch_eh_segment_for_urgent_split(seg_l0, seg_l1, seg_l2, b_id + 1);
		else if (dir)
			prefetch_eh_dir(dir, g_depth, split->depth);

		if (b_id == DIV_2(EH_BUCKET_NUM, 1))
			header_l1 = set_eh_seg_low(&seg_l2[2]);
	
		find_dest_bucket_free_slot(bucket_l2, &dest_ind[0], 4);
	
		WRITE_ONCE(bucket_l0->header, header_l0);

		WRITE_ONCE(bucket_l1[0].header, header_l1);
		WRITE_ONCE(bucket_l1[1].header, header_l1);

		ret = eh_bucket_migrate(split, bucket_l0, bucket_l2, &dest_ind[0], 2);

		if (unlikely(ret == -1)) {
			//to dooooooooooooo record in memory fail handle logging
			return -1;
		}

		release_fence();
		WRITE_ONCE(bucket_l0->header, header_l0s);

		bucket_l0 += 1;
		bucket_l1 += 2;
		bucket_l2 += 4;
		split->bucket_id = b_id + 1;
	}

	if (b_id == EH_BUCKET_NUM) {
		split->type = INCOMPLETE_SPLIT;
		split->dest_seg = seg_l1;

		ret = further_append_split_record(seg_l1, seg_l2, 
							split->hashed_prefix, split->depth);

		if (unlikely(ret == -1)) {
			//to dooooooooooooo record in memory fail handle logging
			return -1;
		}
					
		return 0;
	}

	return 1;
}

int eh_split(struct eh_split_context *split) {
	struct eh_split_entry *s_ent;
	EH_CONTEXT *context, c_val;
	struct eh_dir *dir, *dir_head;
	int g_depth, ret;
	bool thread_split;
	SPLIT_PRIORITY priority;

	ret = 0;
	dir = NULL;

	thread_split = split->thread;

	if (thread_split && split->bucket_id != EH_BUCKET_NUM - 1)
		goto eh_split_segment;

	context = get_eh_context(split->hashed_prefix);

	c_val = READ_ONCE(*context);

	dir_head = head_of_eh_dir(c_val);
	g_depth = eh_depth(c_val);

	if (unlikely(g_depth == split->depth)) { //g_depth never < split->depth
		dir_head = expand_eh_directory(context, c_val, 
								dir_head, g_depth, split->depth + 1);

		if (unlikely((void *)dir_head == MAP_FAILED)) {
			//to dooooooooooooo record in memory fail handle logging
			return -1;
		}
		
		if (likely(dir_head))
			g_depth = split->depth + 1;
		else if (split->type == INCOMPLETE_SPLIT)
			goto eh_split_for_next;
		else
			goto eh_split_segment;
	}
	
	if (likely(g_depth > split->depth))
	        dir = base_slot_of_eh_dir(dir_head, 
					split->hashed_prefix, split->depth, g_depth);

eh_split_segment :
	if (split->type == NORMAL_SPLIT)
		ret = eh_segment_normal_split(split, dir, g_depth);

	if (split->type == URGENT_SPLIT)
		ret = eh_segment_urgent_split(split, dir, g_depth);

	if (ret != 0)
		return ret;

	if (unlikely(dir == NULL || split_eh_directory(dir, 
						split->dest_seg, split->depth, g_depth) == -1))
		goto eh_split_for_next;

	reclaim_page(split->target_seg, EH_SEGMENT_SIZE_BITS);

	return 0;

eh_split_for_next :
	priority = thread_split ? URGENT_PRIO : INCOMPLETE_PRIO;
	s_ent = new_split_record(priority);

	if (unlikely((void *)s_ent == MAP_FAILED)) {
		//to dooooooooooooo record in memory fail handle logging
		return -1;
	}

	init_eh_split_entry(s_ent, split->target_seg, split->dest_seg, 
					split->hashed_prefix, split->depth, split->type);

	return 0;
}
