#include "per_thread.h"
#include "background.h"

#include "eh_rehash.h"
#include "eh_alloc.h"

__attribute__((always_inline, optimize("unroll-loops")))
static void prefetch_eh_split_low_bucket(
				struct eh_segment *low_seg, 
				int id) {
	void *addr;
	int i;

	addr = &low_seg->bucket[id];

	for (i = 0; i < EH_PER_BUCKET_CACHELINE; ++i)
		prefech_w0(addr + MUL_2(i, CACHE_LINE_SHIFT));
}

__attribute__((always_inline, optimize("unroll-loops")))
static void prefetch_eh_split_middle_bucket(
				struct eh_two_segment *middle_seg, 
				int id) {
	void *addr;
	int i, id_2;

	id_2 = MUL_2(id, 1);

	for (i = 0; i < 2; ++i) {
		addr = &middle_seg->bucket[id_2 + i];
		prefech_w0(addr);
	}
}

__attribute__((always_inline, optimize("unroll-loops")))
static void prefetch_eh_split_high_bucket(
				struct eh_four_segment *high_seg, 
				int id) {
	void *addr;
	int i, k, id_4;

	id_4 = MUL_2(id, 2);

	for (i = 0; i < 4; ++i) {
		addr = &high_seg->bucket[id_4 + i];

		for (k = 0; k < DIV_2(EH_PER_BUCKET_CACHELINE, 2); ++k)
			prefech_w0(addr + MUL_2(k, CACHE_LINE_SHIFT));
	}
}


__attribute__((always_inline, optimize("unroll-loops")))
void find_dest_bucket_free_slot(
			struct eh_bucket *start_bucket, 
			u16 *dest_idx) {
	int i, j;

	for (i = 0; i < 4; ++i) {
		dest_idx[i] = EH_SLOT_NUM_PER_BUCKET;

		for (j = 0; j < EH_SLOT_NUM_PER_BUCKET; ++j) {
			EH_BUCKET_SLOT slot = READ_ONCE(start_bucket[i].kv[j]);

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
	EH_BUCKET_SLOT old;

	while (eh_slot_deleted(slot_val)) {
		/*if (unlikely(eh_slot_invalid(slot_val)))
			return INVALID_DELETED_EH_SLOT;*/
		old = cas(slot_addr, slot_val, INVALID_DELETED_EH_SLOT);

		if (likely(old == slot_val)) {
			reclaim_chunk((void *)eh_slot_kv_addr(slot_val));
			return INVALID_DELETED_EH_SLOT;
		}

		slot_val = old;
	}

	return slot_val;
}

static int __migrate_eh_slot(
		struct eh_bucket *dest_bucket,
		EH_BUCKET_SLOT *slot_addr,
		EH_BUCKET_SLOT slot_val,
		EH_BUCKET_SLOT new_slot,
		EH_BUCKET_SLOT old_slot,
		u16 *dest_idx) {
	EH_BUCKET_SLOT *addr;
	int i;

__migrate_eh_slot_again :
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
	if (eh_slot_deleted(old_slot)) {
		WRITE_ONCE(*addr, INVALID_DELETED_EH_SLOT);
		(*dest_idx) = i + 1;
		old_slot = clear_eh_delete_slot(slot_addr, old_slot);

		if (eh_slot_invalid_deleted(old_slot))
			return 0;
	}

	slot_val = old_slot;
	new_slot = replace_eh_slot_kv_addr(new_slot, eh_slot_kv_addr(slot_val));

	if (*addr == INVALID_DELETED_EH_SLOT) {
		old_slot = FREE_EH_SLOT;
		goto __migrate_eh_slot_again;
	}

	WRITE_ONCE(*addr, new_slot);

migrate_eh_half_slot :
	old_slot = cas(slot_addr, slot_val, invalidate_eh_slot(slot_val));

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
		u16 *dest_idx) {
	EH_BUCKET_SLOT old_slot, *addr;

	old_slot = FREE_EH_SLOT;
	addr = &dest_bucket->kv[*dest_idx];

	if (unlikely(!cas_bool(addr, FREE_EH_SLOT, new_slot)))
		(*dest_idx) += 1;
	else {
		old_slot = cas(slot_addr, slot_val, invalidate_eh_slot(slot_val));

		if (likely(old_slot == slot_val)) {
			(*dest_idx) += 1;
			return 0;
		}
	}

	return __migrate_eh_slot(dest_bucket, slot_addr, slot_val, new_slot, old_slot, dest_idx);
}


static int __further_migrate_eh_slot(
			struct eh_four_segment *dest, 
			struct eh_two_segment *seg2, 
			EH_BUCKET_SLOT *slot_addr,
			EH_BUCKET_SLOT slot_val,
			u64 hashed_prefix, 
			int depth) {
	struct eh_segment *next_seg;
	struct kv *kv = (struct kv *)eh_slot_kv_addr(slot_val);
	u64 fingerprint;
	EH_BUCKET_SLOT slot, new_slot, *new_addr;
	struct eh_bucket *bucket;
	EH_BUCKET_HEADER header;
	struct eh_split_context split;
	int slot_id, bucket_id, seg_id;

#ifdef DHT_INTEGER
	split.hashed_key = get_kv_prehash64(kv);
#else
	if (depth < 16 - EH_BUCKET_INDEX_BIT) {
		fingerprint = eh_slot_fingerprint16(slot_val);
		hashed_prefix |= SHIFT_OF(fingerprint, PREHASH_KEY_BITS - 16 - EH_BUCKET_INDEX_BIT - depth);
	}

	hashed_prefix &= ~LST48_KEY_MASK;

	split.hashed_key = hashed_prefix | get_kv_prehash48(kv);
#endif

	seg_id = eh_seg_id_in_seg2(split.hashed_key, depth);

	split.target_seg = &seg2->seg[seg_id];
	split.depth = depth + 1;
	split.incomplete = 0;

	seg2 = &dest->two_seg[seg_id];

	bucket_id = eh_seg2_bucket_idx(split.hashed_key, split.depth);
	bucket = &seg2->bucket[bucket_id];

further_next_migrate_eh_slot :
	seg_id = eh_seg_id_in_seg2(split.hashed_key, split.depth);
	next_seg = &seg2->seg[seg_id];
	header = READ_ONCE(bucket->header);

	bucket_id = eh_seg2_bucket_idx(split.hashed_key, split.depth + 1);
	slot_id = 0;

	if (eh_seg_low(header) && likely(!eh_bucket_stayed(header)))
		seg2 = (struct eh_two_segment *)eh_next_high_seg(header);
	else {
		seg2 = add_eh_new_segment(&split, seg2, bucket, kv, -1);

		if ((void *)seg2 == MAP_FAILED)
			return -1;

		if (seg2 == NULL) {
			dest = split.dest_seg;
			seg2 = &dest->two_seg[seg_id];
			bucket = &seg2->bucket[bucket_id];
			new_addr = &bucket->kv[0];
			new_slot = *new_addr;
			goto further_migrate_eh_slot_success;
		}
	}

	bucket = &seg2->bucket[bucket_id];

further_migrate_eh_slot_again :
	fingerprint = hashed_key_fingerprint18(split.hashed_key, split.depth + 2);
	new_slot = make_eh_ext_slot(fingerprint, kv);

	for (; slot_id < EH_SLOT_NUM_PER_BUCKET; ++slot_id) {
		new_addr = &bucket->kv[slot_id];
		slot = READ_ONCE(*new_addr);

		if (eh_slot_free(slot) && 
				eh_slot_free(cas(new_addr, FREE_EH_SLOT, new_slot)))
			goto further_migrate_eh_slot_success;
	}

	split.target_seg = next_seg;
	split.depth += 1;
	goto further_next_migrate_eh_slot;

further_migrate_eh_slot_success :
	slot = cas(slot_addr, slot_val, invalidate_eh_slot(slot_val));

	if (unlikely(slot != slot_val)) {
		if (eh_slot_deleted(slot)) {
			WRITE_ONCE(*new_addr, INVALID_DELETED_EH_SLOT);

			slot_val = clear_eh_delete_slot(slot_addr, slot);

			if (eh_slot_invalid_deleted(slot_val))
				return 0;

			kv = (struct kv *)eh_slot_kv_addr(slot_val);
			slot_id += 1;
			goto further_migrate_eh_slot_again;
		}
		
		slot_val = slot;
		new_slot = replace_eh_slot_kv_addr(new_slot, eh_slot_kv_addr(slot));

		WRITE_ONCE(*new_addr, new_slot);
		goto further_migrate_eh_slot_success;
	}

	return 0;
}

__attribute__((always_inline))
static int further_migrate_eh_slot(
			struct eh_four_segment *dest, 
			struct eh_two_segment *seg2, 
			EH_BUCKET_SLOT *slot_addr,
			EH_BUCKET_SLOT slot_val,
			u64 hashed_prefix, 
			int bucket_idx, int depth) {
#ifndef DHT_INTEGER
	hashed_prefix |= SHIFT_OF(bucket_idx, 
					PREHASH_KEY_BITS - EH_BUCKET_INDEX_BIT - depth);
#endif
	return __further_migrate_eh_slot(dest, seg2, slot_addr, 
						slot_val, hashed_prefix, depth);
}

__attribute__((always_inline))
static int migrate_eh_int_slot(
		struct eh_bucket *dest_bucket,
		EH_BUCKET_SLOT *slot_addr,
		EH_BUCKET_SLOT slot_val,
		int depth, u16 *dest_idx) {
	EH_BUCKET_SLOT new_slot;
	struct kv *kv = (struct kv *)eh_slot_kv_addr(slot_val);
	u64 fingerprint18;

#ifdef DHT_INTEGER
	fingerprint18 = SHIFT_OF(get_kv_prehash64(kv), 
				depth + EH_BUCKET_INDEX_BIT + 2);
	fingerprint18 >>= (PREHASH_KEY_BITS - 18);
#else
	fingerprint18 = (eh_slot_fingerprint16(slot_val) & INTERVAL(0, 13)) << 4;
	fingerprint18 |= 
		(SHIFT_OF(get_kv_prehash48(kv), depth + EH_BUCKET_INDEX_BIT + 16) 
							>> (PREHASH_KEY_BITS - 4));
#endif

	new_slot = make_eh_ext_slot(fingerprint18, kv);

	return migrate_eh_slot(dest_bucket, slot_addr, slot_val, new_slot, dest_idx);	
}


__attribute__((always_inline))
static int migrate_eh_ext_slot(
		struct eh_bucket *dest_bucket,
		EH_BUCKET_SLOT *slot_addr,
		EH_BUCKET_SLOT slot_val,
		u16 *dest_idx) {
	u64 fingerprint = shift_eh_ext_fing(slot_val);
	struct kv *kv = (struct kv *)eh_slot_kv_addr(slot_val);
	EH_BUCKET_SLOT new_slot = make_eh_slot(fingerprint, kv);

	return migrate_eh_slot(dest_bucket, slot_addr, slot_val, new_slot, dest_idx);	
}

__attribute__((always_inline))
static int migrate_eh_undo_slot(
			struct eh_bucket *bucket,
			struct eh_bucket *high_bucket, 
			struct eh_two_segment *seg2, 
			struct eh_four_segment *dest_seg, 
			u64 hashed_prefix, int depth, 
			int bucket_id, u16 *dest_ind, 
			u16 *undo_ind, int undo_count) {
	EH_BUCKET_SLOT slot_val, *slot_addr;
	int i, d_id;
			
	for (i = 0; i < undo_count; ++i) {
		slot_addr = &bucket->kv[undo_ind[i]];
		slot_val = clear_eh_delete_slot(slot_addr, READ_ONCE(*slot_addr));

		if (unlikely(eh_slot_invalid_deleted(slot_val)))
			continue;

		d_id = eh_slot_fingerprint2(slot_val);

		if (dest_ind[d_id] < EH_SLOT_NUM_PER_BUCKET && 
			likely(migrate_eh_int_slot(&high_bucket[d_id], slot_addr, 
							slot_val, depth, &dest_ind[d_id])) == 0)
			continue;

		if (unlikely(further_migrate_eh_slot(dest_seg, seg2, slot_addr, 
							slot_val, hashed_prefix, bucket_id, depth)) == -1)
			return -1;
	}

	return 0;
}


static struct eh_two_segment *split_eh_segment(
				struct eh_segment *target_seg, 
				struct eh_four_segment *dest_seg, 
				u64 hashed_prefix, int depth) {
	EH_BUCKET_HEADER header, b_header, m_header, h;
	struct eh_two_segment *seg_2;
	struct eh_bucket *bucket, *bucket_m, *bucket_h;
	EH_BUCKET_SLOT slot_val, *slot_addr;
	u16 dest_ind[4];
	u16 undo_ind[4];
	int b_id, s_id, d_id, undo_count;

	header = READ_ONCE(target_seg->bucket[0].header);
	seg_2 = (struct eh_two_segment *)eh_next_high_seg(header);

	b_header = set_eh_seg_splited(header);
	m_header = set_eh_seg_low(&dest_seg->two_seg[0]);

	for (b_id = 0; b_id < EH_BUCKET_NUM; ++b_id) {
		if (b_id != EH_BUCKET_NUM - 1) {
			prefetch_eh_split_low_bucket(target_seg, b_id + 1);
			prefetch_eh_split_high_bucket(dest_seg, b_id + 1);
		}

		if (b_id == DIV_2(EH_BUCKET_NUM, 1))
			m_header = set_eh_seg_low(&dest_seg->two_seg[1]);

		bucket = &target_seg->bucket[b_id];

		h = READ_ONCE(bucket->header);

		if (eh_seg_splited(h))
			continue;

		prefetch_eh_split_middle_bucket(seg_2, b_id);

		bucket_h = &dest_seg->bucket[MUL_2(b_id, 2)];
		bucket_m = &seg_2->bucket[MUL_2(b_id, 1)];
		undo_count = 0;

		find_dest_bucket_free_slot(bucket_h, &dest_ind[0]);

		for (s_id = 0; s_id < EH_SLOT_NUM_PER_BUCKET; ++s_id) {
			slot_addr = &bucket->kv[s_id];
			slot_val = READ_ONCE(*slot_addr);

			if (unlikely(eh_slot_invalid(slot_val)))
				continue;

			if (unlikely(eh_slot_free(slot_val))) {
				slot_val = cas(slot_addr, FREE_EH_SLOT, END_EH_SLOT);

				if (likely(eh_slot_free(slot_val)))
					break;
			}

			slot_val = clear_eh_delete_slot(slot_addr, slot_val);

			if (eh_slot_invalid_deleted(slot_val))
				continue;

			if (eh_slot_ext(slot_val)) {
				d_id = eh_slot_fingerprint2(slot_val);

				if (dest_ind[d_id] < EH_SLOT_NUM_PER_BUCKET)
					if (likely(migrate_eh_ext_slot(
								&bucket_h[d_id], slot_addr, 
								slot_val, &dest_ind[d_id])) == 0)
						continue;
			}

			prefech_r0((void *)eh_slot_kv_addr(slot_val));
			undo_ind[undo_count++] = s_id;

			if (undo_count != 4)
				continue;

			if (unlikely(migrate_eh_undo_slot(bucket, bucket_h, seg_2, 
						dest_seg, hashed_prefix, depth, b_id, &dest_ind[0], 
						&undo_ind[0], 4) != 0))
				return (struct eh_two_segment *)MAP_FAILED;

			undo_count = 0;
		}

		if (undo_count && unlikely(migrate_eh_undo_slot(bucket, bucket_h,
							seg_2, dest_seg, hashed_prefix, depth, b_id, 
							&dest_ind[0], &undo_ind[0], undo_count) != 0))
			return (struct eh_two_segment *)MAP_FAILED;

		WRITE_ONCE(bucket_m[0].header, m_header);
		WRITE_ONCE(bucket_m[1].header, m_header);

		release_fence();
		WRITE_ONCE(bucket->header, b_header);
	}

	return seg_2;
}

__attribute__((always_inline))
static struct eh_two_segment *analyze_eh_split_entry(
                    	struct eh_split_entry *split_ent,
			struct eh_split_context *split,
                    	int high_prio) {
	uintptr_t t_ent, d_ent;
	struct eh_two_segment *ret_seg;

	t_ent = READ_ONCE(split_ent->target);
	d_ent = split_ent->destination;

	if (!high_prio) {
        	if (unlikely(t_ent == INVALID_EH_SPLIT_TARGET))
            		return (struct eh_two_segment *)-1;

        	if (unlikely(!cas_bool(&split_ent->target, t_ent, INVALID_EH_SPLIT_TARGET)))
            		return (struct eh_two_segment *)-1;
	}

	split->depth = eh_split_target_depth(t_ent);// below 48
	split->target_seg = (struct eh_segment *)eh_split_target_seg(t_ent);
	split->dest_seg = (struct eh_four_segment *)eh_split_dest_seg(d_ent);

	if (unlikely(eh_split_incomplete_target(t_ent))) {
		ret_seg = (struct eh_two_segment *)eh_next_high_seg(
						split->target_seg->bucket[0].header);
		split->incomplete = 1;
	} else {
		prefetch_eh_split_low_bucket(split->target_seg, 0);
		prefetch_eh_split_high_bucket(split->dest_seg, 0);
		ret_seg = NULL;
		split->incomplete = 0;
	}

	split->hashed_key = eh_split_prefix(t_ent, d_ent, split->depth);

	return ret_seg;
}
/*
__attribute__((always_inline))
static void hook_eh_seg_split_entry(
				struct eh_four_segment *new_seg, 
				struct eh_split_entry *split_ent,
				int high_prio) {
	EH_BUCKET_HEADER header = ((high_prio || split_ent == NULL) ? 
				INITIAL_EH_BUCKET_HP_HEADER : set_eh_split_entry(split_ent));
	
	new_seg->two_seg[0].bucket[0].header = 
					new_seg->two_seg[1].bucket[0].header = header;
}*/

__attribute__((always_inline))
static void rehook_eh_seg_split_entry(
			struct eh_four_segment *new_seg) {
	EH_BUCKET_HEADER header, *h_addr;
	int seg_id;

	for (seg_id = 0; seg_id < 2; ++seg_id) {
		h_addr = &new_seg->two_seg[seg_id].bucket[0].header;
		header = READ_ONCE(*h_addr);

		if (!eh_seg_low(header) && header != INITIAL_EH_BUCKET_HP_HEADER)
			cas(h_addr, header, INITIAL_EH_BUCKET_HP_HEADER);
	}
}

static int further_append_split_record(
				EH_BUCKET_HEADER header, 
				struct eh_segment *seg,
				u64 hashed_prefix, int depth) {
	struct eh_split_context split;
	struct eh_split_entry *s_entry;
	struct eh_four_segment *next_seg;
	EH_BUCKET_HEADER h1, h2, *h1_addr, *h2_addr;
	RECORD_POINTER rp;
	int high_prio;

	next_seg = (struct eh_four_segment *)eh_next_high_seg(header);

	split.target_seg = seg;
	split.dest_seg = next_seg;
	split.depth = depth;
	split.hashed_key = hashed_prefix;
	split.incomplete = 0;

	high_prio = 1;

	if (unlikely(eh_seg_extra(header))) {
		h1_addr = &next_seg->two_seg[0].bucket[0].header;
		h2_addr = &next_seg->two_seg[1].bucket[0].header;

		h1 = READ_ONCE(*h1_addr);
		h2 = READ_ONCE(*h2_addr);

		if (h1 == INITIAL_EH_BUCKET_HP_HEADER && 
				h2 == INITIAL_EH_BUCKET_HP_HEADER)
			high_prio = 0;
	}

try_further_append_split_record :
	s_entry = append_split_record(&rp, &split, high_prio);

	if (unlikely(s_entry == (struct eh_split_entry *)MAP_FAILED)) {
		//to dooooooooooooo record in memory fail handle logging
		return -1;
	}

	if (unlikely(high_prio == 0)) {
		h1 = cas(h1_addr, INITIAL_EH_BUCKET_HP_HEADER, 
						set_eh_split_entry(s_entry));
		if (unlikely(h1 != INITIAL_EH_BUCKET_HP_HEADER)) {
			high_prio = 1;
			goto try_further_append_split_record;
		}

		h2 = cas(h2_addr, INITIAL_EH_BUCKET_HP_HEADER, 
						set_eh_split_entry(s_entry));

		h1 = READ_ONCE(*h1_addr);

		if (h2 == INITIAL_EH_BUCKET_HP_HEADER && eh_seg_low(h1))
			cas(h2_addr, set_eh_split_entry(s_entry), 
						INITIAL_EH_BUCKET_HP_HEADER);
		else if (h2 != INITIAL_EH_BUCKET_HP_HEADER && 
					h1 == set_eh_split_entry(s_entry) && 
					cas_bool(h1_addr, h1, INITIAL_EH_BUCKET_HP_HEADER)) {
			high_prio = 1;
			goto try_further_append_split_record;
		}
	}

	commit_split_record(rp, high_prio);
	return 0;
}

__attribute__((always_inline))
static int unhook_eh_seg_split_entry(
			struct eh_two_segment *seg2,
			struct eh_four_segment *new_seg,
			u64 hashed_prefix, 
			int depth) {
	EH_BUCKET_HEADER old, h1, h2, header, *h_addr;
	struct eh_split_context split;
	int seg_id, high_prio, ret = 0;

	for (seg_id = 0; seg_id < 2; ++seg_id) {
		h_addr = &new_seg->two_seg[seg_id].bucket[0].header;
		header = READ_ONCE(*h_addr);

		if (likely(!eh_seg_low(header))) {
			old = cas(h_addr, header, INITIAL_EH_BUCKET_HEADER);

			if (likely(old == header))
				continue;

			header = old;
		}

		ret |= further_append_split_record(header, &seg2->seg[seg_id], 
				hashed_prefix | SHIFT_OF(seg_id, PREHASH_KEY_BITS - 1 - depth), 
				depth + 1);
	}

	return ret;	
}

static int __eh_split(
		EH_CONTEXT *contex,
		struct eh_split_context *split, 
		struct eh_two_segment *seg2) {
	struct eh_split_entry *split_ent;
	EH_CONTEXT c_val;
	struct eh_dir *dir, *dir_head;
	RECORD_POINTER rp;
	int g_depth, ret = 0;

	c_val = READ_ONCE(*contex);

	dir_head = head_of_eh_dir(c_val);
	g_depth = eh_depth(c_val);

	if (unlikely(g_depth == split->depth)) { //g_depth never < split->depth
		dir_head = expand_eh_directory(contex, c_val, dir_head, 
						g_depth, split->depth + 1);

		if (unlikely((void *)dir_head == MAP_FAILED)) {
			ret = -1;
			goto eh_split_for_next;
		}

		if (likely(dir_head))
			g_depth = split->depth + 1;
		else {
			if (split->incomplete)
				goto eh_split_for_next;
				
			dir_head = head_of_eh_dir(c_val);
		}
	}

	dir = base_slot_of_eh_dir(dir_head, split->hashed_key, 
						split->depth, g_depth);

	if (likely(!split->incomplete)) {
		prefech_r0(dir);

		seg2 = split_eh_segment(split->target_seg, split->dest_seg, 
						split->hashed_key, split->depth);

		if (unlikely(!seg2))
        	goto eh_split_for_next;

		if (unlikely((void *)seg2 == MAP_FAILED)) {
			ret = -1;
			goto eh_split_for_next;
		}
	}

	if (unlikely(g_depth == split->depth || split_eh_directory(dir, 
			&seg2->seg[0], &seg2->seg[1], split->depth, g_depth) == -1)) {
		split->incomplete = 1;
		goto eh_split_for_next;
	}

	reclaim_page(split->target_seg, EH_SEGMENT_SIZE_BITS);

	return unhook_eh_seg_split_entry(seg2, split->dest_seg, 
						split->hashed_key, split->depth);

eh_split_for_next :
	rehook_eh_seg_split_entry(split->dest_seg);
	split_ent = append_split_record(&rp, split, 1);

	if (unlikely((void *)split_ent == MAP_FAILED)) {
		//to dooooooooooooo record in memory fail handle logging
		return -1;
	}

	commit_split_record(rp, 1);
	return ret;
}

int eh_split(struct eh_split_entry *split_ent, int high_prio) {
	EH_CONTEXT *contex;
	struct eh_split_context split;
	struct eh_two_segment *seg2;

	seg2 = analyze_eh_split_entry(split_ent, &split, high_prio);

	if (unlikely(seg2 == (struct eh_two_segment *)-1))
		return 0;

	contex = get_eh_context(split.hashed_key);

	return  __eh_split(contex, &split, seg2);
}


__attribute__((always_inline))
static void init_eh_seg_new_slot(
			struct eh_four_segment *new_seg, 
			EH_BUCKET_HEADER hook_header,
			struct kv *kv, u64 hashed_key, 
			int depth) {
	u64 fingerprint = hashed_key_fingerprint18(hashed_key, depth + 2);
	int bucket_id = eh_seg4_bucket_idx(hashed_key, depth);
	
	new_seg->bucket[bucket_id].kv[0] = make_eh_ext_slot(fingerprint, kv);

	new_seg->two_seg[0].bucket[0].header = hook_header;
	new_seg->two_seg[1].bucket[0].header = hook_header;
}

__attribute__((always_inline))
static int try_init_eh_seg_new_slot(
				struct eh_four_segment *new_seg, 
				struct kv *kv, u64 hashed_key, 
				int depth) {
	u64 fingerprint = hashed_key_fingerprint18(hashed_key, depth + 2);
	int bucket_id = eh_seg4_bucket_idx(hashed_key, depth);
	EH_BUCKET_SLOT old, new = make_eh_ext_slot(fingerprint, kv);

	old = cas(&new_seg->bucket[bucket_id].kv[0], FREE_EH_SLOT, new);
	
	if (likely(eh_slot_free(old)))
		return 0;
	
	return 1;
}


struct eh_two_segment *add_eh_new_segment(
				struct eh_split_context *split,
				struct eh_two_segment *seg,
				struct eh_bucket *bucket, 
				struct kv *kv, int high_prio) {
	EH_BUCKET_HEADER old, new, header, hook_header;
	struct eh_split_entry *s_ent;
	RECORD_POINTER rp;
	struct eh_four_segment *already_seg;
	struct eh_two_segment *ret_seg2;
	int s2_id = eh_seg2_id_in_seg4(split->hashed_key, split->depth);
	int bucket_id = eh_seg_bucket_idx(split->hashed_key, split->depth);

	header = READ_ONCE(seg->bucket[0].header);

	if (unlikely(eh_seg_low(header))) {
		if (&seg->bucket[0] == bucket) {
			header = clean_eh_bucket_stayed(header);
			WRITE_ONCE(bucket->header, header);
		}

		already_seg = (struct eh_four_segment *)eh_next_high_seg(header);
		ret_seg2 = &already_seg->two_seg[s2_id];
		goto eh_new_segment_existed;
	}

	split->dest_seg = already_seg = (struct eh_four_segment *)alloc_eh_seg();

	if (unlikely((void *)already_seg == MAP_FAILED))
		return (struct eh_two_segment *)MAP_FAILED;

try_add_new_eh_seg :
	new = set_eh_seg_low(&already_seg->two_seg[0]);
	hook_header = INITIAL_EH_BUCKET_HP_HEADER;

	if (unlikely(high_prio == -1))
		new = set_eh_seg_extra(new);
	else if (header == INITIAL_EH_BUCKET_HEADER) {
		s_ent = append_split_record(&rp, split, high_prio);

		if (unlikely((void *)s_ent == MAP_FAILED)) {
			free_page_aligned(split->dest_seg, EH_FOUR_SEGMENT_SIZE);
			return (struct eh_two_segment *)MAP_FAILED;
		}

		if (!high_prio)
			hook_header = set_eh_split_entry(s_ent);
	}

	init_eh_seg_new_slot(already_seg, hook_header, 
							kv, split->hashed_key, split->depth);

	new = set_eh_bucket_stayed(new);
	old = cas(&seg->bucket[0].header, header, new);

	if (old == header) {
		ret_seg2 = NULL;

		if (unlikely(high_prio == -1) || 
						header == INITIAL_EH_BUCKET_HP_HEADER)
			goto finish_add_eh_new_segment;

		if (header != INITIAL_EH_BUCKET_HEADER) {
			s_ent = (struct eh_split_entry *)eh_lp_split_entry(header);

			if (unlikely(upgrade_split_record(&rp, s_ent) != 0))
				goto finish_add_eh_new_segment;
					
			high_prio = 1;
		}

		commit_split_record(rp, high_prio);
		goto finish_add_eh_new_segment;
	}


	if (!eh_seg_low(old)) {
		header = old;
		goto try_add_new_eh_seg;
	}

	already_seg = (struct eh_four_segment *)eh_next_high_seg(old);
	ret_seg2 = &already_seg->two_seg[s2_id];

	free_page_aligned(split->dest_seg, EH_FOUR_SEGMENT_SIZE);

eh_new_segment_existed :
	if (likely(try_init_eh_seg_new_slot(
			already_seg, kv, split->hashed_key, split->depth) == 0)) {
		ret_seg2 = NULL;
		split->dest_seg = already_seg;
	}

finish_add_eh_new_segment :
	new = set_eh_seg_low(&already_seg->two_seg[s2_id]);

	release_fence();
	WRITE_ONCE(bucket->header, new);

	return ret_seg2;
}
