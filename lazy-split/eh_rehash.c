#include "per_thread.h"
#include "background.h"

#include "eh_rehash.h"
#include "eh_alloc.h"


__attribute__((always_inline))
static void hook_eh_seg_split_entry(
				struct eh_four_segment *new_seg, 
				struct eh_split_entry *split_ent,
				int high_prio) {
	if (!high_prio) {
		EH_BUCKET_HEADER header = set_eh_split_entry(split_ent);
		new_seg->two_seg[0].bucket[0].header = set_eh_split_entry_left(header);
		new_seg->two_seg[1].bucket[0].header = header;
	}
}

__attribute__((always_inline))
static void unhook_eh_seg_split_entry(
				struct eh_four_segment *new_seg, 
				int high_prio) {
	if (!high_prio) {
		EH_BUCKET_HEADER header, *addr;

		addr = &new_seg->two_seg[1].bucket[0].header;
		header = READ_ONCE(*addr);

		if (!eh_seg_low(header))
			cas(addr, header, INITIAL_EH_BUCKET_HEADER);

		addr = &new_seg->two_seg[0].bucket[0].header;
		header = READ_ONCE(*addr);

		if (!eh_seg_low(header))
			cas(addr, header, INITIAL_EH_BUCKET_HEADER);
	}
}

__attribute__((always_inline))
static void init_eh_seg_new_slot(
				struct eh_four_segment *new_seg, 
				struct kv *kv, u64 hashed_key, 
				int depth) {
	u64 fingerprint = hashed_key_fingerprint18(hashed_key, depth + 2);
	int bucket_id = eh_seg4_bucket_idx(hashed_key, depth);
	
	new_seg->bucket[bucket_id].kv[0] = make_eh_ext_slot(fingerprint, kv);
}

struct eh_two_segment *add_eh_new_segment(
				struct eh_split_context *split,
				struct eh_two_segment *seg,
				struct kv *kv, int high_prio) {
	EH_BUCKET_HEADER header, *header_addr;
	struct eh_split_entry *s_ent;
	RECORD_POINTER rp;
	struct eh_two_segment *ret_seg;
	struct eh_four_segment *already_seg;
	int s2_id;

	header_addr = &seg->seg[0].bucket[0].header;

	header = READ_ONCE(*header_addr);

	if (unlikely(eh_seg_low(header))) {
		s2_id = eh_seg2_id_in_seg4(split->hashed_key, split->depth);
		split->dest_seg = (struct eh_four_segment *)eh_next_high_seg(header);
		return &split->dest_seg->two_seg[s2_id];
	}

	split->dest_seg = (struct eh_four_segment *)alloc_eh_seg();

	if (unlikely((void *)(split->dest_seg) == MAP_FAILED))
		return (struct eh_two_segment *)MAP_FAILED;
	
	s_ent = append_split_record(&rp, split, 0, high_prio);

	if (unlikely((void *)s_ent == MAP_FAILED))
		ret_seg = (struct eh_two_segment *)MAP_FAILED;
	else {
		EH_BUCKET_HEADER old, new;

		hook_eh_seg_split_entry(split->dest_seg, s_ent, high_prio);
		init_eh_seg_new_slot(split->dest_seg, kv, split->hashed_key, split->depth);

		new = set_eh_seg_low(&split->dest_seg->two_seg[0]);

	try_add_new_eh_seg :
		old = cas(header_addr, header, new);

		if (old == header) {
			new = set_eh_seg_low(&split->dest_seg->two_seg[1]);
			header_addr = &seg->seg[1].bucket[0].header;
			cas(header_addr, INITIAL_EH_BUCKET_HEADER, new);
			commit_split_record(rp, high_prio);

			return NULL;
		}

		if (!eh_seg_low(old)) {
			header = old;
			goto try_add_new_eh_seg;
		}

		s2_id = eh_seg2_id_in_seg4(split->hashed_key, split->depth);
		already_seg = (struct eh_four_segment *)eh_next_high_seg(old);
		ret_seg = &already_seg->two_seg[s2_id];
	}

	free_page_aligned(split->dest_seg, EH_FOUR_SEGMENT_SIZE);

	split->dest_seg = already_seg;

	return ret_seg;
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

static int __migrate_eh_slot(
		struct eh_bucket *dest_bucket,
		EH_BUCKET_SLOT *slot_addr,
		EH_BUCKET_SLOT slot_val,
		EH_BUCKET_SLOT new_slot,
		EH_BUCKET_SLOT old_slot,
		u16 *dest_idx) {
	EH_BUCKET_SLOT *addr;
	int i = *dest_idx;

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
	new_slot = replace_eh_slot_kv_addr(new_slot, eh_slot_kv_addr(old_slot));

	if (eh_slot_deleted(old_slot))
		new_slot = delete_eh_slot(new_slot);

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
	u64 fingerprint = eh_slot_fingerprint16(slot_val);
	EH_BUCKET_SLOT slot, new_slot, *new_addr;
	struct eh_bucket *bucket;
	EH_BUCKET_HEADER header;
	struct eh_split_context split;
	int i, bucket_id, seg_id;

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
	split.recheck = 1;

	seg2 = &dest->two_seg[seg_id];

further_next_migrate_eh_slot :
	seg_id = eh_seg_id_in_seg2(split.hashed_key, split.depth);
	next_seg = &seg2->seg[seg_id];
	header = READ_ONCE(next_seg->bucket[0].header);

	bucket_id = eh_seg2_bucket_idx(split.hashed_key, split.depth + 1);

	if (eh_seg_low(header)) {
		dest = (struct eh_four_segment *)eh_next_high_seg(header);
		seg2 = &dest->two_seg[seg_id];
	} else {
		seg2 = add_eh_new_segment(&split, seg2, kv, 1);

		if ((void *)seg2 == MAP_FAILED)
			return -1;

		if (seg2 == NULL) {
			dest = split.dest_seg;
			seg2 = &dest->two_seg[seg_id];
			new_addr = &seg2->bucket[bucket_id].kv[0];
			new_slot = *new_addr;
			goto further_migrate_eh_slot_success;
		}

		if (seg2 == &split.dest_seg->two_seg[1])
			cas(&next_seg->bucket[0].header, 
						INITIAL_EH_BUCKET_HEADER, set_eh_seg_low(seg2));
	}

	fingerprint = hashed_key_fingerprint18(split.hashed_key, split.depth + 2);
	new_slot = make_eh_ext_slot(fingerprint, kv);

	bucket = &seg2->bucket[bucket_id];

	for (i = 0; i < EH_SLOT_NUM_PER_BUCKET; ++i) {
		new_addr = &bucket->kv[i];
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
		slot_val = slot;
		new_slot = replace_eh_slot_kv_addr(new_slot, eh_slot_kv_addr(slot));

		if (eh_slot_deleted(slot))
			new_slot = delete_eh_slot(new_slot);

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
static EH_BUCKET_SLOT clear_eh_delete_slot(
					EH_BUCKET_SLOT *slot_addr,
					EH_BUCKET_SLOT slot_val) {
	EH_BUCKET_SLOT old;

	/*if (unlikely(eh_slot_invalid(slot_val)))
		return INVALID_EH_SLOT;*/

	old = cas(slot_addr, slot_val, INVALID_DELETED_EH_SLOT);

	if (likely(old == slot_val)) {
		reclaim_chunk((void *)eh_slot_kv_addr(slot_val));
		return INVALID_DELETED_EH_SLOT;
	}

	return old;
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

__attribute__((always_inline, optimize("unroll-loops")))
static void prefetch_eh_split_bucket(
				struct eh_segment *low_seg, 
				struct eh_four_segment *high_seg, 
				int id) {
	void *addr = &low_seg->bucket[id];
	int i, j, k;

	for (i = 0; i < EH_PER_BUCKET_CACHELINE; ++i)
		prefech_r0(addr + MUL_2(i, CACHE_LINE_SHIFT));

	id = MUL_2(id, 2);

	for (j = 0; j < 4; ++j) {
		void *addr2 = &high_seg->bucket[id + j];

		for (k = 0; k < DIV_2(EH_PER_BUCKET_CACHELINE, 2); ++k)
			prefech_r0(addr2 + MUL_2(k, CACHE_LINE_SHIFT));
	}
}

static struct eh_two_segment *split_eh_segment(
						struct eh_segment *target_seg, 
						struct eh_four_segment *dest_seg, 
						u64 hashed_prefix,
						int depth) {
	EH_BUCKET_HEADER b_header;
	struct eh_two_segment *seg_2;
	struct eh_bucket *bucket, *bucket_h;
	EH_BUCKET_SLOT slot_val, *slot_addr;
	u16 dest_ind[4];
	u16 undo_ind[4];
	int b_id, s_id, d_id, undo_count, i;

	b_header = target_seg->bucket[0].header;
	seg_2 = (struct eh_two_segment *)eh_next_high_seg(b_header);
	b_header = set_eh_seg_splited(b_header);

	for (b_id = 0; b_id < EH_BUCKET_NUM; ++b_id) {
		if (b_id != EH_BUCKET_NUM - 1)
			prefetch_eh_split_bucket(target_seg, dest_seg, b_id + 1);

		bucket = &target_seg->bucket[b_id];
		bucket_h = &dest_seg->bucket[MUL_2(b_id, 2)];
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

		try_clear_eh_delete_slot :
			if (unlikely(eh_slot_deleted(slot_val))) {
				slot_val = clear_eh_delete_slot(slot_addr, slot_val);

				if (eh_slot_invalid_deleted(slot_val))
					continue;

				goto try_clear_eh_delete_slot;
			}

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

		migrate_eh_undo_slot :
		//printf("%d\n", undo_count);
			for (i = 0; i < undo_count; ++i) {
				slot_addr = &bucket->kv[undo_ind[i]];
				slot_val = READ_ONCE(*slot_addr);
				d_id = eh_slot_fingerprint2(slot_val);

				if (dest_ind[d_id] < EH_SLOT_NUM_PER_BUCKET) {
					if (likely(migrate_eh_int_slot(&bucket_h[d_id], slot_addr, 
									slot_val, depth, &dest_ind[d_id])) == 0)
						continue;			
				}

				if (unlikely(further_migrate_eh_slot(dest_seg, seg_2, slot_addr, 
								slot_val, hashed_prefix, b_id, depth)) == -1)
					return NULL;
			}

			if (undo_count != 4) {
				undo_count = 0;
				break;
			}

			undo_count = 0;
		}

		if (undo_count)
			goto migrate_eh_undo_slot;

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

	t_ent = READ_ONCE(split_ent->target);

	if (!high_prio) {
        if (unlikely(t_ent == INVALID_EH_SPLIT_TARGET))
            return (struct eh_two_segment *)-1;

        if (unlikely(!cas_bool(&split_ent->target, t_ent, INVALID_EH_SPLIT_TARGET)))
            return (struct eh_two_segment *)-1;
    }

    split->depth = eh_split_target_depth(t_ent);// below 48

    if (unlikely(eh_split_incomplete_target(t_ent))) {
		split->hashed_key = split_ent->hashed_key;
        return (struct eh_two_segment *)eh_split_target_seg(t_ent);
    }

	d_ent = split_ent->destination;

	split->target_seg = (struct eh_segment *)eh_split_target_seg(t_ent);
    split->dest_seg = (struct eh_four_segment *)eh_split_dest_seg(d_ent);

	prefetch_eh_split_bucket(split->target_seg, split->dest_seg, 0);

    split->hashed_key = eh_split_prefix(t_ent, d_ent, split->depth);
	split->recheck = eh_split_target_need_recheck(t_ent);

	return NULL;
}

int eh_split(struct eh_split_entry *split_ent, int high_prio) {
    EH_CONTEXT c_val, *contex;
    struct eh_split_context split;
    struct eh_two_segment *seg2;
    struct eh_dir *dir, *dir_head;
	RECORD_POINTER rp;
    int g_depth, incomplete = 0, lack = 0;

    seg2 = analyze_eh_split_entry(split_ent, &split, high_prio);

    if (unlikely(seg2 == (struct eh_two_segment *)-1))
        return 0;

    contex = get_eh_context(split.hashed_key);
    c_val = READ_ONCE(*contex);

    g_depth = eh_depth(c_val);
    dir_head = head_of_eh_dir(c_val);
    dir = base_slot_of_eh_dir(dir_head, split.hashed_key, split.depth, g_depth);

	if (unlikely(seg2))
		goto dir_update_eh_split;

    prefech_r0(dir);

    unhook_eh_seg_split_entry(split.dest_seg, high_prio);

    if (split.recheck && split.target_seg != seg_of_eh_dir(dir))
		goto eh_split_for_next;

    seg2 = split_eh_segment(split.target_seg, split.dest_seg, 
									split.hashed_key, split.depth);

    if (unlikely(seg2 == NULL))
        goto lack_memory_eh_split;

dir_update_eh_split :
	if (unlikely(g_depth <= split.depth)) {
		dir_head = expand_eh_directory(contex, dir_head, g_depth, split.depth + 1);

		if (unlikely((void *)dir_head == MAP_FAILED))
			goto lack_memory_eh_split;

		if (unlikely(dir_head == NULL))
			goto eh_incomplete_split_for_next;

		g_depth = split.depth + 1;
		dir = base_slot_of_eh_dir(dir_head, split.hashed_key, split.depth, g_depth);
	}

	if (unlikely(split_eh_directory(dir, 
			&seg2->seg[0], &seg2->seg[1], split.depth, g_depth) == -1))
		goto eh_incomplete_split_for_next;

	reclaim_page(split.target_seg, EH_SEGMENT_SIZE_BITS);
	return 0;

lack_memory_eh_split :
	free_reclaim_memory();

	if (seg2) {
	eh_incomplete_split_for_next :
		split.target_seg = (struct eh_segment *)seg2;
		incomplete = 1;
	}

eh_split_for_next :
	split_ent = append_split_record(&rp, &split, incomplete, 1);

	if (unlikely((void *)split_ent == MAP_FAILED)) {
		if (lack++ == 10)
			return -1;

		goto lack_memory_eh_split;	
	}

	commit_split_record(rp, 1);
    return 0;
}