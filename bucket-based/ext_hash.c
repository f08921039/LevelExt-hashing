#include "ext_hash.h"
#include "per_thread.h"

#include "background.h"

u64 eh_contex;


#define EH_BUCKET_CACHELINE_PREFETCH_STEP	4



__attribute__((always_inline, optimize("unroll-loops")))
static void prefetch_eh_bucket_head(void *bucket) {
	int i;

	for (i = 0; i < EH_BUCKET_CACHELINE_PREFETCH_STEP; ++i) {
		prefech_r0(bucket);
		bucket = bucket + CACHE_LINE_SIZE;
	}
}

__attribute__((always_inline))
static void prefetch_eh_bucket_step(void *bucket1_end, void *bucket2_begin, void *current_addr) {
	current_addr = current_addr + (EH_BUCKET_CACHELINE_PREFETCH_STEP << CACHE_LINE_SHIFT);
	
	if (current_addr < bucket1_end)
		prefech_r0(current_addr);
	else if (bucket2_begin)
		prefech_r0(bucket2_begin + (current_addr - bucket1_end));
}

__attribute__((always_inline))
static int get_two_eh_seg_ptr(struct eh_dir_entry *dir_ent,
								struct eh_dir_entry *ent,
								struct eh_segment **high_seg, 
								struct eh_segment **low_seg) {
	while (1) {
		ent->ent1 = READ_ONCE(dir_ent->ent1);
		acquire_fence();

		ent->ent2 = READ_ONCE(dir_ent->ent2);
		acquire_fence();

		if (likely(!eh_seg_locked(ent->ent1)) && 
				likely(ent->ent1 == READ_ONCE(dir_ent->ent1))) {
			*high_seg = (struct eh_segment *)extract_eh_seg_addr(ent->ent1);
			*low_seg = (struct eh_segment *)extract_eh_seg_addr(ent->ent2);
			return eh_seg_depth(ent->ent1);
		}
	}
}

__attribute__((always_inline))
static int get_two_eh_dir_entry(struct eh_dir_entry *dir_ent,
									struct eh_dir_entry *ent) {
	while (1) {
		ent->ent1 = READ_ONCE(dir_ent->ent1);
		acquire_fence();

		ent->ent2 = READ_ONCE(dir_ent->ent2);
		acquire_fence();

		if (likely(!eh_seg_spliting(ent->ent1)) && 
					likely(ent->ent1 == READ_ONCE(dir_ent->ent1)))
			return eh_seg_depth(ent->ent1);
	}
}


static int extend_eh_directory(u64 contex, int g_depth) {
	u64 t_ent, total_num, num, new_dir_val;
	struct eh_dir_entry ent;
	struct eh_dir_entry *new_dir, *old_dir, *old_dir_end;
	int l_depth, i;

	old_dir = (struct eh_dir_entry *)extract_eh_dir_addr(contex);

	old_dir_end = old_dir + (1UL << g_depth);

	g_depth += EH_EXTENSION_STEP;

	total_num = sizeof(struct eh_dir_entry) << g_depth;
	new_dir = (struct eh_dir_entry *)malloc_prefault_page_aligned(total_num);

	if (unlikely((void *)new_dir == MAP_FAILED))
		return -1;

	if (!cas_bool(&eh_contex, contex, set_eh_dir_extending(contex))) {
		if (free_page_aligned(new_dir, total_num))
			return -1;
		return 1;
	}

	new_dir_val = set_eh_dir_depth(new_dir, g_depth);

	while (old_dir != old_dir_end) {
		l_depth = get_two_eh_dir_entry(old_dir, &ent); 
		num = 1UL << (g_depth - l_depth);

		for (i = 0; i < num; ++i) {
			new_dir[i].ent1 = ent.ent1;
			new_dir[i].ent2 = ent.ent2;
		}

		t_ent = set_eh_seg_migrate(ent.ent1);

		if (unlikely(!cas_bool(&(old_dir->ent1), ent.ent1, t_ent)))
			continue;

		new_dir += num;
		old_dir += (num >> EH_EXTENSION_STEP);
	}

	release_fence();
	WRITE_ONCE(eh_contex, new_dir_val);

	delay_page_to_rcpage(old_dir, g_depth + EH_DIR_ENTRY_SIZE_SHIFT - EH_EXTENSION_STEP);

	return 0;
}

__attribute__((always_inline))
static int alloc_eh_four_bucket(struct eh_segment **new_two_seg) {
	if (unlikely(malloc_aligned((void **)&new_two_seg[0], CACHE_LINE_SIZE, EH_SEGMENT_SIZE)))
		return -1;

	if (unlikely(malloc_aligned((void **)&new_two_seg[1], CACHE_LINE_SIZE, EH_SEGMENT_SIZE))) {
		free(new_two_seg[0]);
		return -1;
	}

	return 0;
}



__attribute__((always_inline))
static void free_eh_four_bucket(struct eh_segment **new_two_seg) {
	free(new_two_seg[0]);
	free(new_two_seg[1]);
}

static int mark_eh_seg_spliting(struct eh_dir_entry *dir_ent,
								struct eh_dir_entry *ent, 
								u64 hashed_key, u64 contex,
								int g_depth, int l_depth,
								struct eh_segment **new_two_seg) {
	u64 tmp_ent1;
	int ret;

re_mark_eh_seg_spliting :
	tmp_ent1 = READ_ONCE(dir_ent->ent1);

	if (unlikely(eh_seg_migrate(tmp_ent1))) {
		ent->ent1 = tmp_ent1;
		return 1;
	}

	if (unlikely(eh_seg_changed(ent->ent1, tmp_ent1))) {
		int depth = eh_seg_depth(tmp_ent1);

		if (l_depth < depth)
			return 1;

		spin_fence();
		goto re_mark_eh_seg_spliting;
	}

	if (unlikely(eh_seg_spliting(tmp_ent1))) {
		spin_fence();
		goto re_mark_eh_seg_spliting;
	}

	if (g_depth == l_depth) {
		if (eh_dir_extending(contex)) {
			ent->ent1 = set_eh_seg_migrate(ent->ent1);
			return 1;
		}

		ret = extend_eh_directory(contex, g_depth);

		if (unlikely(ret == -1))
			return -1;

		if (unlikely(ret == 1))
			ent->ent1 = set_eh_seg_migrate(ent->ent1);

		return 1;
	}

	ret = alloc_eh_four_bucket(new_two_seg);

	if (unlikely(ret == -1))
		return -1;

	tmp_ent1 = set_eh_seg_spliting(ent->ent1);

	if (likely(cas_bool(&dir_ent->ent1, ent->ent1, tmp_ent1)))
		return 0;

	free_eh_four_bucket(new_two_seg);

	goto re_mark_eh_seg_spliting;
}

static void update_eh_dir_entry(struct eh_dir_entry *dir_ent, 
								struct eh_segment **new_two_seg, 
								u64 hashed_key,
								int l_depth, int g_depth) {
	struct eh_dir_entry ent;
	struct eh_dir_entry *tmp_ent, *ent_half, *ent_end;
	u64 num, ent1;

	num = 1UL << (g_depth - l_depth);

	ent_half = dir_ent + (num >> 1);
	ent_end = dir_ent + num;

	WRITE_ONCE(ent_half->ent1, dir_ent->ent1);

	ent1 = set_eh_seg_locked(dir_ent->ent1);

	ent.ent2 = extract_eh_seg_addr(dir_ent->ent1);
	ent.ent1 = make_eh_dir_ent1(new_two_seg[1], l_depth + 1);

	for (tmp_ent = ent_end - 1; tmp_ent >= ent_half; --tmp_ent) {
		release_fence();
		WRITE_ONCE(tmp_ent->ent1, ent1);
		release_fence();
		WRITE_ONCE(tmp_ent->ent2, ent.ent2);
		release_fence();
		WRITE_ONCE(tmp_ent->ent1, ent.ent1);
	}

	ent.ent1 = make_eh_dir_ent1(new_two_seg[0], l_depth + 1);

	for (tmp_ent = ent_half - 1; tmp_ent >= dir_ent; --tmp_ent) {
		release_fence();
		WRITE_ONCE(tmp_ent->ent1, ent1);
		release_fence();
		WRITE_ONCE(tmp_ent->ent2, ent.ent2);
		release_fence();
		WRITE_ONCE(tmp_ent->ent1, ent.ent1);
	}
}



static void eh_migrate_segment(struct eh_bucket *old_bucket,
							struct eh_segment **new_two_seg,
							int l_depth) {
	EH_KV_ITEM ent_cachebatch[PER_CACHELINE_EH_KV_ITEM];
	EH_KV_ITEM *kv;
	u16 count[4] = {0, 0, 0, 0};
	int i, j, k, n;

	kv = &(old_bucket->kv[0]);

	for (i = 0; i < EH_PER_BUCKET_CACHELINE; ++i) {
		j = 0;
			
		while (j < PER_CACHELINE_EH_KV_ITEM) {
			ent_cachebatch[j] = READ_ONCE(*kv);

			if (likely(cas_bool(kv, ent_cachebatch[j], set_eh_entry_split(ent_cachebatch[j])))) {
				prefech_r0((void*)eh_entry_kv_addr(ent_cachebatch[j]));
				++j;
				++kv;
			}
		}
		
		if (i != EH_PER_BUCKET_CACHELINE - 1)
			prefech_r0((void*)kv);

		for (k = 0; k < PER_CACHELINE_EH_KV_ITEM; ++k) {
			u64 ent = ent_cachebatch[k];
			struct kv *tmp_kv = (struct kv *)eh_entry_kv_addr(ent);

			if (deleted_kv(tmp_kv))
				reclaim_kv_to_rcpage(tmp_kv);
			else {
				n = specific_interval_kv_hashed_key(tmp_kv, 
								eh_entry_fingerprint16(ent), 
								l_depth + EH_BUCKET_INDEX_BIT - 1, 2);
				new_two_seg[n >> 1]->bucket[n & 1].kv[count[n]] = ent;
				count[n] = count[n] + 1;
			}
		}	
	}

	for (n = 0; n < 4; ++n) {
		k = count[n];
		memset(&(new_two_seg[n >> 1]->bucket[n & 1].kv[k]), 0, (EH_PER_BUCKET_KV_NUM - k) * EH_KV_ITEM_SIZE);
	}	
}

__attribute__((always_inline))
void reclaim_eh_segment(struct eh_segment *seg, int id) {
	EH_KV_ITEM *ent = &(seg->bucket[0].kv[0]);
	EH_KV_ITEM old, tmp;

	old = READ_ONCE(*ent);

	while (1) {
		tmp = cas(ent, old, set_eh_entry_move(old, id));

		if (likely(old == tmp))
			break;

		old = tmp;
	}

	if (eh_entry_move(old, id ^ 1))
		reclaim_kv_to_rcpage((struct kv *)seg);
}

__attribute__((always_inline))
void split_eh_segment(struct eh_segment *low_seg,
					struct eh_segment **new_two_seg,
					struct eh_dir_entry *dir_ent,
					u64 hashed_key, int id,
					int l_depth, int g_depth) {

	
	eh_migrate_segment(&low_seg->bucket[id], new_two_seg, l_depth);

	update_eh_dir_entry(dir_ent, new_two_seg, hashed_key, l_depth, g_depth);

	reclaim_eh_segment(low_seg, id);
}




static int eh_update_entry(struct kv *kv, struct eh_bucket *bucket,
							struct eh_bucket *upper_bucket, 
							u64 fingerprint16, u64 new_entry) {
	struct kv *kv1;
	u64 entry, old_entry;
	void *bucket_end;
	int i;

	bucket_end = ((void *)bucket) + (EH_PER_BUCKET_CACHELINE << CACHE_LINE_SHIFT);

	for (i = 0; i < EH_PER_BUCKET_KV_NUM; ++i) {
		entry = READ_ONCE(bucket->kv[i]);
		acquire_fence();

		if (!eh_entry_valid(entry)) {
			if (upper_bucket)
				prefetch_eh_bucket_head(upper_bucket);

			return i;
		}

		if ((i & (PER_CACHELINE_EH_KV_ITEM - 1)) == 0)
			prefetch_eh_bucket_step(bucket_end, upper_bucket, &bucket->kv[i]);

		if (fingerprint16 == eh_entry_fingerprint16(entry)) {
		re_update_eh_entry :
			kv1 = (struct kv *)eh_entry_kv_addr(entry);

			if (compare_kv_key(kv1, kv) == 0) {
				if (eh_entry_split(entry))
					return -1;

				if (unlikely(i == 0))
					new_entry |= (eh_entry_move(entry, 0) | eh_entry_move(entry, 1));
				
				old_entry = cas(&bucket->kv[i], entry, new_entry);

				if (unlikely(old_entry != entry)) {
					entry = old_entry;
					goto re_update_eh_entry;
				}

					
				reclaim_kv_to_rcpage(kv1);
				return EH_PER_BUCKET_KV_NUM + 1;
			}
		}
	}

	return EH_PER_BUCKET_KV_NUM;
}


static int eh_append_entry(struct kv *kv, 
						struct eh_bucket *bucket, 
						int index, u64 new_entry) {
	u64 old;
	int i;

	for (i = index; i < EH_PER_BUCKET_KV_NUM; ++i) {
		old = cas(&bucket->kv[i], 0, new_entry);

		if (unlikely(old)) {
			if (i == 0 && !eh_entry_valid(old)) {
				new_entry |= old;

				if (cas_bool(&bucket->kv[0], old, new_entry))
					return EH_PER_BUCKET_KV_NUM + 1;

				new_entry ^= old;
				--i;
				continue;
			}
			if (eh_entry_fingerprint16(new_entry) == eh_entry_fingerprint16(old)) {
				u64 old_entry;
				struct kv *kv1;

			re_append_eh_entry :
				kv1 = (struct kv *)eh_entry_kv_addr(old);
				
				if (compare_kv_key(kv1, kv) == 0) {
					if (eh_entry_split(old))
						return -1;

					if (i == 0)
						new_entry |= (eh_entry_move(old, 0) | eh_entry_move(old, 1));

					old_entry = cas(&bucket->kv[i], old, new_entry);

					if (unlikely(old_entry != old)) {
						old = old_entry;
						goto re_append_eh_entry;
					}

					reclaim_kv_to_rcpage(kv1);
					return EH_PER_BUCKET_KV_NUM + 1;
				}
			}
			
			continue;
		}

		return EH_PER_BUCKET_KV_NUM + 1;
	}

	return i;
}

int put_kv_eh_bucket(struct kv *kv, 
				u64 hashed_key, 
				u64 fingerprint16) {
	struct eh_dir_entry ent;
	struct eh_dir_entry *dir_ent;
	struct eh_segment *high_seg, *low_seg;
	struct eh_bucket *high_bucket, *low_bucket;
	struct eh_segment *new_two_seg[2];
	u64 contex, new_entry;
	int low_index, high_index, l_depth, g_depth, id, ret;

	new_entry = make_eh_new_entry(kv, fingerprint16);

retry_put_kv_eh_entry :
	contex = READ_ONCE(eh_contex);
	g_depth = eh_dir_depth(contex);
	dir_ent = (struct eh_dir_entry *)extract_eh_dir_addr(contex);

	dir_ent += eh_dir_index(hashed_key, g_depth);

	l_depth = get_two_eh_seg_ptr(dir_ent, &ent, &high_seg, &low_seg);

	id = eh_bucket_index(hashed_key, l_depth - 1);
	low_bucket = &low_seg->bucket[id];
	prefetch_eh_bucket_head(low_bucket);

	high_bucket = &high_seg->bucket[eh_bucket_index(hashed_key, l_depth)];

	dir_ent -= (eh_dir_index(hashed_key, g_depth) 
					- eh_dir_floor_index(hashed_key, l_depth, g_depth));

	low_index = eh_update_entry(kv, low_bucket, high_bucket, fingerprint16, new_entry);

	if (unlikely(low_index == -1))
		goto retry_put_kv_eh_entry;

	if (low_index == EH_PER_BUCKET_KV_NUM + 1)
		return 0;

	high_index = eh_update_entry(kv, high_bucket, NULL, fingerprint16, new_entry);

	if (unlikely(high_index == -1))
		goto retry_put_kv_eh_entry;


	if (high_index == EH_PER_BUCKET_KV_NUM + 1)
		return 0;

	low_index = eh_append_entry(kv, low_bucket, low_index, new_entry);

	if (unlikely(low_index == -1))
		goto retry_put_kv_eh_entry;

	if (low_index == EH_PER_BUCKET_KV_NUM + 1)
		return 0;

	high_index = eh_append_entry(kv, high_bucket, high_index, new_entry);

	if (unlikely(high_index == -1))
		goto retry_put_kv_eh_entry;

	if (high_index == EH_PER_BUCKET_KV_NUM + 1)
		return 0;

	ret = mark_eh_seg_spliting(dir_ent, &ent, hashed_key, contex, g_depth, l_depth, new_two_seg);

	if (unlikely(ret == -1))
		return -1;

	if (ret == 0)
		split_eh_segment(low_seg, new_two_seg, dir_ent, hashed_key, id, l_depth, g_depth);

	if (unlikely(eh_seg_migrate(ent.ent1))) {
		contex = set_eh_dir_extending(contex);

		while (contex == READ_ONCE(eh_contex))
			spin_fence();
	}

	goto retry_put_kv_eh_entry;
}


int get_kv_eh_bucket(struct kv *kv, 
				u64 hashed_key, 
				u64 fingerprint16) {
	struct eh_dir_entry ent;
	struct eh_dir_entry *dir_ent;
	struct eh_segment *high_seg, *low_seg;
	struct eh_bucket *high_bucket, *low_bucket;
	void *bucket_end;
	u64 entry, contex;
	int i, l_depth, g_depth;

	contex = READ_ONCE(eh_contex);
	g_depth = eh_dir_depth(contex);
	dir_ent = (struct eh_dir_entry *)extract_eh_dir_addr(contex);


	dir_ent += eh_dir_index(hashed_key, g_depth);

	l_depth = get_two_eh_seg_ptr(dir_ent, &ent, &high_seg, &low_seg);

	low_bucket = &low_seg->bucket[eh_bucket_index(hashed_key, l_depth - 1)];	
	prefetch_eh_bucket_head(low_bucket);

	high_bucket = &high_seg->bucket[eh_bucket_index(hashed_key, l_depth)];
	bucket_end = ((void *)low_bucket) + (EH_PER_BUCKET_CACHELINE << CACHE_LINE_SHIFT);

	for (i = 0; i < EH_PER_BUCKET_KV_NUM; ++i) {
		entry = READ_ONCE(low_bucket->kv[i]);
		acquire_fence();

		if (unlikely(!eh_entry_valid(entry))) {
			prefetch_eh_bucket_head(high_bucket);
			break;
		}

		if (!(i & (PER_CACHELINE_EH_KV_ITEM - 1)))
			prefetch_eh_bucket_step(bucket_end, high_bucket, &low_bucket->kv[i]);

		if (eh_entry_fingerprint16(entry) == fingerprint16) {
			struct kv *tmp_kv = (struct kv *)eh_entry_kv_addr(entry);

			if (compare_kv_key(kv, tmp_kv) == 0) {
				if (unlikely(deleted_kv(tmp_kv)))
					return -1;
				copy_kv_val(kv, tmp_kv);
				return 0;
			}
		}
	}

	bucket_end = ((void *)high_bucket) + (EH_PER_BUCKET_CACHELINE << CACHE_LINE_SHIFT);

	for (i = 0; i < EH_PER_BUCKET_KV_NUM; ++i) {
		entry = READ_ONCE(high_bucket->kv[i]);
		acquire_fence();

		if (unlikely(!eh_entry_valid(entry)))
			break;

		if (!(i & (PER_CACHELINE_EH_KV_ITEM - 1)))
			prefetch_eh_bucket_step(bucket_end, NULL, &high_bucket->kv[i]);

		if (eh_entry_fingerprint16(entry) == fingerprint16) {
			struct kv *tmp_kv = (struct kv *)eh_entry_kv_addr(entry);

			if (compare_kv_key(kv, tmp_kv) == 0) {
				if (unlikely(deleted_kv(tmp_kv)))
					return -1;
				copy_kv_val(kv, tmp_kv);
				return 0;
			}
		}
	}

	return -1;
}


