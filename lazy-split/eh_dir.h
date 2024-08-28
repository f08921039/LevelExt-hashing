#ifndef __EH_DIR_H
#define __EH_DIR_H

#include "compiler.h"
#include "kv.h"

#include "eh_seg.h"
#include "eh_context.h"

typedef u64 EH_DIR_HEADER;


#define EH_DIR_HEADER_SIZE	sizeof(EH_DIR_HEADER)
#define EH_DIR_HEADER_SIZE_BITS	LOG2(EH_DIR_HEADER_SIZE)

#ifdef  __cplusplus
extern  "C" {
#endif


#define eh_dir_index(hashed_key, depth)	\
			(((hashed_key) & INTERVAL(0, PREHASH_KEY_BITS - EH_GROUP_BITS - 1))	\
												>> (PREHASH_KEY_BITS - (depth)))
#define eh_dir_floor_index(hashed_key, l_depth, g_depth)	\
			(eh_dir_index(hashed_key, l_depth) << ((g_depth) - (l_depth)))

#define EH_DIR_DEPTH_BIT_START	0
#define EH_DIR_DEPTH_BIT_END	(EH_DEPTH_BITS - 1)

#define EH_DIR_MIGRATE_BIT	(EH_DIR_DEPTH_BIT_END + 1)
#define EH_DIR_SPLITING_BIT	(EH_DIR_DEPTH_BIT_END + 2)

#define EH_DIR_SEG_BIT_START	PAGE_SHIFT
#define EH_DIR_SEG_BIT_END	(VALID_POINTER_BITS - 1)


#define eh_dir_depth(header)	\
			(((header) & INTERVAL(EH_DIR_DEPTH_BIT_START, EH_DIR_DEPTH_BIT_END))	\
				>> EH_DIR_DEPTH_BIT_START)
#define eh_dir_migrate(header)	((header) & SHIFT(EH_DIR_MIGRATE_BIT))
#define eh_dir_spliting(header)	((header) & SHIFT(EH_DIR_SPLITING_BIT))
#define eh_dir_low_seg(header)	((header) &	\
					INTERVAL(EH_DIR_SEG_BIT_START, EH_DIR_SEG_BIT_END))

#define make_eh_dir_header(seg, depth)	(((uintptr_t)(seg)) |	\
								SHIFT_OF(depth, EH_DIR_DEPTH_BIT_START))
#define set_eh_dir_migrate(header)	((header) | SHIFT(EH_DIR_MIGRATE_BIT))
#define set_eh_dir_spliting(header)	((header) | SHIFT(EH_DIR_SPLITING_BIT))
#define clear_eh_dir_spliting(header)	\
							((header) & ~SHIFT(EH_DIR_SPLITING_BIT))


/*eh_dir [47~12:low_seg_addr 11~6:flag 5~0:depth]*/
struct eh_dir {
	EH_DIR_HEADER header;
};

static inline struct eh_dir *head_of_eh_dir(EH_CONTEXT context) {
	return (struct eh_dir *)extract_eh_dir(context);
}

static inline struct eh_dir *slot_of_eh_dir(
					struct eh_dir *dir_head, 
					u64 hashed_key, 
					int g_depth) {
	return &dir_head[eh_dir_index(hashed_key, g_depth)];
}

static inline struct eh_dir *base_slot_of_eh_dir(
					struct eh_dir *dir_head, 
					u64 hashed_key,
					int l_depth,  
					int g_depth) {
	return &dir_head[eh_dir_floor_index(hashed_key, l_depth, g_depth)];
}

static inline struct eh_segment *seg_of_eh_dir(struct eh_dir *dir) {
	EH_DIR_HEADER header = READ_ONCE(dir->header);

	return (struct eh_segment *)eh_dir_low_seg(header);
}


static inline int eh_dir_kv_operation(
					struct kv *kv, 
					u64 hashed_key,
					int (*eh_seg_op)(struct eh_segment*, struct kv*, u64, int)) {
    EH_CONTEXT *contex = get_eh_context(hashed_key);
    EH_CONTEXT c_val = READ_ONCE(*contex);
    struct eh_dir *dir, *dir_head;
	struct eh_segment *seg;
	EH_DIR_HEADER header;
    int l_depth, g_depth;

	dir_head = head_of_eh_dir(c_val);

	g_depth = eh_depth(c_val);

	dir = slot_of_eh_dir(dir_head, hashed_key, g_depth);

	header = READ_ONCE(dir->header);
	seg = (struct eh_segment *)eh_dir_low_seg(header);
	l_depth = eh_dir_depth(header);

    return eh_seg_op(seg, kv, hashed_key, l_depth);
}

/*static inline int put_eh_dir_kv(
					struct eh_dir *dir, 
					struct kv *kv, 
					u64 hashed_key, 
					int g_depth) {
	struct eh_segment *seg;
	int l_depth;
	
	dir = slot_of_eh_dir(dir, hashed_key, g_depth);
	seg = seg_of_eh_dir(dir);
	l_depth = eh_dir_depth(header);

	return put_eh_seg_kv(seg, kv, hashed_key, l_depth);
}


static inline int get_eh_dir_kv(
					struct eh_dir *dir, 
					struct kv *kv, 
					u64 hashed_key, 
					int g_depth) {
	struct eh_segment *seg;
	int l_depth;
	
	dir = slot_of_eh_dir(dir, hashed_key, g_depth);
	seg = seg_of_eh_dir(dir);
	l_depth = eh_dir_depth(header);

	return get_eh_seg_kv(seg, kv, hashed_key, l_depth);
}*/

struct eh_dir *expand_eh_directory(
            EH_CONTEXT *contex,
            struct eh_dir *dir_head,
            int old_depth, int new_depth);

int split_eh_directory(
        struct eh_dir *dir,
        void *new_seg1,
        void *new_seg2,
        int l_depth, int g_depth);

#ifdef  __cplusplus
}
#endif	//__cplusplus

#endif	//__EH_DIR_H
