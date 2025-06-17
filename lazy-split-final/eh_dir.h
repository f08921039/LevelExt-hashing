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

/*
#define eh_dir_index(hashed_key, depth)	\
			(((hashed_key) & INTERVAL(0, PREHASH_KEY_BITS - EH_GROUP_BITS - 1))	\
												>> (PREHASH_KEY_BITS - (depth)))
#define eh_dir_floor_index(hashed_key, l_depth, g_depth)	\
			(eh_dir_index(hashed_key, l_depth) << ((g_depth) - (l_depth)))
*/

#define EH_DIR_DEPTH_BIT_START	0
#define EH_DIR_DEPTH_BIT_END	(EH_DEPTH_BITS - 1)

#define EH_DIR_MIGRATE_BIT	(EH_DIR_DEPTH_BIT_END + 1)
#define EH_DIR_SPLITING_BIT	(EH_DIR_DEPTH_BIT_END + 2)

#define EH_DIR_LOW_SEG_BIT_START	PAGE_SHIFT
#define EH_DIR_LOW_SEG_BIT_END	(VALID_POINTER_BITS - 1)

#define EH_DIR_NODE_BIT_START	VALID_POINTER_BITS
#define EH_DIR_NODE_BIT_END	(EH_DIR_NODE_BIT_START + 15)


#define EH_DIR_LOW_SEG_MASK	INTERVAL(EH_DIR_LOW_SEG_BIT_START, EH_DIR_LOW_SEG_BIT_END)

/*
#define eh_dir_depth(header)	\
			INTERVAL_OF(header, EH_DIR_DEPTH_BIT_START, EH_DIR_DEPTH_BIT_END)
#define eh_dir_node(header)	((header) >> EH_DIR_NODE_BIT_START)
#define eh_dir_migrate(header)	((header) & SHIFT(EH_DIR_MIGRATE_BIT))
#define eh_dir_low_seg(header)	((header) &	\
					INTERVAL(EH_DIR_SEG_BIT_START, EH_DIR_SEG_BIT_END))

#define make_eh_dir_header(seg, depth, node)	(((uintptr_t)(seg)) |	\
								SHIFT_OF(depth, EH_DIR_DEPTH_BIT_START)	| \
								SHIFT_OF(node, EH_DIR_NODE_BIT_START))
#define set_eh_dir_migrate(header)	((header) | SHIFT(EH_DIR_MIGRATE_BIT))
*/


/*eh_dir [47~12:low_seg_addr 11~6:flag 5~0:depth]*/
struct eh_dir {
	EH_DIR_HEADER header;
};

static inline 
unsigned long eh_dir_index(u64 hashed_key, int depth) {
	return (hashed_key & INTERVAL(0, PREHASH_KEY_BITS - EH_GROUP_BITS - 1)) 
												>> (PREHASH_KEY_BITS - depth);
}


static inline 
unsigned long eh_dir_floor_index(u64 hashed_key, int l_depth, int g_depth) {
	return eh_dir_index(hashed_key, l_depth) << (g_depth - l_depth);
}


static inline 
struct eh_dir *head_of_eh_dir(EH_CONTEXT context) {
	return extract_eh_dir(context);
}

static inline 
struct eh_dir *slot_of_eh_dir(
					struct eh_dir *dir_head, 
					u64 hashed_key, 
					int g_depth) {
	return &dir_head[eh_dir_index(hashed_key, g_depth)];
}

static inline 
struct eh_dir *base_slot_of_eh_dir(
					struct eh_dir *dir_head, 
					u64 hashed_key,
					int l_depth,  
					int g_depth) {
	return &dir_head[eh_dir_floor_index(hashed_key, l_depth, g_depth)];
}

static inline 
int eh_dir_depth(EH_DIR_HEADER header) {
	return INTERVAL_OF(header, EH_DIR_DEPTH_BIT_START, EH_DIR_DEPTH_BIT_END);
}

static inline 
int eh_dir_node(EH_DIR_HEADER header) {
	return INTERVAL_OF(header, EH_DIR_NODE_BIT_START, EH_DIR_NODE_BIT_END);
}

static inline 
bool eh_dir_migrate(EH_DIR_HEADER header)	{
	return !!(header & SHIFT(EH_DIR_MIGRATE_BIT));
}

static inline 
bool eh_dir_cannot_split(EH_DIR_HEADER header)	{
	return !!(header & (SHIFT(EH_DIR_MIGRATE_BIT) | SHIFT(EH_DIR_SPLITING_BIT)));
}


static inline 
struct eh_segment *eh_dir_low_seg(EH_DIR_HEADER header)	{
	return (struct eh_segment *)(header & EH_DIR_LOW_SEG_MASK);
}

static inline 
EH_DIR_HEADER make_eh_dir_header(struct eh_segment *seg, int depth, int node) {
	return ((uintptr_t)seg) | 
				SHIFT_OF(depth, EH_DIR_DEPTH_BIT_START)	| 
				SHIFT_OF(node, EH_DIR_NODE_BIT_START);
}

static inline 
EH_DIR_HEADER set_eh_dir_migrate(EH_DIR_HEADER header)	{
	return header | SHIFT(EH_DIR_MIGRATE_BIT);
}

static inline 
EH_DIR_HEADER set_eh_dir_spliting(EH_DIR_HEADER header)	{
	return header | SHIFT(EH_DIR_SPLITING_BIT);
}

static inline 
EH_DIR_HEADER cancel_eh_dir_spliting(EH_DIR_HEADER header)	{
	return header & ~SHIFT(EH_DIR_SPLITING_BIT);
}

static inline 
struct eh_dir *get_eh_dir_entry(u64 hashed_key) {
	EH_CONTEXT c_val, *contex;
	struct eh_dir *dir_head;
	int g_depth;

	contex = get_eh_context(hashed_key);
	c_val = READ_ONCE(*contex);

	dir_head = head_of_eh_dir(c_val);

	g_depth = eh_depth(c_val);

	return slot_of_eh_dir(dir_head, hashed_key, g_depth);
}

static inline 
void prefetch_eh_dir(
			struct eh_dir *dir, 
			int g_depth, 
			int l_depth) {
	int dir_ents;

	dir_ents = EXP_2(g_depth - l_depth);

	while (dir_ents > 0) {
		prefech_w0(dir);
		dir += EXP_2(CACHE_LINE_SHIFT - EH_DIR_HEADER_SIZE_BITS);
		dir_ents -= EXP_2(CACHE_LINE_SHIFT - EH_DIR_HEADER_SIZE_BITS);
	}
}

struct eh_dir *expand_eh_directory(
            EH_CONTEXT *contex,
			EH_CONTEXT c_val,
            struct eh_dir *dir_head,
            int old_depth, int new_depth);

int split_eh_directory(
        struct eh_dir *dir,
        struct eh_segment *new_seg, 
        int l_depth, int g_depth);

#ifdef  __cplusplus
}
#endif	//__cplusplus

#endif	//__EH_DIR_H
