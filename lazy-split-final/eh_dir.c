#include "eh_dir.h"
#include "background.h"

struct eh_dir *expand_eh_directory(
            EH_CONTEXT *context,
            EH_CONTEXT c_val,
            struct eh_dir *dir_head,
            int old_depth, 
            int new_depth) {
    struct eh_dir *new_dir_head, *old_dir_end, *dir_new, *dir_old;
    EH_DIR_HEADER header, mig_header;
    EH_CONTEXT update_c_val;
    struct per_node_context *per_nc;
    unsigned long num, i;
    int depth, diff;

    update_c_val = set_eh_context_updating(c_val);

    if (eh_context_updating(c_val) || 
                !cas_bool(context, c_val, update_c_val))
		return NULL;

    new_dir_head = (struct eh_dir *)malloc_prefault_page_aligned(
                MUL_2((u64)EH_DIR_HEADER_SIZE, new_depth - EH_GROUP_BITS));

    if (unlikely((void *)new_dir_head == MAP_FAILED)) {
        WRITE_ONCE(*context, c_val);
        return (struct eh_dir *)MAP_FAILED;
    }

    old_dir_end = &dir_head[EXP_2(old_depth - EH_GROUP_BITS)];

    dir_new = new_dir_head;
    dir_old = dir_head;

    diff = new_depth - old_depth;

    while (dir_old != old_dir_end) {
        header = READ_ONCE(dir_old->header);

        mig_header = set_eh_dir_migrate(header);

        if (unlikely(!cas_bool(&dir_old->header, header, mig_header)))
            continue;

        depth = eh_dir_depth(header);

        header = cancel_eh_dir_spliting(header);

        num = EXP_2(new_depth - depth);
        
        for (i = 0; i < num; ++i)
            dir_new[i].header = header;

        dir_new += num;
        dir_old += DIV_2(num, diff);
    }

    per_nc = tls_node_context();

	if (old_depth == READ_ONCE(per_nc->global_depth))
		cas(&per_nc->global_depth, old_depth, new_depth);

    c_val = set_eh_dir_context(new_dir_head, new_depth);

    release_fence();
    WRITE_ONCE(*context, c_val);

    reclaim_page(dir_head, old_depth + EH_DIR_HEADER_SIZE_BITS - EH_GROUP_BITS);

    return new_dir_head;
}


int split_eh_directory(
        struct eh_dir *dir,
        struct eh_segment *new_seg, 
        int l_depth, 
        int g_depth) {
    EH_DIR_HEADER header, header1, header2, header2_s;
    unsigned long i, half_num;
    struct eh_dir *dir_half;
    int node_id;

    header = READ_ONCE(dir->header);

    if (unlikely(eh_dir_cannot_split(header) || 
                    l_depth != eh_dir_depth(header)))
        return -1;

    node_id = eh_dir_node(header);
    
    header2 = make_eh_dir_header(&new_seg[1], l_depth + 1, node_id);
    header1 = make_eh_dir_header(&new_seg[0], l_depth + 1, node_id);

    header2_s = set_eh_dir_spliting(header2);

    half_num = EXP_2(g_depth - l_depth - 1);

    dir_half = &dir[half_num];

    for (i = 1; i < half_num; ++i)
        WRITE_ONCE(dir[i].header, header1);

    for (i = 0; i < half_num; ++i)
        WRITE_ONCE(dir_half[i].header, header2_s);

    if (unlikely(!cas_bool(&dir->header, header, header1)))
        return -1;

    cas(&dir_half->header, header2_s, header2);

    return 0;
}
