#include "eh_dir.h"
#include "background.h"

struct eh_dir *expand_eh_directory(
            EH_CONTEXT *contex,
            EH_CONTEXT c_val,
            struct eh_dir *dir_head,
            int old_depth, int new_depth) {
    struct eh_dir *new_dir_head, *old_dir_end, *dir_new, *dir_old;
    EH_DIR_HEADER header;
    unsigned long num, i;
    int depth, diff;

    if (eh_context_updating(c_val) || 
            !cas_bool(contex, c_val, set_eh_context_updating(c_val)))
		return NULL;

    new_dir_head = (struct eh_dir *)malloc_prefault_page_aligned(
                MUL_2((u64)EH_DIR_HEADER_SIZE, new_depth - EH_GROUP_BITS));

    if (unlikely((void *)new_dir_head == MAP_FAILED)) {
        WRITE_ONCE(*contex, c_val);
        return (struct eh_dir *)MAP_FAILED;
    }

    old_dir_end = &dir_head[EXP_2(old_depth - EH_GROUP_BITS)];

    dir_new = new_dir_head;
    dir_old = dir_head;

    diff = new_depth - old_depth;

    while (dir_old != old_dir_end) {
        header = READ_ONCE(dir_old->header);

        if (unlikely(!cas_bool(&dir_old->header, header, set_eh_dir_migrate(header))))
            continue;

        depth = eh_dir_depth(header);

        num = EXP_2(new_depth - depth);
        
        for (i = 0; i < num; ++i)
            dir_new[i].header = header;

        dir_new += num;
        dir_old += DIV_2(num, diff);
    }

    release_fence();
    WRITE_ONCE(*contex, set_eh_dir_context(new_dir_head, new_depth));

    reclaim_page(dir_head, old_depth + EH_DIR_HEADER_SIZE_BITS - EH_GROUP_BITS);

    return new_dir_head;
}


int split_eh_directory(
        struct eh_dir *dir,
        void *new_seg1,
        void *new_seg2,
        int l_depth, int g_depth) {
    EH_DIR_HEADER header, header1, header2;
    unsigned long i, half_num = EXP_2(g_depth - l_depth - 1);
    struct eh_dir *dir_half = &dir[half_num];
    int node_id;

    header = READ_ONCE(dir->header);

    if (unlikely(eh_dir_migrate(header)))
        return -1;

    node_id = eh_dir_node(header);
    
    header2 = make_eh_dir_header(new_seg2, l_depth + 1, node_id);
    header1 = make_eh_dir_header(new_seg1, l_depth + 1, node_id);


    for (i = 1; i < half_num; ++i)
        WRITE_ONCE(dir[i].header, header1);

    for (i = 0; i < half_num; ++i)
        WRITE_ONCE(dir_half[i].header, header2);

    if (unlikely(!cas_bool(&dir->header, header, header1)))
        return -1;

    return 0;
}
