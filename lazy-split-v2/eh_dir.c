#include "eh_dir.h"
#include "background.h"

struct eh_dir *expand_eh_directory(
            EH_CONTEXT *contex,
            struct eh_dir *dir_head,
            int old_depth, int new_depth) {
    struct eh_dir *new_dir_head, *old_dir_end, *dir_new, *dir_old;
    EH_DIR_HEADER header;
    unsigned long num, i;
    int depth, diff;

#if BACKGROUNG_THREAD_NUM > 1
    EH_CONTEXT c_val = READ_ONCE(*contex);

    if (head_of_eh_dir(c_val) != dir_head || 
            eh_context_updating(c_val) || 
            !cas_bool(contex, c_val, set_eh_context_updating(c_val)))
		return NULL;
#endif

    new_dir_head = (struct eh_dir *)malloc_prefault_page_aligned(
                MUL_2((u64)EH_DIR_HEADER_SIZE, new_depth - EH_GROUP_BITS));

    if (unlikely((void *)new_dir_head == MAP_FAILED))
        return (struct eh_dir *)MAP_FAILED;

    old_dir_end = &dir_head[EXP_2(old_depth - EH_GROUP_BITS)];

    dir_new = new_dir_head;
    dir_old = dir_head;

    diff = new_depth - old_depth;

    while (dir_old != old_dir_end) {
        header = READ_ONCE(dir_old->header);

    #if BACKGROUNG_THREAD_NUM > 1
        if (unlikely(!cas_bool(dir_old, header, set_eh_dir_migrate(header))))
            continue;

        header = clear_eh_dir_spliting(header);
    #endif

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
    unsigned long i, half_num = MUL_2(1UL, g_depth - l_depth - 1);
    struct eh_dir *dir_half = &dir[half_num];
    int depth;

    header = READ_ONCE(dir->header);

#if BACKGROUNG_THREAD_NUM > 1
    if (unlikely(eh_dir_spliting(header) || eh_dir_migrate(header)))
        return -1;
#endif

    depth = eh_dir_depth(header);

    if (unlikely(l_depth != depth))
        return -1;
    
    header2 = make_eh_dir_header(new_seg2, l_depth + 1);
    header1 = make_eh_dir_header(new_seg1, l_depth + 1);

#if BACKGROUNG_THREAD_NUM > 1
    EH_DIR_HEADER header1_s = set_eh_dir_spliting(header1);
    EH_DIR_HEADER header2_s = set_eh_dir_spliting(header2);

    release_fence();
    WRITE_ONCE(dir_half->header, header2_s);

    if (unlikely(!cas_bool(&dir->header, header, header1_s))) {
        //WRITE_ONCE(dir_half->header, header);
        return -1;
    }

    for (i = 1; i < half_num; ++i)
        WRITE_ONCE(dir[i].header, header1);

    for (i = 1; i < half_num; ++i)
        WRITE_ONCE(dir_half[i].header, header2);

    cas(&dir->header, header1_s, header1);
    cas(&dir_half->header, header2_s, header2);
#else
    for (i = 0; i < half_num; ++i)
        WRITE_ONCE(dir[i].header, header1);

    for (i = 0; i < half_num; ++i)
        WRITE_ONCE(dir_half[i].header, header2);
#endif

    return 0;
}
