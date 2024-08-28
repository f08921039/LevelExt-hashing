#include "eh_context.h"

#include "eh_dir.h"
#include "eh_seg.h"

#define INITIAL_EH_DIR_PAGE_BITS_PER_GROUP  0
#define INITIAL_EH_SEG_BITS_PER_GROUP   0

#define INITIAL_EH_DIR_PAGE_BITS   \
            (EH_GROUP_BITS + INITIAL_EH_DIR_PAGE_BITS_PER_GROUP)
#define INITIAL_EH_SEG_BITS    (EH_GROUP_BITS + INITIAL_EH_SEG_BITS_PER_GROUP)

#define INITIAL_EH_DIR_PAGE   EXP_2(INITIAL_EH_DIR_PAGE_BITS)
#define INITIAL_EH_SEG    (3 * EXP_2(INITIAL_EH_SEG_BITS))

#define INITIAL_EH_DIR_PAGE_SIZE    MUL_2(INITIAL_EH_DIR_PAGE, PAGE_SHIFT)
#define INITIAL_EH_SEG_SIZE MUL_2(INITIAL_EH_SEG, EH_SEGMENT_SIZE_BITS)

#define INITIAL_EH_G_DEPTH  (INITIAL_EH_DIR_PAGE_BITS + PAGE_SHIFT - EH_DIR_HEADER_SIZE_BITS)
#define INITIAL_EH_L_DEPTH  INITIAL_EH_SEG_BITS

#define INITIAL_EH_DIR_ENT_PER_GROUP    EXP_2(INITIAL_EH_G_DEPTH - EH_GROUP_BITS)
#define INITIAL_EH_DIR_BASE_ENT_PER_GROUP    EXP_2(INITIAL_EH_L_DEPTH - EH_GROUP_BITS)
#define INITIAL_EH_DIR_ENT_PER_BASE_ENT EXP_2(INITIAL_EH_G_DEPTH - INITIAL_EH_L_DEPTH)

EH_CONTEXT eh_group[EH_GROUP_NUM] __attribute__ ((aligned (CACHE_LINE_SIZE)));

int init_eh_structure() {
    struct eh_dir *all_dir;
    struct eh_segment *all_seg;
    int i, j, k;

    all_dir = (struct eh_dir *)malloc_prefault_page_aligned(INITIAL_EH_DIR_PAGE_SIZE);

    if (unlikely((void *)all_dir == MAP_FAILED))
        return -1;

    all_seg = (struct eh_segment *)malloc_prefault_page_aligned(INITIAL_EH_SEG_SIZE);

    if (unlikely((void *)all_seg == MAP_FAILED)) {
        free_page_aligned(all_dir, INITIAL_EH_DIR_PAGE_SIZE);
        return -1;
    }

    for (i = 0; i < EH_GROUP_NUM; ++i) {
        eh_group[i] = set_eh_dir_context(all_dir, INITIAL_EH_G_DEPTH);

        for (j = 0; j < INITIAL_EH_DIR_BASE_ENT_PER_GROUP; ++j) {
            EH_DIR_HEADER header = make_eh_dir_header(all_seg, INITIAL_EH_L_DEPTH);
            
            for (k = 0; k < EH_BUCKET_NUM; ++k)
                all_seg->bucket[k].header = set_eh_seg_low(&all_seg[1]);

            all_seg += 3;

            for (k = 0; k < INITIAL_EH_DIR_ENT_PER_BASE_ENT; ++k)
                all_dir[k].header = header;

            all_dir += INITIAL_EH_DIR_ENT_PER_BASE_ENT;
        }
    }

    memory_fence();
    return 0;
}
