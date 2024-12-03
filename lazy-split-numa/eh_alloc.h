#ifndef __EH_ALLOC_H
#define __EH_ALLOC_H

#include "compiler.h"
#include "eh_seg.h"

#define EH_ALLOC_POOL_SIZE	EXP_2(32)


#define EH_PREFETCH_SEG4_NUM 32

#define EH_PREFETCH_LEVEL_LIMIT 7


#define EH_PREFETCH_OTHER_SEG4_NUM 32

#define RECORD_POOL_SIZE    EXP_2(32)


#ifdef  __cplusplus
extern  "C" {
#endif

typedef union page_pool {
    struct {
        u64 base_page : 36;
        u64 page_num : 28;
    };
    u64 pool;
} PAGE_POOL;

void *alloc_eh_seg();
void *alloc_record_page();
void prefault_eh_seg(int tid);
void prefault_other_eh_seg(int tid);

#ifdef  __cplusplus
}
#endif	//__cplusplus

#endif	//__EH_ALLOC_H
