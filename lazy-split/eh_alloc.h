#ifndef __EH_ALLOC_H
#define __EH_ALLOC_H

#include "compiler.h"
#include "eh_seg.h"

#define EH_ALLOC_POOL_SIZE	EXP_2(32)

#define EH_PREFETCH_SEG_NUM 32 
#define EH_PREFETCH_SEG_SIZE    \
        MUL_2((unsigned long)EH_PREFETCH_SEG_NUM, EH_SEGMENT_SIZE_BITS)

#define EH_PREFETCH_LEVEL_LIMIT 7


#ifdef  __cplusplus
extern  "C" {
#endif

void *alloc_eh_seg();

void prefault_eh_seg(int tid);


#ifdef  __cplusplus
}
#endif	//__cplusplus

#endif	//__EH_ALLOC_H