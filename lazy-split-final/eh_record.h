#ifndef __EH_RECORD_H
#define __EH_RECORD_H

#include "compiler.h"
#include "eh_rehash.h"


#define RECORD_PAGE_SHIFT    (PAGE_SHIFT + 3)
#define RECORD_PAGE_SIZE    EXP_2(RECORD_PAGE_SHIFT)
#define RECORD_PAGE_HEADER_SIZE 16

#define SPLIT_ENT_PER_RECORD_PAGE   \
            ((RECORD_PAGE_SIZE - RECORD_PAGE_HEADER_SIZE) / sizeof(struct eh_split_entry))
#define RECLAIM_ENT_PER_RECORD_PAGE \
            ((RECORD_PAGE_SIZE - RECORD_PAGE_HEADER_SIZE) / sizeof(struct reclaim_entry))

#define INVALID_RECORD_POINTER  0UL


#ifdef  __cplusplus
extern  "C" {
#endif


typedef uintptr_t RECORD_POINTER;


struct reclaim_entry {
    uintptr_t addr;
};

struct record_page {
    union {
        struct eh_split_entry s_ent[SPLIT_ENT_PER_RECORD_PAGE];
        struct reclaim_entry r_ent[RECLAIM_ENT_PER_RECORD_PAGE];
    };

    union {
        void *carve;
        unsigned long counter;
    };

    struct record_page *next;
};


static inline 
void *page_reclaim_addr(struct reclaim_entry *ent) {
    return (void *)(ent->addr & PAGE_MASK);
}

static inline 
unsigned long page_reclaim_size(struct reclaim_entry *ent) {
    return EXP_2(ent->addr & ~PAGE_MASK);
}

static inline 
uintptr_t make_page_reclaim_ent(void *addr, int shift) {
    return ((uintptr_t)addr) | shift;
}



static inline 
struct record_page *page_of_record_pointer(RECORD_POINTER rp) {
    return (struct record_page *)(rp & VALID_POINTER_MASK);
}

static inline 
int ent_num_of_record_pointer(RECORD_POINTER rp) {
    return rp >> VALID_POINTER_BITS;
}

static inline 
RECORD_POINTER make_record_pointer(
                    struct record_page *page, 
                    int ent_num) {
    return ((uintptr_t)page) | SHIFT_OF(ent_num, VALID_POINTER_BITS);
}

#ifdef  __cplusplus
}
#endif	//__cplusplus

#endif	//__EH_RECORD_H
