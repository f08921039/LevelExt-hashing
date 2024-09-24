#include "eh_alloc.h"
#include "per_thread.h"

void *alloc_eh_seg() {
	struct tls_context *tls = tls_context_itself();
    void *addr = tls->seg_alloc;
    void *new_addr = addr + EH_FOUR_SEGMENT_SIZE;

    if (likely(new_addr <= tls->seg_alloc_end)) {
        WRITE_ONCE(tls->seg_alloc, new_addr);
        return addr;
    }

    addr = READ_ONCE(tls->seg_backup);
        
    if (unlikely(addr == NULL))
        return malloc_page_aligned(EH_FOUR_SEGMENT_SIZE);

    new_addr = addr + EH_FOUR_SEGMENT_SIZE;

    tls->seg_backup = NULL;
    tls->seg_alloc = new_addr;

    release_fence();
    WRITE_ONCE(tls->seg_alloc_end, addr + EH_ALLOC_POOL_SIZE);

    return addr;
}

void prefault_eh_seg(int tid) {
    struct tls_context *tls = tls_context_of_tid(tid);
    void *addr, *addr_end, *pre_addr, *pre_addr2, *new_addr;
    unsigned long diff, pre_size;

    addr_end = READ_ONCE(tls->seg_alloc_end);
    acquire_fence();

    pre_addr = tls->seg_prefault;

    if (unlikely(pre_addr > addr_end) || 
            unlikely(pre_addr < addr_end - EH_ALLOC_POOL_SIZE))
        return;

    addr = READ_ONCE(tls->seg_alloc);

    if (pre_addr >= addr) {
        if (tls->seg_prefetch_level == 0) {
            diff = (unsigned long)(pre_addr - addr);

            if (diff >= EH_PREFETCH_SEG_SIZE)
                return;

            pre_size = EH_PREFETCH_SEG_SIZE - diff;
        } else {
            tls->seg_prefetch_level >>= 1;
            pre_size = MUL_2(EH_PREFETCH_SEG_SIZE, tls->seg_prefetch_level);
        }
    } else {
        /*diff = (unsigned long)(addr - pre_addr);
        pre_size = diff + MUL_2(EH_PREFETCH_SEG_SIZE, tls->seg_prefetch_level);*/
        pre_size = MUL_2(EH_PREFETCH_SEG_SIZE, tls->seg_prefetch_level);
        pre_addr = addr;

        if (tls->seg_prefetch_level < EH_PREFETCH_LEVEL_LIMIT)
            tls->seg_prefetch_level += 1;
    }

    pre_addr2 = pre_addr + pre_size;
    
    if (unlikely(pre_addr2 >= addr_end)) {
        if (prefault_page_aligned(pre_addr, addr_end - pre_addr) != -1) {
            new_addr = malloc_page_aligned(EH_ALLOC_POOL_SIZE);

            if (unlikely(new_addr == MAP_FAILED))
                tls->seg_prefault = addr_end;
            else {
                if (prefault_page_aligned(new_addr, pre_addr2 - addr_end) != -1)
                    tls->seg_prefault = new_addr + (pre_addr2 - addr_end);
                else
                    tls->seg_prefault = new_addr;
            
                release_fence();
                WRITE_ONCE(tls->seg_backup, new_addr);
            }
        }
    } else if (likely(prefault_page_aligned(pre_addr, pre_size) != -1))
        tls->seg_prefault = pre_addr2;
}