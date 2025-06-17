#include "eh_alloc.h"

#include "per_thread.h"

void hint_eg_seg_prefault(int num) {
    struct tls_context *tls;
    unsigned long hints;

    tls = tls_context_itself();

    hints = tls->seg_prefault_hint;

    release_fence();
    WRITE_ONCE(tls->seg_prefault_hint, hints + num);
}

bool is_prefault_seg_enough(int num) {
    struct tls_context *tls;
    PAGE_POOL seg_pool;
    u64 seg_base, seg_num;
    void *addr, *prefault;

    tls = tls_context_itself();

    seg_pool.pool = READ_ONCE(tls->seg_pool.pool);
    acquire_fence();

    prefault = READ_ONCE(tls->seg_prefault);

    seg_base = seg_pool.base_page;
    seg_num = num + seg_pool.page_num;

    addr = (void *)MUL_2(seg_base, PAGE_SHIFT) + MUL_2(seg_num, EH_SEGMENT_SIZE_BITS);

    return (prefault >= addr ? true : false);
}


struct eh_segment *alloc_eh_seg(int num) {
	struct tls_context *tls;
    PAGE_POOL seg_pool, pool;
    u64 seg_base, seg_num;
    //void *addr;
    struct eh_segment *seg;

    tls = tls_context_itself();

    seg_pool.pool = READ_ONCE(tls->seg_pool.pool);

re_alloc_eh_seg :
    seg_base = seg_pool.base_page;
    seg_num = seg_pool.page_num;

    //addr = (void *)MUL_2(seg_base, PAGE_SHIFT) + MUL_2(seg_num, EH_SEGMENT_SIZE_BITS);
    seg = (struct eh_segment *)MUL_2(seg_base, PAGE_SHIFT);
    seg += seg_num;

    if (likely(seg_num + num <= MAX_EH_SEG_IN_POOL)) {
        pool.base_page = seg_base;
        pool.page_num = seg_num + num;

        pool.pool = cas(&tls->seg_pool.pool, seg_pool.pool, pool.pool);

        if (likely(pool.pool == seg_pool.pool))
            return seg;

        seg_pool.pool = pool.pool;
        goto re_alloc_eh_seg;
    }

    if (seg_num != MAX_EH_SEG_IN_POOL) {
        pool.base_page = seg_base;
        pool.page_num = MAX_EH_SEG_IN_POOL;

        pool.pool = cas(&tls->seg_pool.pool, seg_pool.pool, pool.pool);

        if (unlikely(pool.pool != seg_pool.pool)) {
            seg_pool.pool = pool.pool;
            goto re_alloc_eh_seg;
        }

        //to dooooooooooooo: free remain seg_pool
        //free_page_aligned(addr, MUL_2(MAX_EH_SEG_IN_POOL - seg_num, EH_SEGMENT_SIZE_BITS));
    }

    //addr = READ_ONCE(tls->seg_backup);
    seg = (struct eh_segment *)READ_ONCE(tls->seg_backup);

    if (unlikely(seg == NULL))
        return (struct eh_segment *)malloc_page_aligned(
                                MUL_2(num, EH_SEGMENT_SIZE_BITS));

    tls->seg_backup = NULL;

    pool.base_page = DIV_2((uintptr_t)seg, PAGE_SHIFT);
    pool.page_num = num;

    release_fence();
    WRITE_ONCE(tls->seg_pool.pool, pool.pool);

    return seg;
}


struct eh_segment *alloc_other_eh_seg(int num, int nid) {
    struct per_node_context *per_nc;
	struct tls_context *tls;
    PAGE_POOL seg_pool, other_seg_pool, pool, other_pool, extra_pool;
    u64 p, seg_base, seg_num;
    struct eh_segment *seg;
    int tid;

    per_nc = &node_context.all_node_context[nid];
    tid = tid_itself() % per_nc->max_work_thread_num;
    tls = &per_nc->max_tls_context[tid];

    extra_pool.pool = READ_ONCE(tls->numa_extra_seg.pool);

    if (num == extra_pool.page_num && 
            cas_bool(&tls->numa_extra_seg.pool, extra_pool.pool, 0))
        return (void *)MUL_2(extra_pool.base_page, PAGE_SHIFT);

    other_seg_pool.pool = READ_ONCE(tls->numa_seg_pool.pool);

re_alloc_other_eh_seg :
    seg_base = other_seg_pool.base_page;
    seg_num = other_seg_pool.page_num;

    seg = (struct eh_segment *)MUL_2(seg_base, PAGE_SHIFT);
    seg += seg_num;

    if (likely(seg_num + num <= MAX_EH_SEG_FOR_OTHER_POOL)) {
        other_pool.base_page = seg_base;
        other_pool.page_num = seg_num + num;

        p = cas(&tls->numa_seg_pool.pool, other_seg_pool.pool, other_pool.pool);

        if (likely(p == other_seg_pool.pool))
            return seg;

        other_seg_pool.pool = p;
        goto re_alloc_other_eh_seg;
    }

    if (seg_num != MAX_EH_SEG_FOR_OTHER_POOL) {
        other_pool.base_page = seg_base;
        other_pool.page_num = MAX_EH_SEG_FOR_OTHER_POOL;

        p = cas(&tls->numa_seg_pool.pool, other_seg_pool.pool, other_pool.pool);

        if (unlikely(p != other_seg_pool.pool)) {
            other_seg_pool.pool = p;
            goto re_alloc_other_eh_seg;
        }

        extra_pool.base_page = DIV_2((uintptr_t)seg, PAGE_SHIFT);
        extra_pool.page_num = MAX_EH_SEG_FOR_OTHER_POOL - seg_num;

        if (!cas_bool(&tls->numa_extra_seg.pool, 0, extra_pool.pool)) {
            //to dooooooooooooo: free remain extra_seg
            //free_page_aligned((void *)seg, MUL_2(extra_pool.page_num, EH_SEGMENT_SIZE_BITS));
        }

        other_seg_pool.pool = other_pool.pool;
    }

    seg_pool.pool = READ_ONCE(tls->seg_pool.pool);

re_alloc_max_other_eh_seg :
    seg_base = seg_pool.base_page;
    seg_num = seg_pool.page_num;

    if (likely(seg_num + MAX_EH_SEG_FOR_OTHER_POOL <= MAX_EH_SEG_IN_POOL)) {
        pool.base_page = seg_base;
        pool.page_num = seg_num + MAX_EH_SEG_FOR_OTHER_POOL;

        seg = (struct eh_segment *)MUL_2(seg_base, PAGE_SHIFT);
        seg += seg_num;

        other_pool.base_page = DIV_2((uintptr_t)seg, PAGE_SHIFT);
        other_pool.page_num = num;

        p = cas(&tls->seg_pool.pool, seg_pool.pool, pool.pool);

        if (unlikely(p != seg_pool.pool)) {
            seg_pool.pool = p;
            goto re_alloc_max_other_eh_seg;
        }

        p = cas(&tls->numa_seg_pool.pool, other_seg_pool.pool, other_pool.pool);

        if (likely(p == other_seg_pool.pool))
            return seg;

        //to dooooooooooooo: free other_seg
        //free_page_aligned((void *)seg, MUL_2(MAX_EH_SEG_FOR_OTHER_POOL, EH_SEGMENT_SIZE_BITS));

        other_seg_pool.pool = p;
        goto re_alloc_other_eh_seg;
    }

    return (struct eh_segment *)alloc_node_page(MUL_2(num, EH_SEGMENT_SIZE_BITS), nid);
}


struct record_page *alloc_record_page() {
    struct tls_context *tls;
    u64 pool_page_base, pool_page_num;
    struct record_page *r_page;

    tls = tls_context_itself();
    
    pool_page_base = tls->record_pool.base_page;
    pool_page_num = tls->record_pool.page_num;

    if (unlikely(pool_page_num == MAX_RECOED_PAGE_IN_POOL)) {
        r_page = (struct record_page *)alloc_node_page(
                                    RECORD_POOL_SIZE, tls_node_id());

        if (unlikely((void *)r_page == MAP_FAILED))
            return (struct record_page *)malloc_page_aligned(
                                                    RECORD_PAGE_SIZE);

        tls->record_pool.page_num = 1;
        tls->record_pool.base_page = DIV_2((uintptr_t)r_page, PAGE_SHIFT);

        return r_page;
    }

    tls->record_pool.page_num = pool_page_num + 1;

    r_page = (struct record_page *)MUL_2(pool_page_base, PAGE_SHIFT);
    r_page += pool_page_num;

    return r_page;
}

struct record_page *alloc_other_record_page(int nid) {
    struct per_node_context *per_nc;
    struct tls_context *tls;
    u64 pool_page_base, pool_page_num, pool;
    struct record_page *r_page;
    PAGE_POOL rec_pool, new_pool;
    int tid;

    per_nc = &node_context.all_node_context[nid];
    tid = tid_itself() % per_nc->max_work_thread_num;
    tls = &per_nc->max_tls_context[tid];

    rec_pool.pool = READ_ONCE(tls->numa_record_pool.pool);

re_alloc_other_record_page :
    pool_page_base = rec_pool.base_page;
    pool_page_num = rec_pool.page_num;

    if (unlikely(pool_page_num == MAX_RECOED_PAGE_IN_POOL)) {
        r_page = (struct record_page *)alloc_node_page(
                                        RECORD_POOL_SIZE, nid);

        if (unlikely((void *)r_page == MAP_FAILED))
            return (struct record_page *)malloc_page_aligned(
                                                    RECORD_PAGE_SIZE);

        new_pool.base_page = DIV_2((uintptr_t)r_page, PAGE_SHIFT);
        new_pool.page_num = 1;

        pool = cas(&tls->numa_record_pool.pool, rec_pool.pool, new_pool.pool);

        if (unlikely(rec_pool.pool != pool)) {
            rec_pool.pool = pool;
            goto re_alloc_other_record_page;
        }

        return r_page;
    }

    new_pool.base_page = pool_page_base;
    new_pool.page_num = pool_page_num + 1;

    pool = cas(&tls->numa_record_pool.pool, rec_pool.pool, new_pool.pool);

    if (unlikely(rec_pool.pool != pool)) {
        rec_pool.pool = pool;
        goto re_alloc_other_record_page;
    }

    r_page = (struct record_page *)MUL_2(pool_page_base, PAGE_SHIFT);
    r_page += pool_page_num;

    return r_page;
}

int prefault_eh_seg(int tid) {
    struct tls_context *tls;
    PAGE_POOL seg_pool;
    void *addr, *addr_end, *pre_addr, *pre_addr2, *new_addr;
    unsigned long seg_base, seg_num, all_hints, hints, faults, fault_size;
    int ret;

    tls = tls_context_of_tid(tid);

    seg_pool.pool = READ_ONCE(tls->seg_pool.pool);
    acquire_fence();

    seg_base = seg_pool.base_page;
    seg_num = seg_pool.page_num;

    addr = (void *)MUL_2(seg_base, PAGE_SHIFT);
    addr_end = addr + EH_ALLOC_POOL_SIZE;

    pre_addr = tls->seg_prefault;

    if (unlikely(pre_addr >= addr_end) || unlikely(pre_addr < addr))
        return 0;

    ret = 0;

    addr += MUL_2(seg_num, EH_SEGMENT_SIZE_BITS);

    all_hints = READ_ONCE(tls->seg_prefault_hint);
    acquire_fence();

    hints = all_hints - tls->seg_prefault_count;

    if (hints > MAX_EH_PREFETCH_SEG) {
        ret = 1;
        faults = MAX_EH_PREFETCH_SEG;
    } else
        faults = hints;

    fault_size = MUL_2(faults, EH_SEGMENT_SIZE_BITS);

    if (pre_addr < addr) {
        ret = 1;
        faults = 0;
        fault_size = EH_PREFETCH_SEG_SIZE;
        pre_addr = addr;
    } else if (addr + EH_PREFETCH_SEG_SIZE > pre_addr + fault_size)
        fault_size = EH_PREFETCH_SEG_SIZE;//fault_size = (addr + EH_PREFETCH_SEG_SIZE) - pre_addr;

    tls->seg_prefault_count += faults;

    pre_addr2 = pre_addr + fault_size;

    if (unlikely(pre_addr2 >= addr_end)) {
        if (prefault_page_aligned(pre_addr, addr_end - pre_addr) != -1) {
            new_addr = alloc_node_page(EH_ALLOC_POOL_SIZE, tls_node_id());

            if (unlikely(new_addr == MAP_FAILED))
                tls->seg_prefault = addr_end;
            else {
                if (prefault_page_aligned(new_addr, EH_PREFETCH_SEG_SIZE) != -1)
                    tls->seg_prefault = new_addr + EH_PREFETCH_SEG_SIZE;
                else
                    tls->seg_prefault = new_addr;
            
                release_fence();
                WRITE_ONCE(tls->seg_backup, new_addr);
            }
        }
    } else if (fault_size != 0 && 
            likely(prefault_page_aligned(pre_addr, fault_size) != -1))
        tls->seg_prefault = pre_addr2;

    return ret;
}
