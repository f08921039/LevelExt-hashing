#include "eh_alloc.h"
#include "per_thread.h"

#define MAX_RECOED_PAGE_IN_POOL    DIV_2(RECORD_POOL_SIZE, RECORD_PAGE_SHIFT)

#define EH_PREFETCH_SEG_SIZE    \
            MUL_2(EH_PREFETCH_SEG4_NUM, EH_FOUR_SEGMENT_SIZE_BITS)

#define EH_PREFETCH_OTHER_SEG_SIZE    \
            MUL_2(EH_PREFETCH_OTHER_SEG4_NUM, EH_FOUR_SEGMENT_SIZE_BITS)


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

void *alloc_record_page() {
    struct tls_context *tls = tls_context_itself();
    u64 pool_page_base, pool_page_num;
    void *r_pool;
    
    pool_page_base = tls_context->record_pool.base_page;
    pool_page_num = tls_context->record_pool.page_num;

    if (unlikely(pool_page_num == MAX_RECOED_PAGE_IN_POOL)) {
        r_pool = alloc_node_page(RECORD_POOL_SIZE, tls_node_id());

        if (unlikely(r_pool == NULL))
            return malloc_page_aligned(RECORD_PAGE_SIZE);

        tls_context->record_pool.page_num = 1;
        tls_context->record_pool.base_page = DIV_2((uintptr_t)r_pool, PAGE_SHIFT);

        return r_pool;
    }

    tls_context->record_pool.page_num = pool_page_num + 1;

    r_pool = (void *)MUL_2(pool_page_base, PAGE_SHIFT);
    return r_pool + MUL_2(pool_page_num, RECORD_PAGE_SHIFT);
}

void prefault_eh_seg(int tid) {
    struct tls_context *tls;
    void *addr, *addr_end, *pre_addr, *pre_addr2, *new_addr;
    unsigned long diff, pre_size;

    tls = tls_context_of_tid(tid);

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
            new_addr = alloc_node_page(EH_ALLOC_POOL_SIZE, tls_node_id());

            if (unlikely(new_addr == NULL))
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


void prefault_other_eh_seg(int tid) {
    struct tls_context *tls;
    RECORD_POINTER rp;
    struct record_page *r_page, *next_r_page;
    struct eh_four_segment *seg4_base;
    PAGE_POOL pool;
    void *pre_addr;
    unsigned long pre_size, pre_size2;
    int ent_num, prefault_ent_num, adv_ent_num, ent_diff;

    tls = tls_context_of_tid(tid);

    rp = READ_ONCE(tls->numa_split_tail);

    r_page = (struct record_page *)page_of_record_pointer(rp);
    ent_num = ent_num_of_record_pointer(rp);

    next_r_page = READ_ONCE(r_page->next);

    if (unlikely(next_r_page))
        return;

    pool.pool = r_page->pool.pool;

    seg4_base = (struct eh_four_segment *)MUL_2(pool.base_page, PAGE_SHIFT);
    prefault_ent_num = pool.page_num;

    if (prefault_ent_num >= ent_num) {
        ent_diff = prefault_ent_num - ent_num;

        if (ent_diff >= EH_PREFETCH_OTHER_SEG4_NUM)
            return;

        adv_ent_num = EH_PREFETCH_OTHER_SEG4_NUM - ent_diff;
    } else {
        prefault_ent_num = ent_num;
        adv_ent_num = EH_PREFETCH_OTHER_SEG4_NUM + tls->seg_prefetch_level;
    }

    pre_addr = ((void *)seg4_base) + MUL_2(prefault_ent_num, EH_FOUR_SEGMENT_SIZE_BITS);
    pre_size = MUL_2(adv_ent_num, EH_FOUR_SEGMENT_SIZE_BITS);

    if (unlikely(prefault_ent_num + adv_ent_num > MAX_OTHER_SEG4_IN_POOL)) {
        pre_size2 = MUL_2(MAX_OTHER_SEG4_IN_POOL - prefault_ent_num, EH_FOUR_SEGMENT_SIZE_BITS);

        if (prefault_page_aligned(pre_addr, pre_size2) != -1) {
            pool.page_num = MAX_OTHER_SEG4_IN_POOL;
            WRITE_ONCE(r_page->pool.pool, pool.pool);

            next_r_page = (struct record_page *)
                            alloc_node_page(RECORD_PAGE_SIZE, tls_node_id());

            if (unlikely(next_r_page == NULL))
                return;

            seg4_base = (struct eh_four_segment *)
                            alloc_node_page(OTHER_SEG4_POOL_SIZE, tls_node_id());

            if (unlikely(seg4_base == NULL)) {
                free_page_aligned(next_r_page, RECORD_PAGE_SIZE);
                return;
            }

            next_r_page->pool.base_page = DIV_2((uintptr_t)seg4_base, PAGE_SHIFT);
            next_r_page->pool.page_num = 0;
            next_r_page->next = NULL;

            r_page = cas(&r_page->next, NULL, next_r_page);

            if (unlikely(r_page != NULL)) {
                free_page_aligned(next_r_page, RECORD_PAGE_SIZE);
                free_node_page(seg4_base, OTHER_SEG4_POOL_SIZE);
                return;
            }

            if (prefault_page_aligned((void *)seg4_base, EH_PREFETCH_OTHER_SEG_SIZE) != -1)
                next_r_page->pool.page_num = EH_PREFETCH_OTHER_SEG4_NUM;
        }
    } else if (likely(prefault_page_aligned(pre_addr, pre_size) != -1)) {
        pool.page_num = prefault_ent_num + adv_ent_num;
        WRITE_ONCE(r_page->pool.pool, pool.pool);
    }
}
