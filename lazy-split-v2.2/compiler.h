#ifndef __COMPILER_H
#define __COMPILER_H

#define _GNU_SOURCE

#ifdef  __cplusplus
extern  "C" {
#endif


#include <errno.h>
#include <unistd.h>
#include <inttypes.h>
#include <math.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
//#include <stdarg.h>
#include <string.h>
#include <endian.h>
#include <byteswap.h>
#include <limits.h>

#include <malloc.h>
#include <assert.h>
#include <sched.h>
#include <signal.h>

#include <sys/mman.h>
#include <sys/time.h>

#include <pthread.h>

#include <numa.h>

#define SHIFT(bit)	(1UL << (bit))
#define SHIFT_OF(val, bit)	(((unsigned long)(val)) << (bit))
#define MASK(bit)	~(SHIFT(bit) - 1)
#define INTERVAL(bit1, bit2)	((MASK(bit1) ^ MASK(bit2)) | SHIFT(bit2))

#define LOG2(val)	(((sizeof(unsigned long) * CHAR_BIT) - 1) - (__builtin_clzl((val))))
#define DIV_2(val, exp)	((val) >> (exp))
#define MUL_2(val, exp)	((val) << (exp))
#define EXP_2(exp)	(MUL_2(1UL, exp))

#define PAGE_SIZE	4096UL
#define PAGE_SHIFT	12UL
#define PAGE_MASK	MASK(PAGE_SHIFT)

#define CACHE_LINE_SIZE 64UL
#define CACHE_LINE_SHIFT 6UL
#define CACHE_LINE_MASK	MASK(CACHE_LINE_SHIFT)

#define POINTER_BITS	64UL
#define VALID_POINTER_BITS	48UL
#define VALID_POINTER_MASK	(~MASK(VALID_POINTER_BITS))

#ifndef AVX512_SUPPORT
	#if defined(__AVX512F__) && defined(__AVX512BW__) && defined(__AVX512DQ__)
		#define AVX512_SUPPORT 1
	#endif
#endif

#if defined(AVX512_SUPPORT)
#include <x86intrin.h>
#endif

#include <x86intrin.h>


/******copy from rapidjson******/

#define BYTEORDER_LITTLE_ENDIAN 1 // Little endian machine.
#define BYTEORDER_BIG_ENDIAN 0 // Big endian machine.

//#define BYTEORDER_ENDIAN BYTEORDER_LITTLE_ENDIAN

#ifndef BYTEORDER_ENDIAN
    // Detect with GCC 4.6's macro.
	#if defined(__BYTE_ORDER__)
		#if (__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__)
			#define BYTEORDER_ENDIAN BYTEORDER_LITTLE_ENDIAN
		#elif (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
			#define BYTEORDER_ENDIAN BYTEORDER_BIG_ENDIAN
		#else
			#error "Unknown machine byteorder endianness detected. User needs to define BYTEORDER_ENDIAN."
	#endif
    // Detect with GLIBC's endian.h.
	#elif defined(__GLIBC__)
		#include <endian.h>
		#if (__BYTE_ORDER == __LITTLE_ENDIAN)
			#degcc compiler.h -march=nativefine BYTEORDER_ENDIAN BYTEORDER_BIG_ENDIAN
		#else
			#error "Unknown machine byteorder endianness detected. User needs to define BYTEORDER_ENDIAN."
		#endif
    // Detect with _LITTLE_ENDIAN and _BIG_ENDIAN macro.
	#elif defined(_LITTLE_ENDIAN) && !defined(_BIG_ENDIAN)
		#define BYTEORDER_ENDIAN BYTEORDER_LITTLE_ENDIAN
	#elif defined(_BIG_ENDIAN) && !defined(_LITTLE_ENDIAN)
		#define BYTEORDER_ENDIAN BYTEORDER_BIG_ENDIAN
    // Detect with architecture macros.
	#elif defined(__sparc) || defined(__sparc__) || defined(_POWER) || defined(__powerpc__) || defined(__ppc__) || defined(__hpux) || defined(__hppa) || defined(_MIPSEB) || defined(_POWER) || defined(__s390__)
		#define BYTEORDER_ENDIAN BYTEORDER_BIG_ENDIAN
	#elif defined(__i386__) || defined(__alpha__) || defined(__ia64) || defined(__ia64__) || defined(_M_IX86) || defined(_M_IA64) || defined(_M_ALPHA) || defined(__amd64) || defined(__amd64__) || defined(_M_AMD64) || defined(__x86_64) || defined(__x86_64__) || defined(_M_X64) || defined(__bfin__)
		#define BYTEORDER_ENDIAN BYTEORDER_LITTLE_ENDIAN
	#elif defined(_MSC_VER) && (defined(_M_ARM) || defined(_M_ARM64))
		#define BYTEORDER_ENDIAN BYTEORDER_LITTLE_ENDIAN
	#else
		#error "Unknown machine byteorder endianness detected. User needs to define BYTEORDER_ENDIAN."
	#endif
#endif



/******copy from wormhole******/

typedef char            s8 __attribute__((aligned(1)));
typedef short           s16 __attribute__((aligned(2)));
typedef int             s32 __attribute__((aligned(4)));
typedef long            s64 __attribute__((aligned(8)));

static_assert(sizeof(s8) == 1, "sizeof(s8)");
static_assert(sizeof(s16) == 2, "sizeof(s16)");
static_assert(sizeof(s32) == 4, "sizeof(s32)");
static_assert(sizeof(s64) == 8, "sizeof(s64)");

typedef unsigned char   u8 __attribute__((aligned(1)));
typedef unsigned short  u16 __attribute__((aligned(2)));
typedef unsigned int    u32 __attribute__((aligned(4)));
typedef unsigned long   u64 __attribute__((aligned(8)));

static_assert(sizeof(u8) == 1, "sizeof(u8)");
static_assert(sizeof(u16) == 2, "sizeof(u16)");
static_assert(sizeof(u32) == 4, "sizeof(u32)");
static_assert(sizeof(u64) == 8, "sizeof(u64)");

#define MAX_LONG_INTEGER	0xFFFFFFFFFFFFFFFF


#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)


/******copy from linux kernel******/

#define __packed	__attribute__((__packed__))

#define __scalar_type_to_expr_cases(type)				\
		unsigned type:	(unsigned type)0,			\
		signed type:	(signed type)0

#define __unqual_scalar_typeof(x) typeof(				\
		_Generic((x),						\
			char:	(char)0,				\
			__scalar_type_to_expr_cases(char),		\
			__scalar_type_to_expr_cases(short),		\
			__scalar_type_to_expr_cases(int),		\
			__scalar_type_to_expr_cases(long),		\
			__scalar_type_to_expr_cases(long long),	\
			default: (x)))

#define READ_ONCE(x)	(*(const volatile __unqual_scalar_typeof(x) *)&(x))


#define WRITE_ONCE(x, val)						\
do {									\
	*(volatile typeof(x) *)&(x) = (val);				\
} while (0)


#undef offsetof
#define offsetof(TYPE, MEMBER)	__builtin_offsetof(TYPE, MEMBER)

#define container_of(ptr, type, member) ({				\
	void *__mptr = (void *)(ptr);					\
	((type *)(__mptr - offsetof(type, member))); })

#define __get_unaligned_t(type, ptr) ({						\
	const struct { type x; }__packed *__pptr = (typeof(__pptr))(ptr);	\
	__pptr->x;								\
})

#define le32_to_cpu bswap_32
#define le64_to_cpu bswap_64

static inline u32 get_unaligned_le32(const void *p)
{
	return le32_to_cpu(__get_unaligned_t(u32, p));
}

static inline u64 get_unaligned_le64(const void *p)
{
	return le64_to_cpu(__get_unaligned_t(u64, p));
}

/******copy from masstree******/


static inline void memory_fence() {
    asm volatile("mfence" : : : "memory");
}

static inline void acquire_fence() {
    asm volatile("" : : : "memory");
}

static inline void release_fence() {
    asm volatile("" : : : "memory");
}

static inline void compiler_fence() {
    asm volatile("" : : : "memory");
}

static inline void spin_fence() {
    asm volatile("pause" : : : "memory"); // equivalent to "rep; nop"
}

/**********others**********/
#define cas(ptr, old, new)	(__sync_val_compare_and_swap((ptr), (old), (new)))
#define cas_bool(ptr, old, new)	(__sync_bool_compare_and_swap((ptr), (old), (new)))

#define atomic_add(ptr, inc)	(__atomic_fetch_add((ptr), (inc), __ATOMIC_RELAXED))

/*low temporal locality*/
static inline void prefech_r0(void *addr) {
	__builtin_prefetch(addr, 0, 0);
}

/*high temporal locality*/
static inline void prefech_r3(void *addr) {
	__builtin_prefetch(addr, 0, 3);
}

static inline void prefech_w0(void *addr) {
	__builtin_prefetch(addr, 1, 0);
}

//value != 0
static inline int msb32(int value) {
	//return 31 - __builtin_clz(value);
	return _bit_scan_reverse(value);
}

static inline int lsb32(int value) {
	//return __builtin_ctz(value);
	return _bit_scan_forward(value);
}

/*static inline int msb64(u64 value) {
	//unsigned index;
	//_BitScanReverse64(&index, value);
	//return index;
	return __bsrq(value);
}

static inline int lsb64(u64 value) {
	//unsigned index;
	//_BitScanForward64(&index, value);
	//return index;
	return __bsfq(value);
}

static inline unsigned long lzcnt_u64(u64 value) {
	return _lzcnt_u64(value);
	//return ((val == 0UL) ? 64 : __builtin_clzl(val));
}*/

/*static inline unsigned long long timestamp_loose() {
	return __rdtsc();
}

static inline unsigned long long timestamp_strict() {
	return __rdtscp();
}*/

typedef struct bitmask BITMASK;

static inline int max_node_num() {
	return numa_num_configured_nodes();
}

static inline int max_cpu_num() {
	return numa_num_possible_cpus();
}

static inline BITMASK *alloc_cpu_bitmask() {
	return numa_allocate_cpumask();
}

static inline void free_cpu_bitmask(BITMASK *mask) {
	numa_free_cpumask(mask);
}

static inline int *alloc_cpu_array() {
	return malloc(sizeof(int) * max_cpu_num());
}

static inline void free_cpu_array(int *arr) {
	free(arr);
}

static inline int is_bitmask_set(const BITMASK *bmp, unsigned int n) {
	return numa_bitmask_isbitset(bmp, n);
}

static inline void clear_bitmask(BITMASK *bmp) {
	numa_bitmask_clearall(bmp);
}

static inline int get_node_cpuid(int node, BITMASK *mask) {
	return numa_node_to_cpus(node, mask);
}

static inline int cpuid_to_array(BITMASK *mask, int *arr) {
	int i, total = 0;

	for (i = 0; i < max_cpu_num(); ++i)
		if (is_bitmask_set(mask, i))
			arr[total++] = i;
	
	return total;
}


static inline void *alloc_node_page(size_t size, int node) {
	return numa_alloc_onnode(size, node);
}

static inline void free_node_page(void *addr, size_t size) {
	numa_free(addr, size);
}


static inline unsigned long sys_time_us() {
	struct  timeval val;
	gettimeofday(&val, NULL);
	return val.tv_sec * 1000000 + val.tv_usec;
}

/*static inline int bind_cpu(pthread_t thread, int id) {
	cpu_set_t mask;
	CPU_ZERO(&mask);

	CPU_SET(id, &mask);

	if (pthread_setaffinity_np(thread, sizeof(mask), &mask) == -1)
		return -1;

	return 0;
}*/


static inline int create_default_thread(
							pthread_t *restrict thread,
                    		void *(*start_routine)(void *),
		            		void *restrict arg) {
	return pthread_create(thread, NULL, start_routine, arg);
}

static inline int create_binding_thread(
							pthread_t *restrict thread,
                    		void *(*start_routine)(void *),
                    		void *restrict arg, 
							int cpuid) {
	pthread_attr_t p_attr;
	cpu_set_t mask;
	int ret;

	if (pthread_attr_init(&p_attr))
		return -1;

	CPU_ZERO(&mask);
	CPU_SET(cpuid, &mask);

	if (pthread_attr_setaffinity_np(&p_attr, sizeof(mask), &mask))
		return -1;

	ret = pthread_create(thread, &p_attr, start_routine, arg);

	pthread_attr_destroy(&p_attr);

	return ret;
}

static inline int create_grouping_thread(
							pthread_t *restrict thread,
                    		void *(*start_routine)(void *),
                    		void *restrict arg, 
							BITMASK *cpu_mask) {
	pthread_attr_t p_attr;
	cpu_set_t mask;
	int i, ret;

	if (pthread_attr_init(&p_attr))
		return -1;

	CPU_ZERO(&mask);

	for (i = 0; i < max_cpu_num(); ++i)
		if (is_bitmask_set(cpu_mask, i))
			CPU_SET(i, &mask);

	if (pthread_attr_setaffinity_np(&p_attr, sizeof(mask), &mask))
		return -1;

	ret = pthread_create(thread, &p_attr, start_routine, arg);

	pthread_attr_destroy(&p_attr);

	return ret;
}

static inline int unbind_thread(pthread_t thread, int cpu_num) {
	cpu_set_t mask;
	int i;

	CPU_ZERO(&mask);

	for (i = 0; i < cpu_num; ++i)
		CPU_SET(i, &mask);

	return pthread_setaffinity_np(thread, sizeof(cpu_set_t), &mask);
}

static inline int terminate_the_thread(pthread_t thread) {
	return pthread_kill(thread, SIGINT);
}

static inline int malloc_aligned(void **memptr, size_t alignment, size_t size) {
	return posix_memalign(memptr, alignment, size);
}

static inline void *malloc_prefault_page_aligned(size_t size) {
	return mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON | MAP_POPULATE, -1, 0);
}

static inline void *malloc_page_aligned(size_t size) {
	return mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, -1, 0);
}

static inline int free_page_aligned(void *addr, size_t size) {
	return munmap(addr, size);
}

static inline int prefault_page_aligned(void *addr, size_t size) {
	//return madvise(addr, size, MADV_POPULATE_WRITE);
	return 0;
}


#ifdef  __cplusplus
}
#endif	//__cplusplus

#endif	//__COMPILER_H
