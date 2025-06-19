
#include "dht.h"
#include <stdio.h>

#define NUMA_NUM	2
#define WT_NUM_PER_NUMA	36
#define MAX_WT_NUM_PER_NUMA	36


void *func(void *para) {
	u64 i, ent = 51200000;
	u64 val;
	u64 t1, t2;
	int ind = (int)para;
	
	t1 = sys_time_us();

	for (i = ent * ind; i < ent * (ind + 1); ++i) {
		if (dht_kv_insert(i, i)) {
			printf("dht_kv_insert %lu failed\n", i);
			return NULL;
		}
	}

	t2 = sys_time_us();

	printf("thread %d inserts %lu kvs: %lu us\n", ind, ent, t2 - t1);

	for (i = ent * ind; i < ent * (ind + 1); ++i) {
		if (dht_kv_lookup(i, &val) || val != i) {
			printf("dht_kv_lookup %lu error\n", i);
			return NULL;
		}
	}

	t1 = sys_time_us();

	printf("thread %d lookups %lu kvs: %lu us\n", ind, ent, t1 - t2);

	while (1) {}
}


int main() {
	struct dht_node_context n_context;
	int max_node_thread[NUMA_NUM];
	int node_thread[NUMA_NUM];
	struct dht_work_function *work_func[NUMA_NUM];
	struct dht_work_function work_func_arr[NUMA_NUM][MAX_WT_NUM_PER_NUMA];

	int n, i, ret;

	for (n = 0; n < NUMA_NUM; ++n) {
		max_node_thread[n] = MAX_WT_NUM_PER_NUMA;
		node_thread[n] = WT_NUM_PER_NUMA;
                work_func[n] = &work_func_arr[n][0];

		for (i = 0; i < WT_NUM_PER_NUMA; ++i) {
			work_func_arr[n][i].start_routine = &func;
			work_func_arr[n][i].arg = (void *)(n * WT_NUM_PER_NUMA + i);
		}
	}

	n_context.nodes = NUMA_NUM;
	n_context.max_node_thread = &max_node_thread[0];

	ret = dht_init_structure(&n_context);
	
	if (ret) {
		printf("dht_init_structure failed\n");
		return -1;
	}

	n_context.node_thread = &node_thread[0];
	n_context.node_func = &work_func[0];

	ret = dht_create_thread(&n_context);

	if (ret) {
		printf("dht_create_thread failed\n");
		return -1;
	}

	while (1) {}
	return 0;
}
