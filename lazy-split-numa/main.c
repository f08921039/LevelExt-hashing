
#include "dht.h"
#include <stdio.h>

#define NUMA_NUM	2
#define WT_NUM_PER_NUMA	36
#define MAX_WT_NUM_PER_NUMA	36

void *func(void *para) {
	struct dht_kv_context *contex;
	u64 i, ent = 51200000;
	int ind = (int)para;

	u64 t1, t2;

	t1 = sys_time_us();

	for (i = ent * ind; i < ent * (ind + 1); ++i) {
		if (dht_kv_put(i, i)) {
			printf("dht_kv_put %lu failed\n", i);
			return NULL;
		}
	}

	t2 = sys_time_us();

	printf("thread %d puts %lu kvs: %lu us\n", ind, ent, t2 - t1);


	for (i = ent * ind; i < ent * (ind + 1); ++i) {
		contex = dht_kv_get(i);

		if (failed_dht_kv_context(contex)) {
			printf("dht_kv_get %lu memory alloc fail\n", i);
			continue;
		}

		if (empty_dht_kv_context(contex)) {
			printf("dht_kv_get %lu failed\n", i);
			continue;
		}

		if (contex->key != contex->value)
			printf("noooooo\n");

		free(contex);
	}

	t1 = sys_time_us();

	printf("thread %d gets %lu kvs: %lu us\n", ind, ent, t1 - t2);


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
