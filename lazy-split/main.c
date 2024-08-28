
#include "dht.h"
//#include "prehash.h"
#include <stdio.h>



void *func(void *para) {
	struct kv_ret_context *contex;
	u64 i;
	int ind = (int)para;

	u64 t1, t2;

	t1 = sys_time_us();

	for (i = 51200000 * ind; i < 51200000 * (ind + 1); ++i) {
		//printf("%ld\n", i);//key = prehash64(&i, sizeof(u64), 0);
		if (dht_kv_put(i, i)) {
			printf("dht_kv_put %lu failed\n", i);
			return NULL;
		}
	}

	t2 = sys_time_us();
	printf("%lu\n", t2 - t1);


	for (i = 51200000 * ind; i < 51200000 * (ind + 1); ++i) {
		contex = dht_kv_get(i);

		if (failed_kv_ret_context(contex)) {
			printf("dht_kv_get %lu memory alloc fail\n", i);
			continue;
		}

		if (empty_kv_ret_context(contex)) {
			printf("dht_kv_get %lu failed\n", i);
			continue;
		}
		//printf("%lu %lu\n", contex->key, contex->value);
		if (contex->key != contex->value)
			printf("noooooo\n");

		free(contex);
	}

	t1 = sys_time_us();
	printf("%lu\n", t1 - t2);


	printf("finish\n");


	while (1) {}
}


int main() {
	int ret;
	void *(*fuuc_arr[20])(void *);
	void *para_arr[20];

	ret = dht_init_structure();
	
	if (ret) {
		printf("dht_init_structure failed\n");
	}

	/*fuuc_arr[0] = &func1;
	para_arr[0] = NULL;

	fuuc_arr[1] = &func2;
	para_arr[1] = NULL;

	fuuc_arr[2] = &func3;
	para_arr[2] = NULL;*/

	/*fuuc_arr[0] = &func;
	para_arr[0] = NULL;*/

	int i;
	
	for (i = 0; i < 20; ++i) {
		fuuc_arr[i] = &func;
		para_arr[i] = (void *)i;
	}

	ret = dht_create_thread(&fuuc_arr[0], &para_arr[0], 5, 6);

	if (ret) {
		printf("dht_create_thread failed\n");
	}

	while (1) {}
	return 0;
}
