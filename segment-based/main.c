
#include "dht.h"
#include <stdio.h>


/*void *func1(void *para) {
	struct kv_get_contex *contex;
	struct kv_get_contex tmp_contex;

	if (dht_kv_put(156324, 666)) {
		printf("dht_kv_put failed\n");
		return NULL;
	}

	contex = dht_kv_get(156324);
	if (!contex) {
		printf("dht_kv_get failed\n");
		return NULL;
	}
	printf("%lu %lu\n", contex->key, contex->value);
	free(contex);

	if (dht_kv_delete(156324)) {
		printf("dht_kv_delete failed\n");
		return NULL;
	}

	contex = dht_kv_get(156324);
	if (!contex) {
		printf("dht_kv_get not found key 156324\n");
	}

	if (dht_kv_put(156324, 168)) {
		printf("dht_kv_put failed\n");
		return NULL;
	}

	tmp_contex.key = 156324;

	if (dht_kv_get_context(&tmp_contex)) {
		printf("dht_kv_get failed\n");
		return NULL;
	}contex = dht_kv_get(156324);
	if (!contex) {
		printf("dht_kv_get failed\n");
		return NULL;
	}
	printf("%lu %lu\n", contex->key, contex->value);
	free(contex);
		
	}
	return NULL;
}*/

void *func1(void *para) {
	struct kv_get_contex *contex;
	u64 i, key;
	u64 interval = 576460752303423488 / (512*256);
u64 t1, t2;

	t1 = sys_time_us();
	for (i = 0; i < 256000 * 85; ++i) {
		key = (i % (512*256)) * interval + i;
		if (dht_kv_put(key, key)) {
			printf("dht_kv_put %lu failed\n", i);
			return NULL;
		}

	}
	t2 = sys_time_us();
	printf("%lu\n", t2 - t1);

	for (i = 256000 * 85; i < 256000 * 170; ++i) {
		key = (i % (512*256)) * interval + i;
		contex = dht_kv_get(key);
		if (!contex) {
			printf("dht_kv_get %lu memory alloc fail\n", i);
			continue;
		}

		if (empty_kv_get_contex(contex)) {
			printf("dht_kv_get %lu failed\n", i);
			continue;
		}
		//printf("%lu %lu\n", contex->key, contex->value);
		if (key != contex->key || contex->key != contex->value)
			printf("noooooo\n");
		free(contex);
	}

	t1 = sys_time_us();
	printf("%lu\n", t1 - t2);

	/*for (i = 0; i < 256000 * 128; ++i) {
		key = (i % (512*128)) * interval + i;
		if (dht_kv_put(key, key + 1111)) {
			printf("dht_kv_put %lu failed\n", i);
			return NULL;
		}
	}*/


	/*for (i = 0; i < 256000 * 128; ++i) {
		//printf("dht_kv_get %d\n", i);
		key = (i % (512*128)) * interval + i;
		contex = dht_kv_get(key);
		if (!contex) {
			printf("dht_kv_get %lu memory alloc fail\n", i);
			continue;256000 * 128
		}

		if (empty_kv_get_contex(contex)) {
			printf("dht_kv_get %lu failed\n", i);
			continue;
		}
		//printf("%lu %lu\n", contex->key, contex->value);
		if (contex->key != contex->value)
			printf("noooooo\n");
		free(contex);
	}*/

	printf("finish\n");
	/*for (i = 0; i < 256 * 2 + 102400; ++i) {
		if (dht_kv_put(i, i)) {
			printf("dht_kv_put %d failed\n", i);
			return NULL;
		}
	}*/

	/*for (i = 0; i < 256 * 2 + 102400; ++i) {
		if (dht_kv_put(i, 256 * 2 + 102400 - i)) {
			printf("dht_kv_put %d failed\n", i);
			return NULL;256000 * 128
		}
	}*/


	/*for (i = 0; i < 256 * 2 + 102400; ++i) {
		printf("dht_kv_get %d\n", i);
		contex = dht_kv_get(i);
		if (!contex) {
			printf("dht_kv_get %d failed\n", i);
			return NULL;
		}
		printf("%lu %lu\n", contex->key, contex->value);
		free(contex);
	}*/


	while (1) {}
}


void *func2(void *para) {
	struct kv_get_contex *contex;
	u64 i, key;
	u64 interval = 576460752303423488 / (512*256);
u64 t1, t2;

	t1 = sys_time_us();
	for (i = 256000 * 85; i < 256000 * 170; ++i) {
		key = (i % (512*256)) * interval + i;
		if (dht_kv_put(key, key)) {
			printf("dht_kv_put %lu failed\n", i);
			return NULL;
		}

	}
	t2 = sys_time_us();
	printf("%lu\n", t2 - t1);


	for (i = 256000 * 170; i < 256000 * 256; ++i) {
		key = (i % (512*256)) * interval + i;
		contex = dht_kv_get(key);
		if (!contex) {
			printf("dht_kv_get %lu memory alloc fail\n", i);
			continue;
		}

		if (empty_kv_get_contex(contex)) {
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


void *func3(void *para) {
	struct kv_get_contex *contex;
	u64 i, key;
	u64 interval = 576460752303423488 / (512*256);
u64 t1, t2;

	t1 = sys_time_us();
	for (i = 256000 * 170; i < 256000 * 256; ++i) {
		key = (i % (512*256)) * interval + i;
		if (dht_kv_put(key, key)) {
			printf("dht_kv_put %lu failed\n", i);
			return NULL;
		}
	}
	t2 = sys_time_us();
	printf("%lu\n", t2 - t1);


	for (i = 0; i < 256000 * 85; ++i) {
		key = (i % (512*256)) * interval + i;
		contex = dht_kv_get(key);
		if (!contex) {
			printf("dht_kv_get %lu memory alloc fail\n", i);
			continue;
		}

		if (empty_kv_get_contex(contex)) {
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


void *func(void *para) {
	struct kv_get_contex *contex;
	u64 i;
	int ind = (int)para;

u64 t1, t2;

	t1 = sys_time_us();
	for (i = 51200000 * ind; i < 51200000 * (ind + 1); ++i) {
		
		if (dht_kv_put(i, i)) {
			printf("dht_kv_put %lu failed\n", i);
			return NULL;
		}
	}
	t2 = sys_time_us();
	printf("%lu\n", t2 - t1);


	for (i = 51200000 * ind; i < 51200000 * (ind + 1); ++i) {
		contex = dht_kv_get(i);
		if (!contex) {
			printf("dht_kv_get %lu memory alloc fail\n", i);
			continue;
		}

		if (empty_kv_get_contex(contex)) {
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
	
	for (i = 0; i < 4; ++i) {
		fuuc_arr[i] = &func;
		para_arr[i] = (void *)i;
	}

	ret = dht_create_thread(&fuuc_arr[0], &para_arr[0], 4, 6);

	if (ret) {
		printf("dht_create_thread failed\n");
	}

	while (1) {}
	return 0;
}
