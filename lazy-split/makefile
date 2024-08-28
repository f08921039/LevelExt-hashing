CC=gcc
DFLAGS=-DDHT_INTEGER -DLARGE_EH_SEGMENT -DWIDE_EH_BUCKET_INDEX_BIT
CFLAGS=-Wall -O3 -pthread $(DFLAGS) -g

OBJS = main.o dht.o dht_init.o eh_context.o eh_alloc.o eh_dir.o \
		eh_seg.o eh_rehash.o per_thread.o background.o prehash.o

main: $(OBJS)
	$(CC) $(CFLAGS) -o main $(OBJS)
main.o : dht.h main.c
	$(CC) $(CFLAGS) -c main.c
dht.o : dht.c dht.h dht_init.h prehash.h \
		kv.h compiler.h per_thread.h eh.h
	$(CC) $(CFLAGS) -c dht.c
dht_init.o : dht_init.c dht_init.h per_thread.h \
				background.h kv.h compiler.h eh.h
	$(CC) $(CFLAGS) -c dht_init.c
eh_context.o : eh_context.h eh_context.c eh_dir.h eh_seg.h kv.h compiler.h
	$(CC) $(CFLAGS) -c eh_context.c
eh_alloc.o : eh_alloc.h eh_alloc.c eh_seg.h compiler.h per_thread.h
	$(CC) $(CFLAGS) -c eh_alloc.c
eh_dir.o : eh_dir.h eh_dir.c eh_seg.h eh_context.h kv.h compiler.h background.h
	$(CC) $(CFLAGS) -c eh_dir.c
eh_seg.o : eh_seg.h eh_seg.c eh_rehash.h kv.h compiler.h per_thread.h
	$(CC) $(CFLAGS) -c eh_seg.c
eh_rehash.o : eh_rehash.h eh_rehash.c eh_context.h eh_dir.h eh_seg.h \
				eh_alloc.h kv.h compiler.h per_thread.h background.h
	$(CC) $(CFLAGS) -c eh_rehash.c
background.o : background.c background.h per_thread.h compiler.h eh.h
	$(CC) $(CFLAGS) -c background.c
per_thread.o : per_thread.c per_thread.h kv.h compiler.h eh.h
	$(CC) $(CFLAGS) -c per_thread.c
prehash.o : prehash.c
	$(CC) $(CFLAGS) -c prehash.c

clean:
	rm -f main $(OBJS)

