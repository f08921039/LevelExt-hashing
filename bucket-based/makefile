CC=gcc
DFLAGS=-DDHT_INTEGER -DLARGE_EH_SEGMENT -DWIDE_EH_BUCKET_INDEX_BIT -DTEST_NOPREHASH
CFLAGS=-Wall -O3 -pthread $(DFLAGS) -g

OBJS = main.o dht.o dht_init.o ext_hash.o per_thread.o background.o prehash.o

main: $(OBJS)
	$(CC) $(CFLAGS) -o main $(OBJS)
main.o : compiler.h dht.h main.c
	$(CC) $(CFLAGS) -c main.c
dht.o : dht.c dht.h dht_init.h prehash.h \
	kv.h compiler.h per_thread.h background.h ext_hash.h
	$(CC) $(CFLAGS) -c dht.c
dht_init.o : dht_init.c dht_init.h per_thread.h background.h kv.h compiler.h \
		ext_hash.h
	$(CC) $(CFLAGS) -c dht_init.c
ext_hash.o : ext_hash.c ext_hash.h kv.h compiler.h per_thread.h background.h
	$(CC) $(CFLAGS) -c ext_hash.c
background.o : background.c background.h per_thread.h compiler.h ext_hash.h
	$(CC) $(CFLAGS) -c background.c
per_thread.o : per_thread.c per_thread.h kv.h compiler.h
	$(CC) $(CFLAGS) -c per_thread.c
prehash.o : prehash.c
	$(CC) $(CFLAGS) -c prehash.c

clean:
	rm -f main $(OBJS)

