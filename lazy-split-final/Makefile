CFLAGS = -Wall -O3 -g -pthread -DDHT_INTEGER
CFLAGS += -I.
LIBS = -lnuma
MALLOC = -I`jemalloc-config --includedir` \
	-L`jemalloc-config --libdir` -Wl,-rpath,`jemalloc-config --libdir` \
	-ljemalloc `jemalloc-config --libs`
NAME = main
ODIR = .
OBJ = o

OBJS = background.$(OBJ) \
	dht.$(OBJ) \
	dht_init.$(OBJ) \
	eh_alloc.$(OBJ) \
	eh_context.$(OBJ) \
	eh_dir.$(OBJ) \
	eh_rehash.$(OBJ) \
	eh_seg.$(OBJ) \
	main.$(OBJ) \
	per_thread.$(OBJ) \
	prehash.$(OBJ)

OBJECTS = $(patsubst %,$(ODIR)/%,$(OBJS))
VPATH = .

.c.$(OBJ):
	@test -d $(ODIR) || mkdir -p $(ODIR)
	$(CC) $(CFLAGS) -c -o $(ODIR)/$@ $< $(LIBS) $(MALLOC)

$(NAME):$(OBJS)
	$(CC) $(CFLAGS) -o $(ODIR)/$(NAME) $(OBJECTS) $(LIBS) $(MALLOC)

.PHONY: clean
clean:
	$(RM) $(ODIR)/$(NAME) $(OBJECTS)
