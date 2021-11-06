CPP :=g++
FLUSH_FLAG :=-DCLWB
CFLAGS :=-Iinclude -Itree -mclflushopt -mclwb -fmax-errors=5 -O3 $(FLUSH_FLAG)
CFLAGS_DEBUG :=-Iinclude -Itree -mclflushopt -mclwb -fmax-errors=5 -g -DDEBUG $(FLUSH_FLAG)

TREES := tree/*.h
LINK_LIB :=-lpmemobj -pthread

all: main datagen preload test
	@echo "finish make"

main: main.cc $(TREES)
	$(CPP) $(CFLAGS) main.cc -o main $(LINK_LIB)

preload: preload.cc $(TREES)
	$(CPP) $(CFLAGS) preload.cc -o preload $(LINK_LIB)

datagen: gen.cc
	$(CPP) $(CFLAGS) gen.cc -o datagen

test: test.cc $(TREES)
	$(CPP) $(CFLAGS) test.cc -o test $(LINK_LIB)

.PHONY: debug
debug: 
	$(CPP) $(CFLAGS_DEBUG) main.cc -o main $(LINK_LIB)
	$(CPP) $(CFLAGS_DEBUG) preload.cc -o preload $(LINK_LIB)
	$(CPP) $(CFLAGS_DEBUG) gen.cc -o datagen
	$(CPP) $(CFLAGS_DEBUG) test.cc -o test $(LINK_LIB)

.PHONY: clean
clean: 
	@rm main preload datagen test
