GCC ?= gcc
OBJS = src/lua-resty-zookeeper.o
TARGET = lib/resty/zk.so

ZOOKEEPER_INCLUDE_DIR = /usr/local/include
ZOOKEEPER_LIB_DIR = /usr/local/lib
LUAJIT_CFLAGS = $(shell pkg-config --cflags luajit)
LUAJIT_LFLAGS = $(shell pkg-config --libs luajit)

.PHONY: all clean

all: $(TARGET)

$(OBJS): %.o: %.c
	$(GCC) -o $@ -c -O3 -fPIC $(LUAJIT_CFLAGS) -I$(ZOOKEEPER_INCLUDE_DIR) $^

$(TARGET): $(OBJS)
	$(GCC) -shared $(OBJS) $(LUAJIT_LFLAGS) -L$(ZOOKEEPER_LIB_DIR) -lpthread -lm -ldl -lzookeeper_mt -o $@

clean:
	rm -f src/*.o
	rm -f lib/resty/*.so
