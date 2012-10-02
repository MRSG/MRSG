CC = gcc

INSTALL_PATH = $$HOME/simgrid
INCLUDES = -I../include
DEFS = -L$(INSTALL_PATH)/lib
LDADD = -lm -lsimgrid

all: hello.bin

%.bin: %.c
	$(CC) $(INCLUDES) $(DEFS) -o $@ $^ ../libmrsg.a $(LDADD)

clean:
	rm -vf *.bin *.log

.PHONY: clean
