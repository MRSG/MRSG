CC = gcc

INSTALL_PATH = $$HOME/simgrid
INCLUDES = -I../include
DEFS = -L$(INSTALL_PATH)/lib
LDADD = -lm -lsimgrid

hello.bin: hello.c
	$(CC) $(INCLUDES) $(DEFS) -o $@ $^ ../libmrsg.a $(LDADD)

clean:
	rm -vf *.bin *.log

.PHONY: clean
