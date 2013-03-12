CC = gcc

INSTALL_PATH = $$HOME/simgrid
INCLUDES = -I../include
DEFS = -L$(INSTALL_PATH)/lib
LDADD = -lm -lsimgrid

SRC=$(wildcard *.c)
BIN=$(SRC:.c=.bin)

all: $(BIN)

%.bin: %.c
	$(CC) $(INCLUDES) $(DEFS) -o $@ $^ ../libmrsg.a $(LDADD)

clean:
	rm -vf *.bin *.log *.trace *.plist

.PHONY: clean
