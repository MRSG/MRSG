CC = gcc

INSTALL_PATH = $$HOME/simgrid
INCLUDES = -I../include -I$(INSTALL_PATH)/include
DEFS = -L$(INSTALL_PATH)/lib
LDADD = -lm -lsimgrid

SRC=$(wildcard *.c)
BIN=$(SRC:.c=.bin)

all: $(BIN)

%.bin: %.c ../libmrsg.a
	$(CC) $(INCLUDES) $(DEFS) -o $@ $^ $(LDADD)

../libmrsg.a:
	make -C ../

clean:
	rm -vf *.bin *.csv *.trace *.plist

.PHONY: clean
