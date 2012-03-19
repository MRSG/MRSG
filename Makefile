CC = gcc
CFLAGS = -Wall -g

INSTALL_PATH = $$HOME/simgrid
INCLUDES = -Iinclude -I$(INSTALL_PATH)/include
DEFS = -L$(INSTALL_PATH)/lib
LDADD = -lm -lsimgrid

BIN = libmrsg.a
OBJ = common.o simcore.o dfs.o master.o worker.o user.o

all: $(BIN)

$(BIN): $(OBJ)
	ar rcs $(BIN) $(OBJ)
#	$(CC) $(INCLUDES) $(DEFS) $(CFLAGS) $(LDADD) -o $@ $^

%.o: src/%.c include/*.h
	$(CC) $(INCLUDES) $(DEFS) $(CFLAGS) -c -o $@ $<

verbose: clean
	$(eval CFLAGS += -DVERBOSE)

debug: clean
	$(eval CFLAGS += -O0)

final: clean
	$(eval CFLAGS += -O2)

check:
	@grep --color=auto -A4 -n -E "/[/*](FIXME|TODO)" include/*.h src/*.c

clean:
	rm -vf $(BIN) *.o *.log *.trace

.SUFFIXES:
.PHONY: all check clean debug final verbose
