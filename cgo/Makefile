
.PHONY: all clean
SRC:=$(shell pwd)
INC:=-I$(SRC)/../agent/node -I$(SRC)/../agent/event
LIB:=-L$(SRC)

all:
	gcc -Wall -g main.c -o main $(INC) $(LIB) -letcd -lpthread -lrt
clean:
	rm -rf main