
CPP = g++
CC = gcc

LIB = -lpthread -ldl
BIN = sqlite load

all : $(BIN)
sqlite : sqlite3.c shell.c
	$(CC) -o $@ $^ $(LIB)
load : load.cpp sqlite3.o
	$(CPP) -o $@ $^ $(LIB)
sqlite3.o : sqlite3.c
	$(CC) -o $@ -c $^

.PHONY: all, clean

t:
	./load

SQL = "sqlite3 nutrients.db -header -column"
tt:
	sqlite3 nutrients.db -header < Query1.txt
	sqlite3 nutrients.db -header < Query2.txt
	sqlite3 nutrients.db -header < Query3.txt
	sqlite3 nutrients.db -header < Query4.txt
	sqlite3 nutrients.db -header < Query5.txt
	sqlite3 nutrients.db -header < Query6.txt
	sqlite3 nutrients.db -header < Query7.txt

t3:
	sqlite3 nutrients.db -header < Query3.txt
