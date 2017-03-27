##############################################################
#               CMake Project Wrapper Makefile               #
############################################################## 
CC = g++
CFLAGS = -std=c++11 -Wall -g
OBJ = obj
LIB = lib


#all: $(LIB)/bufmgr.a $(OBJ)/filescan.o $(OBJ)/main.o $(OBJ)/btree.o $(OBJ)/test.o
#	rm -f ../relA*;\
#	$(CC) $(CFLAGS) -I. obj/filescan.o obj/main.o obj/btree.o lib/bufmgr.a lib/exceptions.a -o badgerdb_main

all: $(LIB)/bufmgr.a $(OBJ)/filescan.o $(OBJ)/mytest.o $(OBJ)/btree.o $(OBJ)/main.o
	rm -f ../relA*;\
	$(CC) $(CFLAGS) -I. obj/filescan.o obj/mytest.o obj/btree.o lib/bufmgr.a lib/exceptions.a -o out.mytest 
	$(CC) $(CFLAGS) -I. obj/filescan.o obj/main.o obj/btree.o lib/bufmgr.a lib/exceptions.a -o badgerdb_main

$(LIB)/bufmgr.a: $(LIB)/exceptions.a buffer.* file.* page.* bufHashTbl.*
	cd $(OBJ)/;\
	$(CC) $(CFLAGS) -I.. -c ../buffer.cpp ../file.cpp ../page.cpp ../bufHashTbl.cpp;\
	ar cq ../lib/bufmgr.a buffer.o file.o page.o bufHashTbl.o

$(LIB)/exceptions.a: exceptions/*
	cd $(OBJ)/exceptions;\
	$(CC) $(CFLAGS) -c -I../../ ../../exceptions/*.cpp;\
	ar cq ../../lib/exceptions.a *.o

$(OBJ)/filescan.o: filescan.*
	cd $(OBJ)/;\
	$(CC) $(CFLAGS) -c -I../ ../filescan.cpp

$(OBJ)/main.o: main.cpp
	cd $(OBJ)/;\
	$(CC) $(CFLAGS) -c -I../ ../main.cpp

$(OBJ)/mytest.o: mytest.cpp
	cd $(OBJ)/;\
	$(CC) $(CFLAGS) -c -I../ ../mytest.cpp

$(OBJ)/btree.o: btree.*
	cd $(OBJ)/;\
	$(CC) $(CFLAGS) -c -I../ ../btree.cpp

tt:
	./out.mytest

t:
	rm rel*
	./out.mytest

t0:
	./badgerdb_main 0

t1:
	./badgerdb_main 1

t2:
	./badgerdb_main 2

t3:
	./badgerdb_main 3

clean:
	#rm -rf $(OBJ)/exceptions/*.o
	rm -rf $(OBJ)/*.o
	#rm -rf $(LIB)/*
	#rm -rf exceptions/*.o
	rm -f badgerdb_main
	rm -f out.mytest

ca:
	#rm -rf $(OBJ)/exceptions/*.o
	rm -rf $(OBJ)/*.o
	rm -rf $(LIB)/*
	#rm -rf exceptions/*.o
	rm -f badgerdb_main
	rm rel*

doc:
	doxygen Doxyfile
