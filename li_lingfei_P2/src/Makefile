##############################################################
#               CMake Project Wrapper Makefile               #
############################################################## 

all:
	g++ -std=c++11 *.cpp exceptions/*.cpp -I. -Wall -o badgerdb_main

t:
	./badgerdb_main

clean:
	rm -f badgerdb_main test.? ../test.?

doc:
	doxygen Doxyfile
