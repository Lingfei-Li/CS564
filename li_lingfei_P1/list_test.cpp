#include<iostream>
#include<string>
#include<sstream>
#include<vector>
#include<algorithm>
#include<fstream>
#include<ctype.h>
#include<cstdlib>
#include"wl.h"

using namespace std;
int main() {

    List* list = new List();

    for(int i = 0; i < 101; i ++) {
        list->add(i);
    }
    list->dump();



	return 0;
}
