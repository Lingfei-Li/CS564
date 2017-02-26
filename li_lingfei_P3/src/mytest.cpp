
#include <vector>
#include "btree.h"
#include "page.h"
#include "filescan.h"
#include "page_iterator.h"
#include "file_iterator.h"
#include "exceptions/insufficient_space_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/end_of_file_exception.h"
#include <vector>
#include <fstream>

using namespace badgerdb;

typedef struct tuple {
	int i;
	double d;
	char s[64];
} RECORD;

int main() {
    BufMgr * bufMgr = new BufMgr(100);
    const std::string relationName = "relation_test";
    std::string indexName;

    BTreeIndex index(relationName, indexName, bufMgr, offsetof(tuple,i), INTEGER);

    std::vector<int> input;

    std::ifstream myfile;
    myfile.open ("input");

    int x;
    while(myfile>>x) {
        input.push_back(x);
    }

    myfile.close();

    for(size_t i = 0; i < input.size(); i ++) {

        int key = input[i];

        RecordId rid;
        rid.page_number = i;
        rid.slot_number = i;

        index.insertEntry((void*)&key, rid);

        char ch;
        std::cout<<"Paused\n";
        std::cin>>ch;
    }

    index.dumpLeaf();
    index.dumpLevel(0);

//    bufMgr->printSelfNonNull();

    delete bufMgr;


    return 0;
}
