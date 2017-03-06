
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
#include <iostream>

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

    for(int i = 0; i < input[0]; i ++) {
        int key = i;

        RecordId rid;
        rid.page_number = i;
        rid.slot_number = i;

        index.insertEntry((void*)&key, rid);
    }

    index.dumpAllLevels();

    char cmd;
    int arg;
    while(std::cin>>cmd) {
        if(cmd == 'i') {
            std::cin>>arg;
            int key = arg;
            RecordId rid;
            rid.page_number = arg;
            rid.slot_number = arg;
            index.insertEntry((void*)&key, rid);
        }
        else if(cmd == 'd') {
            std::cin>>arg;
//            int key = arg;
//            index.deleteEntry((void*)&key);
        }
        else if(cmd == 'p') {

        }
        else if(cmd == 'q') {
            return 0;
        }
        else {

        }
        index.dumpAllLevels();
//        bufMgr->printSelfNonNull();
//        bufMgr->printSelf();
    }


    int low = 10, high = 20;
    index.startScan((void*)&low, GTE, (void*)&high, LTE);
    printf("Starting scan\n");
    try {
        RecordId rid;
        rid.page_number = 1;
        while(rid.page_number != 0) {
            index.scanNext(rid);
            printf("rid.pageNo: %d slotNo: %d\n", rid.page_number, rid.slot_number);
        }
    } catch(IndexScanCompletedException e) {
    }
    index.endScan();


//    index.dumpAllLevels();
//    bufMgr->printSelfPinned();
//    bufMgr->printSelfNonNull();


    delete bufMgr;


    return 0;
}
