
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
#include <algorithm>

using namespace badgerdb;

typedef struct tuple {
	int i;
	double d;
	char s[64];
} RECORD;


void testDeletion(std::vector<int>& input, BTreeIndex& index, BufMgr* bufMgr);

void testScan(std::vector<int>& input, BTreeIndex& index, BufMgr* bufMgr);

void testInsertion(BTreeIndex& index, BufMgr& bufMgr);

void manualTest(BTreeIndex& index, BufMgr* bufMgr);

int main() {
    BufMgr * bufMgr = new BufMgr(100);
    const std::string relationName = "relation_test";
    std::string indexName;

    BTreeIndex index(relationName, indexName, bufMgr, offsetof(tuple,i), INTEGER);

    std::ifstream myfile;
    myfile.open ("input");

    int n;
    myfile>>n;
    myfile.close();
    std::vector<int> input;
    for(int i = 0; i < n; i ++) {
        int key = i*2;

        RecordId rid; rid.page_number = key; rid.slot_number = key; 

        input.push_back(key);
        index.insertEntry((void*)&key, rid);
    }

    index.dumpAllLevels();

    testDeletion(input, index, bufMgr);
//    manualTest(index, bufMgr);

    delete bufMgr;

    return 0;
}


void testDeletion(std::vector<int>& input, BTreeIndex& index, BufMgr* bufMgr) {

    std::vector<int> deletionIndex;
    for(size_t i = 0; i < input.size(); i ++) {
        deletionIndex.push_back(i);
    }
    std::random_shuffle(deletionIndex.begin(), deletionIndex.end());

    for(size_t i = 0; i < deletionIndex.size(); i ++) {
        int key = 11;
//        int key = input[deletionIndex[i]];
        printf("deleting %d...\n", key);
        index.deleteEntry((void*)&key);
        if(index.validate(false) == false) {
            break;
        }
    }

    return;
}

void testScan(std::vector<int>& input, BTreeIndex& index, BufMgr* bufMgr) {
    /*
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
    */


//    index.dumpAllLevels();
//    bufMgr->printSelfPinned();
//    bufMgr->printSelfNonNull();
}

void testInsertion(BTreeIndex& index, BufMgr& bufMgr) {

    return;
}


void manualTest(BTreeIndex& index, BufMgr* bufMgr) {
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
            int key = arg;
            index.deleteEntry((void*)&key);
        }
        else if(cmd == 'p') {

        }
        else if(cmd == 'q') {
            return;
        }
        else {

        }
        index.dumpAllLevels();
        index.validate(true);
        if(bufMgr->pinnedCnt() != 0) {
            bufMgr->printSelfPinned();
        }
//        bufMgr->printSelfNonNull();
//        bufMgr->printSelf();
    }

}
