
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

const char* padding = "                 ";


void testDeletion(std::vector<int>& input, BTreeIndex& index, BufMgr* bufMgr);

void testScan(std::vector<int>& input, BTreeIndex& index, BufMgr* bufMgr, int low, int high);

void insertEntries(std::vector<int>& input, BTreeIndex& index, BufMgr* bufMgr);

void testInsertion(BTreeIndex& index, BufMgr& bufMgr);

void manualTest(std::vector<int>& input, BTreeIndex& index, BufMgr* bufMgr);

Datatype datatype;

int main() {
    BufMgr * bufMgr = new BufMgr(100);
    const std::string relationName = "relation_test";
    std::string indexName;

    std::ifstream myfile;
    myfile.open ("input");

    std::string type;
    myfile>>type;
    if(type == "i" || type == "int") {
        datatype = INTEGER;
    }
    else if(type == "d" || type=="double"){
        datatype = DOUBLE;
    }
    else if(type == "s" || type=="str" || type=="string"){
        datatype = STRING;
    }

    BTreeIndex index(relationName, indexName, bufMgr, offsetof(tuple,i), datatype);


    std::string test;
    int n;
    myfile>>test>>n;
    std::vector<int> input;
    for(int i = 0; i < n; i ++) {
        int key = i*2;

        input.push_back(key);
    }

    if(test == "d" || test == "del" || test=="deletion") {
        testDeletion(input, index, bufMgr);
    }
    else if(test == "s" || test == "scan") {
        int low, high;
        myfile>>low>>high;
        testScan(input, index, bufMgr, low, high);
    }
    else if(test == "m" || test == "manual") {
        manualTest(input, index, bufMgr);
    }

    myfile.close();

    delete bufMgr;

    return 0;
}


void insertEntries(std::vector<int>& input, BTreeIndex& index, BufMgr* bufMgr) {
    for(size_t i = 0; i < input.size(); i ++) {
        if(datatype == INTEGER) {
            int key = input[i];
            RecordId rid; rid.page_number = input[i]; rid.slot_number = input[i]; 
            index.insertEntry((void*)&key, rid);
        }
        else if(datatype == DOUBLE) {
            double key = (double)(input[i]);
            RecordId rid; rid.page_number = input[i]; rid.slot_number = input[i]; 
            index.insertEntry((void*)&key, rid);
        }
        else if(datatype == STRING){
            char key[15];
            snprintf(key, 14, "%d%s", input[i], padding);
            RecordId rid; rid.page_number = input[i]; rid.slot_number = input[i]; 
            index.insertEntry((void*)&key, rid);
        }
    }
}

void testDeletion(std::vector<int>& input, BTreeIndex& index, BufMgr* bufMgr) {


    srand(time(0));
    int maxEpoch = 10;
    for(int epoch = 0; epoch < maxEpoch; epoch ++) {
        printf("Epoch %d\n", epoch);
        insertEntries(input, index, bufMgr);

        std::vector<int> deletionIndex;
        for(size_t i = 0; i < input.size(); i ++) {
            deletionIndex.push_back(i);
        }
        std::random_shuffle(deletionIndex.begin(), deletionIndex.end());

        for(size_t i = 0; i < deletionIndex.size(); i ++) {

//            index.dumpAllLevels();

            if(datatype == INTEGER) {
                int key = input[deletionIndex[i]];
                printf("deleting %d...\n", key);
                if(index.deleteEntry((void*)&key) == false || index.validate(false) == false) {
                    index.dumpAllLevels();
                    printf("Deletion test failed\n");
                    return;
                }
            }
            else if(datatype == DOUBLE) {
                double key = (double)input[deletionIndex[i]];
                printf("deleting %lf...\n", key);
                if(index.deleteEntry((void*)&key) == false || index.validate(false) == false) {
                    index.dumpAllLevels();
                    printf("Deletion test failed\n");
                    return;
                }
            }
            else if(datatype == STRING){
                char key[15];
                snprintf(key, 14, "%d%s", input[deletionIndex[i]], padding);
                printf("deleting %s...\n", key);
                if(index.deleteEntry((void*)&key) == false || index.validate(false) == false) {
                    index.dumpAllLevels();
                    printf("Deletion test failed\n");
                    return;
                }
            }
        }
    }

    printf("Deletion test passed\n");
}

void testScan(std::vector<int>& input, BTreeIndex& index, BufMgr* bufMgr, int low, int high) {

    try {
        index.endScan();
        printf("Error: endScan should throw ScanNotInitializedException\n");
        return;
    } catch(ScanNotInitializedException ex){ }

    insertEntries(input, index, bufMgr);

    index.dumpAllLevels();

    if(datatype == INTEGER) {
        index.startScan((void*)&low, GTE, (void*)&high, LTE);
    }
    else if(datatype == DOUBLE) {
        double lowDouble = (double)(low);
        double highDouble = (double)(high);
        index.startScan((void*)&lowDouble, GTE, (void*)&highDouble, LTE);
    }
    else if(datatype == STRING){
        char lowString[15];
        snprintf(lowString, 14, "%d%s", low, padding);
        char highString[15];
        snprintf(highString, 14, "%d%s", high, padding);
        index.startScan((void*)&lowString, GTE, (void*)&highString, LTE);
    }

    index.dumpAllLevels();
    bufMgr->printSelfPinned();

    printf("Starting scan\n");
    printf("low: %d, high: %d\n", low, high);
    try {
        RecordId rid;
        rid.page_number = 1;
        while(rid.page_number != 0) {
            index.scanNext(rid);
            printf("rid.pageNo: %d slotNo: %d\n", rid.page_number, rid.slot_number);
        }
    } catch(IndexScanCompletedException e) {
        printf("Scan completed\n");
    }

    index.endScan();

    index.dumpAllLevels();
    bufMgr->printSelfPinned();
}

void testInsertion(BTreeIndex& index, BufMgr& bufMgr) {

    return;
}


void manualTest(std::vector<int>& input, BTreeIndex& index, BufMgr* bufMgr) {

    insertEntries(input, index, bufMgr);

    index.dumpAllLevels();
    char cmd;
    int arg;
    while(std::cin>>cmd) {
        if(cmd == 'i') {
            if(datatype == INTEGER) {
                std::cin>>arg;
                int key = arg*2;
                RecordId rid;
                rid.page_number = arg;
                rid.slot_number = arg;
                index.insertEntry((void*)&key, rid);
            }
            else if(datatype == DOUBLE) {
                double key;
                std::cin>>key;
                RecordId rid;
                rid.page_number = arg;
                rid.slot_number = arg;
                index.insertEntry((void*)&key, rid);
            }
            else if(datatype == STRING){
                std::string str;
                std::cin>>str;
                char key[15];
                snprintf(key, 14, "%s%s", str.c_str(), padding);
                RecordId rid; rid.page_number = arg; rid.slot_number = arg; 
                index.insertEntry((void*)&key, rid);
            }
        }
        else if(cmd == 'd') {
            if(datatype == INTEGER) {
                std::cin>>arg;
                int key = arg;
                index.deleteEntry((void*)&key);
            }
            else if(datatype == DOUBLE) {
                double key;
                std::cin>>key;
                index.deleteEntry((void*)&key);
            }
            else if(datatype == STRING){
                std::string str;
                std::cin>>str;
                char key[15];
                snprintf(key, 14, "%s%s", str.c_str(), padding);
                printf("deleting %s...\n", key);
                index.deleteEntry((void*)&key);
            }
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
    }
}
