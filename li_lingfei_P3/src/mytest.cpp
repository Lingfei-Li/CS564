
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

struct RECORD {
    int i;
    double d;
    char s[64];
};

struct TestFailException {};

const char* padding = "                 ";

void createRelation(const std::string& relationName, Datatype datatype, std::vector<int>& input);

void testDeletion(std::vector<int>& input, BTreeIndex& index, BufMgr* bufMgr);

void testScan(std::vector<int>& input, BTreeIndex& index, BufMgr* bufMgr, int low, int high);

void insertEntries(std::vector<int>& input, BTreeIndex& index, BufMgr* bufMgr);

void testInsertion(BTreeIndex& index, BufMgr& bufMgr);

void manualTest(std::vector<int>& input, BTreeIndex& index, BufMgr* bufMgr);

Datatype datatype;

int main() {
    BufMgr * bufMgr = new BufMgr(100);
    std::string indexName;
    std::string relationName = "rel_";

    std::ifstream myfile;
    myfile.open ("input");

    std::string type;
    myfile>>type;
    if(type == "i" || type == "int") {
        datatype = INTEGER;
        relationName += "int_";
    }
    else if(type == "d" || type=="double"){
        datatype = DOUBLE;
        relationName += "dbl_";
    }
    else if(type == "s" || type=="str" || type=="string"){
        datatype = STRING;
        relationName += "str_";
    }

    std::string test;
    int n;
    myfile>>test>>n;
    std::vector<int> input;
    for(int i = 0; i < n; i ++) {
        int key = i*2;
        input.push_back(key);
    }

    relationName += test+"_";
    relationName += std::to_string(n);


    if(File::exists(relationName) == false) {
        createRelation(relationName, datatype, input);
    }

    int attrOffset = 0;
    if(datatype == INTEGER) {
        attrOffset = offsetof(RECORD, i);
    }
    else if(datatype == DOUBLE) {
        attrOffset = offsetof(RECORD, d);
    }
    else if(datatype == STRING) {
        attrOffset = offsetof(RECORD, s);
    }

    if(test == "d" || test == "del" || test=="deletion") {
        int maxEpoch = 10;
        for(int epoch = 0; epoch < maxEpoch; epoch ++) {
            printf("Epoch %d\n", epoch);
            if(File::exists(indexName)) {
                File::remove(indexName);
            }
            BTreeIndex index(relationName, indexName, bufMgr, attrOffset, datatype);
            try{
                testDeletion(input, index, bufMgr);
                printf("Deletion test passed\n");
            } catch(TestFailException e) {
                index.dumpAllLevels();
                printf("Deletion test failed\n");
                break;
            }
        }
    }
    else if(test == "s" || test == "scan") {
        BTreeIndex index(relationName, indexName, bufMgr, attrOffset, datatype);
        int low, high;
        myfile>>low>>high;
        testScan(input, index, bufMgr, low, high);
    }
    else if(test == "m" || test == "manual") {
        BTreeIndex index(relationName, indexName, bufMgr, attrOffset, datatype);
        manualTest(input, index, bufMgr);
    }
    else {
    
    }

    myfile.close();

    delete bufMgr;

    return 0;
}


void createRelation(const std::string& relationName, Datatype datatype, std::vector<int>& input) {
    // Create a new database file.
    PageFile new_file = PageFile::create(relationName);

    RECORD record1;
    // Allocate some pages and put data on them.
    for(int i = 0; i < (int)input.size(); i++) {
        PageId new_page_number;
        Page new_page = new_file.allocatePage(new_page_number);

        int key = 2 * i;
        snprintf(record1.s, 14, "%d%s", key, padding);
        record1.i = key;
        record1.d = (double)key;
        std::string new_data(reinterpret_cast<char*>(&record1), sizeof(record1));

        new_page.insertRecord(new_data);
        new_file.writePage(new_page_number, new_page);
    }
}


void testDeletion(std::vector<int>& input, BTreeIndex& index, BufMgr* bufMgr) {
    srand(time(0));
    std::vector<int> deletionIndex;
    for(size_t i = 0; i < input.size(); i ++) {
        deletionIndex.push_back(i);
    }
    std::random_shuffle(deletionIndex.begin(), deletionIndex.end());

    for(size_t i = 0; i < deletionIndex.size(); i ++) {
        if(datatype == INTEGER) {
            int key = input[deletionIndex[i]];
            index.dprintf("deleting %d...\n", key);
            if(index.deleteEntry((void*)&key) == false || index.validate(false) == false) {
                throw TestFailException();
            }
        }
        else if(datatype == DOUBLE) {
            double key = (double)input[deletionIndex[i]];
            index.dprintf("deleting %lf...\n", key);
            if(index.deleteEntry((void*)&key) == false || index.validate(false) == false) {
                throw TestFailException();
            }
        }
        else if(datatype == STRING){
            char key[15];
            snprintf(key, 14, "%d%s", input[deletionIndex[i]], padding);
            index.dprintf("deleting %s...\n", key);
            if(index.deleteEntry((void*)&key) == false || index.validate(false) == false) {
                throw TestFailException();
            }
        }
    }

}

void testScan(std::vector<int>& input, BTreeIndex& index, BufMgr* bufMgr, int low, int high) {

    try {
        index.endScan();
        printf("Error: endScan should throw ScanNotInitializedException\n");
        return;
    } catch(ScanNotInitializedException ex){ }

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
