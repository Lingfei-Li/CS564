/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/invalid_page_exception.h"
#include "exceptions/end_of_file_exception.h"

#include <string>
#include <sstream>
#include <vector>


namespace badgerdb
{



template<class T>
const void BTreeIndex::startScan_helper(T lowKeyVal,
				   const Operator lowOpParm,
				   T highKeyVal,
				   const Operator highOpParm)
{
    int level = 0;
    PageId curPageNo = this->rootPageNum;
    Page* curPage = NULL;
    while(level++ < this->height) {
        this->bufMgr->readPage(this->file, curPageNo, curPage);

        NonLeafNode<T>* curNode = (NonLeafNode<T>*)curPage;

        //Note: we must scan from rhs until some key less than the or equal to lowVal. 
        int i = curNode->usage - 1;
        for(i = curNode->usage - 1; i >= 0; i -- ){
            if(smallerThanOrEquals(curNode->pageKeyPairArray[i].key, lowKeyVal)) {
                break;
            }
        }

        PageId tmpPageNo = curNode->pageKeyPairArray[i + 1].pageNo; //Use the page ptr to the found key's right

        printf("searching internal node... next page: %d\n", tmpPageNo);

        this->bufMgr->unPinPage(this->file, curPageNo, false);

        curPageNo = tmpPageNo;
    }

    printf("leaf page no: %d\n", curPageNo);

    PageId leafPageNo = curPageNo;
    Page* leafPage = NULL;
    this->bufMgr->readPage(this->file, leafPageNo, leafPage);

    LeafNode<T>* leafNode = (LeafNode<T>*)leafPage;

    int i;
    for(i = 0; i < leafNode->usage; i ++ ){
        if((lowOpParm == GT && smallerThan(lowKeyVal, leafNode->ridKeyPairArray[i].key)) || 
           (lowOpParm == GTE && smallerThanOrEquals(lowKeyVal, leafNode->ridKeyPairArray[i].key))) {
            this->nextEntry = i;
            printf("starting scan at page %d index %d keyVal ", curPageNo, i);
            std::cout<<leafNode->ridKeyPairArray[i].key<<std::endl;
            break;
        }
    }

    if(i == leafNode->usage) {
        //No matching entry in this leaf. Move to the sibling
        PageId tmpLeafPageNo = leafNode->rightSibPageNo;
        this->bufMgr->unPinPage(this->file, leafPageNo, false);
        leafPageNo = tmpLeafPageNo;
        if(leafPageNo != 0) {
            this->bufMgr->readPage(this->file, leafPageNo, leafPage);
            this->nextEntry = 0;
            printf("starting scan at page %d index %d keyVal ", curPageNo, i);
            std::cout<<leafNode->ridKeyPairArray[i].key<<std::endl;
        }
    }

    this->currentPageNum = leafPageNo;
    if(leafPageNo != 0) {
        this->currentPageData = leafPage;
    }
}


template<class T>
const void BTreeIndex::scanNext_helper(RecordId& outRid, T lowVal, T highVal) 
{
    LeafNode<T>* curNode = (LeafNode<T>*)this->currentPageData;

    //Note that char* also works here since we don't modify anyting
    //For insertion and deletion, must use assignKey() function
    T nextKeyVal = curNode->ridKeyPairArray[this->nextEntry].key;

    if(this->currentPageNum == 0 || this->nextEntry == curNode->usage) {
        throw IndexScanCompletedException();
    }

    if((this->highOp == LT && smallerThanOrEquals(highVal, nextKeyVal)) || 
       (this->highOp == LTE && smallerThan(highVal, nextKeyVal))) {
        throw IndexScanCompletedException();
    }

    outRid = curNode->ridKeyPairArray[this->nextEntry].rid;

    this->nextEntry ++;

    //Reached the end of current leaf node
    if(this->nextEntry == curNode->usage) {
        //Release current page
        PageId tmpCurrentPageNum = this->currentPageNum;

        this->currentPageNum = curNode->rightSibPageNo;

        this->bufMgr->unPinPage(this->file, tmpCurrentPageNum, false);

        if(this->currentPageNum != 0) {
            this->bufMgr->readPage(this->file, this->currentPageNum, this->currentPageData);
            this->nextEntry = 0;
        }
    }
}


}
