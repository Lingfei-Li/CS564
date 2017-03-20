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


//#define DEBUG

namespace badgerdb
{

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------
BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType)
{

    printf("BTreeIndex: constructor invoked\n");

    //Init buffer manager, attr type, attr offset
    this->bufMgr = bufMgrIn;
    this->attributeType = attrType;
    this->attrByteOffset = attrByteOffset;

    //Determine index filename
    std::ostringstream idxStr;
    idxStr<<relationName<<"."<<attrByteOffset;
    std::string indexName = idxStr.str();

    outIndexName = indexName;

    //Init non-leaf and leaf node occupancy
    if(attrType == INTEGER) {
        this->leafOccupancy = INTARRAYLEAFSIZE;
        this->nodeOccupancy = INTARRAYNONLEAFSIZE;

    } else if(attrType == DOUBLE) {
        this->leafOccupancy = DOUBLEARRAYLEAFSIZE;
        this->nodeOccupancy = DOUBLEARRAYNONLEAFSIZE;

    } else if(attrType == STRING) {
        this->leafOccupancy = STRINGARRAYLEAFSIZE;
        this->nodeOccupancy = STRINGARRAYNONLEAFSIZE;
    }

    printf("Node and leaf occupancy: %d, %d\n", this->nodeOccupancy, this->leafOccupancy);

    //Open or create the index file
    if(File::exists(indexName)) {
        printf("BTreeIndex: constructor: existing file read\n");
        this->file = new BlobFile(indexName, false);

        //Read info from meta info page
        const PageId metaInfoPageNo = 1;
        Page* metaInfoPage = NULL;
        this->bufMgr->readPage(this->file, metaInfoPageNo, metaInfoPage);
        struct IndexMetaInfo* indexMetaInfo = (struct IndexMetaInfo*)(metaInfoPage);
        this->rootPageNum = indexMetaInfo->rootPageNo;
        this->height = indexMetaInfo->height;

        //Release meta info page
        this->bufMgr->unPinPage(this->file, metaInfoPageNo, false);
    }
    else {
        printf("BTreeIndex: constructor: new index file created\n");
        this->file = new BlobFile(indexName, true);

        PageId metaInfoPageNo = 0;
        Page* metaInfoPage = NULL;

        //Allocate meta info page for the index page
        this->bufMgr->allocPage(this->file, metaInfoPageNo, metaInfoPage);
        assert(metaInfoPageNo == 1);

        this->headerPageNum = metaInfoPageNo;
        struct IndexMetaInfo* indexMetaInfo = (struct IndexMetaInfo*)metaInfoPage;
        strncpy(indexMetaInfo->relationName, indexName.c_str(), indexName.length());
        indexMetaInfo->attrByteOffset = attrByteOffset;
        indexMetaInfo->attrType = attrType;

        //Allocate root page for the index file
        PageId rootPageNo = 0;
        Page* rootPage = NULL;
        this->bufMgr->allocPage(this->file, rootPageNo, rootPage);
        this->rootPageNum = indexMetaInfo->rootPageNo = rootPageNo;
        this->height = indexMetaInfo->height = 0;

        //Build the root node as leaf to init the tree structure
        if(attrType == INTEGER) {
            LeafNode<int>* rootNode = (LeafNode<int>*)rootPage;
            rootNode->rightSibPageNo = 0;
            rootNode->usage = 0;

        } else if(attrType == DOUBLE){
            LeafNode<double>* rootNode = (LeafNode<double>*)rootPage;
            rootNode->rightSibPageNo = 0;
            rootNode->usage = 0;
        } else {
            LeafNode<char*>* rootNode = (LeafNode<char*>*)rootPage;
            rootNode->rightSibPageNo = 0;
            rootNode->usage = 0;
        }
        //Release meta info page
        this->bufMgr->unPinPage(this->file, metaInfoPageNo, true);
        //Release root node
        this->bufMgr->unPinPage(this->file, this->rootPageNum, true);
    }

//    printMeta();

    printf("BTreeIndex: constructor finished\n");
}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------
  /**
   * BTreeIndex Destructor. 
	 * End any initialized scan, flush index file, after unpinning any pinned pages, from the buffer manager
	 * and delete file instance thereby closing the index file.
	 * Destructor should not throw any exceptions. All exceptions should be caught in here itself. 
	 * */
BTreeIndex::~BTreeIndex()
{ }

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------
const void BTreeIndex::insertEntry(const void *key, const RecordId rid)
{
    if(this->attributeType == INTEGER) {
        this->insertEntry_template<int>(*(int*)key, rid);
    }
    else if(this->attributeType == DOUBLE) {
        this->insertEntry_template<double>(*(double*)key, rid);
    }
    else {
        char truncatedKey[11];
        snprintf(truncatedKey, 10, "%s", (char*)key);
        truncatedKey[10] = '\0';
        this->insertEntry_template<char*>(truncatedKey, rid);
    }
}



// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

const void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{
    //Set scan parameters and check scan condition
    if((lowOpParm != GT && lowOpParm != GTE) || (highOpParm != LT && highOpParm != LTE)) {
        throw new BadOpcodesException();
    }
	this->scanExecuting = true;
	this->lowOp = lowOpParm;
	this->highOp = highOpParm;

    if(this->attributeType == INTEGER) {
        this->lowValInt = *(int*)lowValParm;
        this->highValInt = *(int*)highValParm;
        if(smallerThan(this->highValInt, this->lowValInt)) {
            throw new BadScanrangeException();
        }
    }
    else if(this->attributeType == DOUBLE) {
        this->lowValDouble = *(double*)lowValParm;
        this->highValDouble = *(double*)highValParm;
        if(smallerThan(this->highValDouble, this->lowValDouble)) {
            throw new BadScanrangeException();
        }
    }
    else {
        assignKey(this->lowValString, (char*)lowValParm);
        assignKey(this->highValString, (char*)highValParm);
        if(smallerThan(this->highValString, this->lowValString)) {
            throw new BadScanrangeException();
        }
    }

    //Locate scan starting position
    if(this->attributeType == INTEGER) {
        this->startScan_helper<int>(*(int*)lowValParm, lowOpParm, *(int*)highValParm, highOpParm);
    }
    else if(this->attributeType == DOUBLE) {
        this->startScan_helper<double>(*(double*)lowValParm, lowOpParm, *(double*)highValParm, highOpParm);
    }
    else {
        this->startScan_helper<char*>((char*)lowValParm, lowOpParm, (char*)highValParm, highOpParm);
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

const void BTreeIndex::scanNext(RecordId& outRid) 
{
    if(this->attributeType == INTEGER) {
        this->scanNext_helper<int>(outRid, this->lowValInt, this->highValInt);
    }
    else if(this->attributeType == DOUBLE) {
        this->scanNext_helper<double>(outRid, this->lowValDouble, this->highValDouble);
    }
    else {
        this->scanNext_helper<char*>(outRid, this->lowValString, this->highValString);
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
const void BTreeIndex::endScan() 
{
    this->bufMgr->unPinPage(this->file, this->currentPageNum, false);
	this->scanExecuting = false;
}

// -----------------------------------------------------------------------------
// BTreeIndex::printMeta
// -----------------------------------------------------------------------------

const void BTreeIndex::printMeta() 
{
    this->bufMgr->printSelfNonNull();
    const PageId metaInfoPageNo = 1;
    Page* metaInfoPage = NULL;
    this->bufMgr->readPage(this->file, metaInfoPageNo, metaInfoPage);
    struct IndexMetaInfo* indexMetaInfo = (struct IndexMetaInfo*)(metaInfoPage);

    std::cout<<"\n========== Index Meta Info ==========\n";

    std::cout<<"RootPageNo: "<<indexMetaInfo->rootPageNo<<std::endl;
    std::cout<<"Height: "<<indexMetaInfo->height<<std::endl;
    std::cout<<"AttrByteOffset: "<<indexMetaInfo->attrByteOffset<<std::endl;
    std::cout<<"RelationName: "<<indexMetaInfo->relationName<<std::endl;
    std::cout<<"AttrType: "<<indexMetaInfo->attrType<<std::endl;

    this->bufMgr->unPinPage(this->file, metaInfoPageNo, false);

    std::cout<<"=====================================\n\n";
}


// -----------------------------------------------------------------------------
// BTreeIndex::dumpAllLevels
// -----------------------------------------------------------------------------
const void BTreeIndex::dumpAllLevels()  
{
    for(int i = 0; i <= this->height; i ++) {
        printf("Dumping Level %d\n", i);
        dumpLevel(i);
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::dumpLevel
// -----------------------------------------------------------------------------

const void BTreeIndex::dumpLevel(int dumpLevel)  
{
    if(dumpLevel == this->height) {
        if(this->attributeType == INTEGER) {
            dumpLeaf<int>();
        }
        else if(this->attributeType == DOUBLE) {
            dumpLeaf<double>();
        }
        else {
            dumpLeaf<char*>();
        }
    }
    else {
        if(this->attributeType == INTEGER) {
            dumpLevel1<int>(this->rootPageNum, 0, dumpLevel);
        }
        else if(this->attributeType == DOUBLE) {
            dumpLevel1<double>(this->rootPageNum, 0, dumpLevel);
        }
        else {
            dumpLevel1<char*>(this->rootPageNum, 0, dumpLevel);
        }
    }

}


template<class T>
const void BTreeIndex::dumpLevel1(PageId curPageNo, int curLevel, int dumpLevel) 
{
    if(curLevel > dumpLevel) {
        return;
    }
    Page* curPage = NULL;

    if(curPageNo != 0) {
        this->bufMgr->readPage(this->file, curPageNo, curPage);
        NonLeafNode<T>* curNode = (NonLeafNode<T>*)curPage;
        if(curLevel == dumpLevel) {
            std::cout<<"\t"<<curPageNo<<": ";

            if(curNode->usage > this->nodeOccupancy) {
                return;
            }

            for(int i = 0; i < curNode->usage; i ++) {
                std::cout<<"["<<curNode->pageKeyPairArray[i].pageNo<<"] "<<curNode->pageKeyPairArray[i].key<<" ";
            }
            std::cout<<"["<<curNode->pageKeyPairArray[curNode->usage].pageNo<<"]";
            std::cout<<" u: "<<curNode->usage<<"\n";
        }
        else {
            for(int i = 0; i < curNode->usage + 1; i ++) {
                dumpLevel1<T>(curNode->pageKeyPairArray[i].pageNo, curLevel + 1, dumpLevel);
            }
        }
        this->bufMgr->unPinPage(this->file, curPageNo, false);
    }
}

template<char*>
const void BTreeIndex::dumpLevel1(PageId curPageNo, int curLevel, int dumpLevel) 
{
    if(curLevel > dumpLevel) {
        return;
    }
    Page* curPage = NULL;

    if(curPageNo != 0) {
        this->bufMgr->readPage(this->file, curPageNo, curPage);
        NonLeafNode<char*>* curNode = (NonLeafNode<char*>*)curPage;
        if(curLevel == dumpLevel) {
            std::cout<<"\t"<<curPageNo<<" usage "<<curNode->usage<<": ";

            if(curNode->usage > this->nodeOccupancy) return;

            for(int i = 0; i < curNode->usage; i ++) {
                printf("[%d] %s ", curNode->pageKeyPairArray[i].pageNo, curNode->pageKeyPairArray[i].key);
            }
            std::cout<<"["<<curNode->pageKeyPairArray[curNode->usage].pageNo<<"]\n";
        }
        else {
            for(int i = 0; i < curNode->usage + 1; i ++) {
                dumpLevel1<char*>(curNode->pageKeyPairArray[i].pageNo, curLevel + 1, dumpLevel);
            }
        }
        this->bufMgr->unPinPage(this->file, curPageNo, false);
    }
}




// -----------------------------------------------------------------------------
// BTreeIndex::dumpLeaf
// -----------------------------------------------------------------------------
template<class T>
const void BTreeIndex::dumpLeaf() 
{
    PageId curPageNo = this->rootPageNum;
    Page* curPage = NULL;
    for(int i = 0; i < this->height; i ++) {
        this->bufMgr->readPage(this->file, curPageNo, curPage);
        NonLeafNode<T>* curNode = (NonLeafNode<T>*)curPage;
        PageId tmp = curPageNo;
        curPageNo = curNode->pageKeyPairArray[0].pageNo;
        this->bufMgr->unPinPage(this->file, tmp, false);
    }

    while(curPageNo != 0) {
        if(curPageNo > 10000) {
            printf("\n\n##### Warning: curPageNo %d is too large, stopping \n\n\n", curPageNo);
            return;
        }
        this->bufMgr->readPage(this->file, curPageNo, curPage);
        LeafNode<T>* curNode = (LeafNode<T>*)curPage;
        std::cout<<"\t"<<curPageNo<<": ";
        for(int i = 0; i < curNode->usage; i ++) {
            std::cout<<curNode->ridKeyPairArray[i].key<<" ";
        }
        std::cout<<" u:"<<curNode->usage<<"";
        std::cout<<"\t"<<"-> "<<curNode->rightSibPageNo<<"\n";
        PageId tmp = curPageNo;
        curPageNo = curNode->rightSibPageNo;
        this->bufMgr->unPinPage(this->file, tmp, false);
    }
}

template<char*>
const void BTreeIndex::dumpLeaf() 
{
    PageId curPageNo = this->rootPageNum;
    Page* curPage = NULL;
    for(int i = 0; i < this->height; i ++) {
        this->bufMgr->readPage(this->file, curPageNo, curPage);
        NonLeafNode<char*>* curNode = (NonLeafNode<char*>*)curPage;
        PageId tmp = curPageNo;
        curPageNo = curNode->pageKeyPairArray[0].pageNo;
        this->bufMgr->unPinPage(this->file, tmp, false);
    }

    while(curPageNo != 0) {
        this->bufMgr->readPage(this->file, curPageNo, curPage);
        LeafNode<char*>* curNode = (LeafNode<char*>*)curPage;
        std::cout<<curPageNo<<"->"<<curNode->rightSibPageNo<<": ";
        for(int i = 0; i < curNode->usage; i ++) {
            printf("%s ", curNode->ridKeyPairArray[i].key);
        }
        std::cout<<" u:"<<curNode->usage<<"";
        std::cout<<"\n";
        PageId tmp = curPageNo;
        curPageNo = curNode->rightSibPageNo;
        this->bufMgr->unPinPage(this->file, tmp, false);
    }
}




const bool BTreeIndex::deleteEntry(const void *key)
{

    std::stack<PageId> pinnedPage;
    std::vector<PageId> disposePageNo;
    bool result = true;

    try {
        if(this->attributeType == INTEGER) {
            this->deleteEntry_helper<int>(*(int*)key, this->rootPageNum, NULL, -2, 0, disposePageNo, pinnedPage);
        }
        else if(this->attributeType == DOUBLE) {
            this->deleteEntry_helper<double>(*(double*)key, this->rootPageNum, NULL, -2, 0, disposePageNo, pinnedPage);
        }
        else {
            char truncatedKey[11];
            snprintf(truncatedKey, 10, "%s", (char*)key);
            truncatedKey[10] = '\0';
            this->deleteEntry_helper<char*>(truncatedKey, this->rootPageNum, NULL, -2, 0, disposePageNo, pinnedPage);
        }
    }
    catch(DeletionKeyNotFoundException e) {
        while(pinnedPage.empty() == false) {
            PageId pageNo = pinnedPage.top();
            this->bufMgr->unPinPage(this->file, pageNo, false);
            pinnedPage.pop();
        }
        printf("Deletion key not found\n");
        result = false;
    }

//    this->bufMgr->flushFile(this->file);
    for(size_t i = 0; i < disposePageNo.size(); i ++) {
        try{
            this->bufMgr->disposePage(this->file, disposePageNo[i]);
        }catch(InvalidPageException e) {

        }
    }

    return result;

}


const bool BTreeIndex::validate(bool showInfo) {
    if(showInfo) std::cout<<"\n========= Validation Result ==========\n";

    std::stack<PageId> pinnedPage;
    try {
        if(this->attributeType == INTEGER) {
            this->validate_helper<int>(this->rootPageNum, 0, pinnedPage);
        }
        else if(this->attributeType == DOUBLE) {
            this->validate_helper<double>(this->rootPageNum, 0, pinnedPage);
        }
        else {
            this->validate_helper<char*>(this->rootPageNum, 0, pinnedPage);
        }
        if(this->bufMgr->pinnedCnt() != 0) {
            printf("Buffer manager is not clean:\n");
            this->bufMgr->printSelfPinned();
            throw ValidationFailedException();
        }
    }
    catch(ValidationFailedException e) {
        while(pinnedPage.empty() == false) {
            PageId pageNo = pinnedPage.top();
            this->bufMgr->unPinPage(this->file, pageNo, false);
            pinnedPage.pop();
        }
        std::cout<<"======================================\n";
        printf("Validation failed\n");
        std::cout<<"======================================\n\n";
        return false;
    }

    if(showInfo) {
        printf("Validation passed\n");
        std::cout<<"======================================\n\n";
    }
    return true;
}


}
