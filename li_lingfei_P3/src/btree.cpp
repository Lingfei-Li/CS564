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
            rootNode->leftSibPageNo = 0;
            rootNode->usage = 0;

        } else if(attrType == DOUBLE){
            LeafNode<double>* rootNode = (LeafNode<double>*)rootPage;
            rootNode->rightSibPageNo = 0;
            rootNode->leftSibPageNo = 0;
            rootNode->usage = 0;
        } else {
            LeafNode<char*>* rootNode = (LeafNode<char*>*)rootPage;
            rootNode->rightSibPageNo = 0;
            rootNode->leftSibPageNo = 0;
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
        this->insertEntry_template<char*>((char*)key, rid);
    }
}

template<class T>
const void BTreeIndex::createNewRoot(PageKeyPair<T>& ret) 
{
    if(ret.pageNo != 0) {
        PageId rootPageNo;
        Page* rootPage;
        this->bufMgr->allocPage(this->file, rootPageNo, rootPage);
        NonLeafNode<T>* rootNode = (NonLeafNode<T>*)rootPage;


        rootNode->pageKeyPairArray[0] = PageKeyPair<T>(this->rootPageNum, 0);
        rootNode->usage = 0;

        this->rootPageNum = rootPageNo;

        this->insertEntryInNonLeaf<T>(ret.key, ret.pageNo, rootNode);

        this->bufMgr->unPinPage(this->file, rootPageNo, true);

        this->height ++;
    }
}


template<class T>
const void BTreeIndex::insertEntry_template(T key, const RecordId rid) 
{
    PageKeyPair<T> ret = this->insertEntry_helper(key, rid, this->rootPageNum, 0);
    this->createNewRoot<T>(ret);
}



// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

const void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{


    if((lowOpParm != GT && lowOpParm != GTE) || (highOpParm != LT && highOpParm != LTE)) {
        throw new BadOpcodesException();
    }

	this->scanExecuting = true;

	this->lowOp = lowOpParm;
	this->highOp = highOpParm;

    this->scannedPageNum = std::vector<PageId>();

    if(this->attributeType == INTEGER) {

        /*

        int lowKeyVal = *(int*)lowValParm;
        int highKeyVal = *(int*)highValParm;
        if(lowKeyVal > highKeyVal) {
            throw new BadScanrangeException();
        }

        this->lowValInt = lowKeyVal;
        this->highValInt = highKeyVal;

        int level = 0;
        PageId curPageNo = this->rootPageNum;
        Page* curPage = NULL;
        while(level++ < this->height) {
            this->bufMgr->readPage(this->file, curPageNo, curPage);

            struct NonLeafNodeInt* curNode = (struct NonLeafNodeInt*)curPage;

            //Note: we must scan from rhs until some key less than the lowVal. 
            int i = curNode->usage - 1;
            for(i = curNode->usage - 1; i >= 0; i -- ){
                if(curNode->keyArray[i] <= lowKeyVal) {
                    break;
                }
            }


            PageId tmpPageNo = curNode->pageNoArray[i + 1]; //Use the page ptr to the found key's right

            printf("searching internal node... next page: %d\n", tmpPageNo);

            this->bufMgr->unPinPage(this->file, curPageNo, false);

            curPageNo = tmpPageNo;
        }

        printf("leaf page no: %d\n", curPageNo);

    
        PageId leafPageNo = curPageNo;
        Page* leafPage = NULL;
        this->bufMgr->readPage(this->file, leafPageNo, leafPage);

        struct LeafNodeInt* leafNode = (struct LeafNodeInt*)leafPage;

        for(int i = 0; i < leafNode->usage; i ++ ){
            if((lowOpParm == GT && lowKeyVal < leafNode->keyArray[i]) || (lowOpParm == GTE && lowKeyVal <= leafNode->keyArray[i])) {
                this->nextEntry = i;
                printf("starting scan at %d %d %d\n", curPageNo, i, leafNode->keyArray[i]);
                break;
            }
        }

        this->currentPageNum = leafPageNo;
        this->scannedPageNum.push_back(leafPageNo);
        this->currentPageData = leafPage;
        */

    }
    else if(this->attributeType == DOUBLE){
        /*
        this->nextEntry;
        this->currentPageNum;
        this->currentPageData;
        this->lowValInt;
        this->lowValDouble;
        this->lowValString;
        this->highValInt;
        this->highValDouble;
        this->highValString;
        */
    
    }
    else {
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

const void BTreeIndex::scanNext(RecordId& outRid) 
{
    /*
    if(this->attributeType == INTEGER) {
        struct LeafNodeInt* curNode = (struct LeafNodeInt*)this->currentPageData;
        int nextKeyVal = curNode->keyArray[this->nextEntry];

        if(this->currentPageNum == 0 || this->nextEntry == curNode->usage) {
            throw IndexScanCompletedException();
        }
    

        if((this->highOp == LT && this->highValInt <= nextKeyVal) || (this->highOp == LTE && this->highValInt < nextKeyVal)) {
            throw IndexScanCompletedException();
        }

        outRid = curNode->ridArray[this->nextEntry ++ ];

        if(this->nextEntry == curNode->usage) {
            this->currentPageNum = curNode->rightSibPageNo;
            if(this->currentPageNum != 0) {
                this->bufMgr->readPage(this->file, this->currentPageNum, this->currentPageData);
                this->nextEntry = 0;
                this->scannedPageNum.push_back(this->currentPageNum);
            }
        }
    }
    else {
    
    }

    */
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
const void BTreeIndex::endScan() 
{
    for(size_t i = 0; i < this->scannedPageNum.size(); i ++) {
        this->bufMgr->unPinPage(this->file, this->scannedPageNum[i], false);
    }

	this->scanExecuting = true;
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
            dumpLeaf<char[10]>();
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
            dumpLevel1<char[10]>(this->rootPageNum, 0, dumpLevel);
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
            std::cout<<curPageNo<<" usage "<<curNode->usage<<": ";
            for(int i = 0; i < curNode->usage; i ++) {
                std::cout<<"["<<curNode->pageKeyPairArray[i].pageNo<<"] "<<curNode->pageKeyPairArray[i].key<<" ";
            }
            std::cout<<"["<<curNode->pageKeyPairArray[curNode->usage].pageNo<<"]\n";
        }
        else {
            for(int i = 0; i < curNode->usage + 1; i ++) {
                dumpLevel1<T>(curNode->pageKeyPairArray[i].pageNo, curLevel + 1, dumpLevel);
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
        this->bufMgr->readPage(this->file, curPageNo, curPage);
        LeafNode<T>* curNode = (LeafNode<T>*)curPage;
        std::cout<<curNode->leftSibPageNo<<"<-"<<curPageNo<<": ";
        for(int i = 0; i < curNode->usage; i ++) {
            std::cout<<curNode->ridKeyPairArray[i].key<<" ";
        }
        std::cout<<" u:"<<curNode->usage<<"";
        std::cout<<"\n";
        PageId tmp = curPageNo;
        curPageNo = curNode->rightSibPageNo;
        this->bufMgr->unPinPage(this->file, tmp, false);
    }
}




const void BTreeIndex::deleteEntry(const void *key)
{

    std::vector<PageId> disposePageNo = std::vector<PageId>();

    if(this->attributeType == INTEGER) {
        this->deleteEntry_helper<int>(*(int*)key, this->rootPageNum, NULL, -1, 0, disposePageNo);
    }
    else if(this->attributeType == DOUBLE) {
        this->deleteEntry_helper<double>(*(double*)key, this->rootPageNum, NULL, -1, 0, disposePageNo);
    }
    else {
        this->deleteEntry_helper<char*>((char*)key, this->rootPageNum, NULL, -1, 0, disposePageNo);
    }

//    this->bufMgr->flushFile(this->file);
    for(size_t i = 0; i < disposePageNo.size(); i ++) {
        try{
            this->bufMgr->disposePage(this->file, disposePageNo[i]);
        }catch(InvalidPageException e) {

        }
    }

}


const bool BTreeIndex::validate() {
    std::cout<<"\n========= Validation Result ==========\n";
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
        printf("Validation failed\n");
        std::cout<<"======================================\n\n";
        return false;
    }

    printf("Validation passed\n");
    std::cout<<"======================================\n\n";
    return true;
}


}
