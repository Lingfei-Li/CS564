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
#include <cstdarg>


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

    dprintf("BTreeIndex: constructor invoked\n");

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
        dprintf("sizeof pageno key pair: %d\n", sizeof(PageKeyPair<char*>));
        dprintf("sizeof internal node: %d\n", sizeof(NonLeafNode<char*>));

        dprintf("sizeof rid key pair: %d\n", sizeof(RIDKeyPair<char*>));
        dprintf("sizeof leaf node: %d\n", sizeof(LeafNode<char*>));
    }

    dprintf("Node and leaf occupancy: %d, %d\n", this->nodeOccupancy, this->leafOccupancy);

    dprintf("offset of sibling %d\n", offsetof(LeafNode<char*>, rightSibPageNo));

    //Open or create the index file
    if(File::exists(indexName)) {
        dprintf("BTreeIndex: constructor: existing file read\n");
        this->file = new BlobFile(indexName, false);

        //Read info from meta info page
        this->headerPageNum = 1;
        Page* metaInfoPage = NULL;
        this->bufMgr->readPage(this->file, this->headerPageNum, metaInfoPage);
        IndexMetaInfo* indexMetaInfo = (IndexMetaInfo*)(metaInfoPage);
        this->rootPageNum = indexMetaInfo->rootPageNo;
        this->height = indexMetaInfo->height;

        dprintf("root: %d height: %d\n", this->rootPageNum, this->height);

        //Release meta info page
        this->bufMgr->unPinPage(this->file, this->headerPageNum, false);
    }
    else {
        dprintf("BTreeIndex: constructor: new index file created\n");
        this->file = new BlobFile(indexName, true);

        Page* metaInfoPage = NULL;

        //Allocate meta info page for the index page
        this->bufMgr->allocPage(this->file, this->headerPageNum, metaInfoPage);

        IndexMetaInfo* indexMetaInfo = (IndexMetaInfo*)metaInfoPage;
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
        this->bufMgr->unPinPage(this->file, this->headerPageNum, true);
        //Release root node
        this->bufMgr->unPinPage(this->file, this->rootPageNum, true);

        this->createIndexFromRelation(relationName);

    }
//    printMeta();

    this->dumpAllLevels();
    dprintf("BTreeIndex: constructor finished\n");
}


//Reads the relation and add all <key, rid> pairs to the index
const void BTreeIndex::createIndexFromRelation(const std::string& relationName) {
    {
		FileScan fscan(relationName, this->bufMgr);
		try {
			RecordId scanRid;
			while(1) {
				fscan.scanNext(scanRid);
				//Assuming RECORD.i is our key, lets extract the key, which we know is INTEGER and whose byte offset is also know inside the record. 
				std::string recordStr = fscan.getRecord();
				const char *record = recordStr.c_str();

                if(this->attributeType == INTEGER) {
                    int key = *((int*)(record + this->attrByteOffset));
                    this->insertEntry((void*)&key, scanRid);
#ifdef DEBUG
                    std::cout << "Extracted Key: " << key << std::endl;
#endif
                } else if(this->attributeType == DOUBLE){
                    double key = *((double*)(record + this->attrByteOffset));
                    this->insertEntry((void*)&key, scanRid);
#ifdef DEBUG
                    std::cout << "Extracted Key: " << key << std::endl;
#endif
                } else {
                    char key[15];
                    assignKey(key, (char*)(record + this->attrByteOffset));
                    this->insertEntry((void*)&key, scanRid);
#ifdef DEBUG
                    std::cout << "Extracted Key: " << key << std::endl;
#endif
                }
			}
		} catch(EndOfFileException e) {
#ifdef DEBUG
			std::cout << "Read all records" << std::endl;
#endif
		}
    }
}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------
BTreeIndex::~BTreeIndex()
{ 
    this->scanExecuting = false;

    //Save meta info to header page
    Page* metaInfoPage = NULL;
    this->bufMgr->readPage(this->file, this->headerPageNum, metaInfoPage);
    IndexMetaInfo* indexMetaInfo = (IndexMetaInfo*)(metaInfoPage);
    indexMetaInfo->rootPageNo = this->rootPageNum;
    indexMetaInfo->height = this->height;
    this->bufMgr->unPinPage(this->file, this->headerPageNum, true);

    this->bufMgr->flushFile(this->file);

    delete this->file;
}

// -----------------------------------------------------------------------------
// Insertion functions
// -----------------------------------------------------------------------------
const void BTreeIndex::insertEntry(const void *key, const RecordId rid)
{
    if(this->attributeType == INTEGER) {
        PageKeyPair<int> ret = this->insertEntry_helper(*(int*)key, rid, this->rootPageNum, 0);
        this->createNewRoot<int>(ret);
    }
    else if(this->attributeType == DOUBLE) {
        PageKeyPair<double> ret = this->insertEntry_helper(*(double*)key, rid, this->rootPageNum, 0);
        this->createNewRoot<double>(ret);
    }
    else {
        char truncatedKey[STRINGSIZE+1];
        assignKey(truncatedKey, (char*)key);
        PageKeyPair<char*> ret = this->insertEntry_helper((char*)truncatedKey, rid, this->rootPageNum, 0);
        this->createNewRoot<char*>(ret);
    }
    this->dumpAllLevels();
}

template<class T>
const PageKeyPair<T> BTreeIndex::insertEntry_helper(T key, const RecordId rid, PageId curPageNo, int level) {
    Page* curPage = NULL;
    this->bufMgr->readPage(this->file, curPageNo, curPage);

    PageKeyPair<T> ret;

    if(level == this->height){
        //Base case: Reached leaf
        LeafNode<T>* node = (LeafNode<T>*)curPage;

        insertEntryInLeaf<T>(key, rid, node);

        //Split
        if(node->usage == this->leafOccupancy) {
            dprintf("splitting...\n");
            this->dumpAllLevels();
            PageId newPageNo;
            Page* newPage;
            this->bufMgr->allocPage(this->file, newPageNo, newPage);
            LeafNode<T>* newNode = (LeafNode<T>*)newPage;
            dprintf("new leaf: %d\n", newPageNo);

            //redistribute with the full leaf node
            int cnt = 0;
            for(int i = this->leafOccupancy/2; i < this->leafOccupancy; i ++) {
                //newNode is rhs, node is lhs
                newNode->ridKeyPairArray[cnt].rid = node->ridKeyPairArray[i].rid;
                assignKey(newNode->ridKeyPairArray[cnt].key, node->ridKeyPairArray[i].key);
                cnt ++;
            }

            //set usage
            newNode->usage = cnt;
            node->usage = this->leafOccupancy - cnt;

            //set sib pointers. note: order is important
            newNode->rightSibPageNo = node->rightSibPageNo;
            node->rightSibPageNo = newPageNo;

            //copy up
            ret.set(newPageNo, newNode->ridKeyPairArray[0].key);

            //Jobs with the new node is done. Release the new node
            this->bufMgr->unPinPage(this->file, newPageNo, true);
        }
    }
    else {
        //Normal case: internal node
        NonLeafNode<T>* node = (NonLeafNode<T>*)curPage;

        //Find the position
        int i;
        for(i = 0; i < node->usage; i ++ ){
            if(smallerThan(key, node->pageKeyPairArray[i].key)) {
                break;
            }
        }

        //Recursive call to insert the entry in child
        PageId childPageNo = node->pageKeyPairArray[i].pageNo;
        PageKeyPair<T> pushUp = this->insertEntry_helper(key, rid, childPageNo, level + 1);

        //Insert the copy-up entry
        if(pushUp.pageNo != 0) {
            insertEntryInNonLeaf(pushUp.key, pushUp.pageNo, node);

            //Split
            if(node->usage == this->nodeOccupancy) {
                PageId newPageNo;
                Page* newPage;
                this->bufMgr->allocPage(this->file, newPageNo, newPage);
                NonLeafNode<T>* newNode = (NonLeafNode<T>*)newPage;

                //push up
                ret.set(newPageNo, node->pageKeyPairArray[this->nodeOccupancy/2].key);
                                                    
                //redistribute
                int cnt = 0;
                for(int i = this->nodeOccupancy/2+1; i < this->nodeOccupancy; i ++) {
                    newNode->pageKeyPairArray[cnt].pageNo = node->pageKeyPairArray[i].pageNo;
                    assignKey(newNode->pageKeyPairArray[cnt].key, node->pageKeyPairArray[i].key);
                    cnt ++;
                }
                newNode->pageKeyPairArray[cnt].pageNo = node->pageKeyPairArray[this->nodeOccupancy].pageNo;
                assignKey(newNode->pageKeyPairArray[cnt].key, node->pageKeyPairArray[this->nodeOccupancy].key);

                //set usage
                newNode->usage = cnt;
                node->usage = this->nodeOccupancy - cnt - 1;

                //Jobs with the new node is done. Release the new node
                this->bufMgr->unPinPage(this->file, newPageNo, true);
            }
        }
    }

    this->bufMgr->unPinPage(this->file, curPageNo, true);
    return ret;
}

template<class T>
const void BTreeIndex::createNewRoot(PageKeyPair<T>& ret) 
{
    if(ret.pageNo != 0) {
        PageId rootPageNo;
        Page* rootPage;
        this->bufMgr->allocPage(this->file, rootPageNo, rootPage);
        NonLeafNode<T>* rootNode = (NonLeafNode<T>*)rootPage;

        dprintf("new root page no: %d\n", rootPageNo);
        rootNode->pageKeyPairArray[0].pageNo = this->rootPageNum;
        rootNode->usage = 0;

        this->rootPageNum = rootPageNo;

        this->insertEntryInNonLeaf<T>(ret.key, ret.pageNo, rootNode);

        this->bufMgr->unPinPage(this->file, rootPageNo, true);

        this->height ++;
    }
}


template<class T>
const void BTreeIndex::insertEntryInLeaf(T key, const RecordId rid, LeafNode<T>* node) {
#ifdef DEBUG
    std::cout<<"inserting leaf key: "<<key<<std::endl;
#endif
    int i = 0;
    for(i = 0; i < node->usage; i ++ ){
        if(smallerThan(key, node->ridKeyPairArray[i].key)) {
            break;
        }
    }

    //Shift all elements after this position
    for(int j = node->usage; j > i; j -- ){
        node->ridKeyPairArray[j].rid = node->ridKeyPairArray[j-1].rid;
        assignKey(node->ridKeyPairArray[j].key, node->ridKeyPairArray[j-1].key);
    }

    node->ridKeyPairArray[i].rid = rid;
    assignKey(node->ridKeyPairArray[i].key, key);

    dprintf("right sib: %d\n", node->rightSibPageNo);

    node->usage ++;
}


template<class T>
const void BTreeIndex::insertEntryInNonLeaf(T key, const PageId pageNo, NonLeafNode<T>* node) {
#ifdef DEBUG
    std::cout<<"inserting internal key: "<<key<<" pageNo "<<pageNo<<std::endl;
#endif
    int i = 0;
    for(i = 0; i < node->usage; i ++ ){
        if(smallerThan(key, node->pageKeyPairArray[i].key)) {
            break;
        }
    }

    //Shift all elements after this position
    for(int j = node->usage; j > i; j -- ){
        node->pageKeyPairArray[j+1].pageNo = node->pageKeyPairArray[j+1-1].pageNo;
        assignKey(node->pageKeyPairArray[j].key, node->pageKeyPairArray[j-1].key);
    }
    node->pageKeyPairArray[i+1].pageNo = pageNo;
    assignKey(node->pageKeyPairArray[i].key, key);

    node->usage ++;
}


// -----------------------------------------------------------------------------
// Scan functions
// -----------------------------------------------------------------------------
const void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{
    //Set scan parameters and check scan condition
    if((lowOpParm != GT && lowOpParm != GTE) || (highOpParm != LT && highOpParm != LTE)) {
        throw BadOpcodesException();
    }
	this->scanExecuting = true;
	this->lowOp = lowOpParm;
	this->highOp = highOpParm;

    if(this->attributeType == INTEGER) {
        this->lowValInt = *(int*)lowValParm;
        this->highValInt = *(int*)highValParm;
        if(smallerThan(this->highValInt, this->lowValInt)) {
            throw BadScanrangeException();
        }
    }
    else if(this->attributeType == DOUBLE) {
        this->lowValDouble = *(double*)lowValParm;
        this->highValDouble = *(double*)highValParm;
        if(smallerThan(this->highValDouble, this->lowValDouble)) {
            throw BadScanrangeException();
        }
    }
    else {
        assignKey(this->lowValString, (char*)lowValParm);
        assignKey(this->highValString, (char*)highValParm);
        if(smallerThan(this->highValString, this->lowValString)) {
            throw BadScanrangeException();
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
            if(smallerThanOrEquals<T>(curNode->pageKeyPairArray[i].key, lowKeyVal)) {
                break;
            }
        }

        PageId tmpPageNo = curNode->pageKeyPairArray[i + 1].pageNo; //Use the page ptr to the found key's right

        dprintf("searching internal node... next page: %d\n", tmpPageNo);

        this->bufMgr->unPinPage(this->file, curPageNo, false);

        curPageNo = tmpPageNo;
    }

    dprintf("leaf page no: %d\n", curPageNo);

    PageId leafPageNo = curPageNo;
    Page* leafPage = NULL;
    this->bufMgr->readPage(this->file, leafPageNo, leafPage);

    LeafNode<T>* leafNode = (LeafNode<T>*)leafPage;

    int i;
    for(i = 0; i < leafNode->usage; i ++ ){
        if((lowOpParm == GT && smallerThan(lowKeyVal, leafNode->ridKeyPairArray[i].key)) || 
           (lowOpParm == GTE && smallerThanOrEquals<T>(lowKeyVal, leafNode->ridKeyPairArray[i].key))) {
            this->nextEntry = i;
            dprintf("starting scan at page %d index %d keyVal ", curPageNo, i);
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
            dprintf("starting scan at page %d index %d keyVal ", curPageNo, i);
            std::cout<<leafNode->ridKeyPairArray[i].key<<std::endl;
        }
    }

    this->currentPageNum = leafPageNo;
    if(leafPageNo != 0) {
        this->currentPageData = leafPage;
    }
}

const void BTreeIndex::scanNext(RecordId& outRid) 
{
    if(this->scanExecuting == false) {
        throw ScanNotInitializedException();
    }
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

template<class T>
const void BTreeIndex::scanNext_helper(RecordId& outRid, T lowVal, T highVal) 
{
    LeafNode<T>* curNode = (LeafNode<T>*)this->currentPageData;

    //Note that char* also works here since we don't modify anyting
    //For insertion and deletion, must use assignKey() function instead
    T nextKeyVal = curNode->ridKeyPairArray[this->nextEntry].key;

    if(this->currentPageNum == 0 || this->nextEntry == curNode->usage) {
        throw IndexScanCompletedException();
    }

    if((this->highOp == LT && !smallerThan(nextKeyVal, highVal)) || 
       (this->highOp == LTE && !smallerThanOrEquals<T>(nextKeyVal, highVal))) {
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

const void BTreeIndex::endScan() 
{
    if(this->scanExecuting == false) {
        throw ScanNotInitializedException();
    }
    this->bufMgr->unPinPage(this->file, this->currentPageNum, false);
	this->scanExecuting = false;
}


// -----------------------------------------------------------------------------
// Deletion functions
// -----------------------------------------------------------------------------
const bool BTreeIndex::deleteEntry(const void *key)
{
    std::set<PageId> pinnedPage;
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
            assignKey(truncatedKey, (char*)key);
            this->deleteEntry_helper<char*>(truncatedKey, this->rootPageNum, NULL, -2, 0, disposePageNo, pinnedPage);
        }
    }
    catch(DeletionKeyNotFoundException e) {
        std::set<PageId>::iterator it;
        for (it = pinnedPage.begin(); it != pinnedPage.end(); ++it) {
            PageId pageNo = *it;
            this->bufMgr->unPinPage(this->file, pageNo, false);
        }
        pinnedPage.clear();
        dprintf("Deletion key not found\n");
        result = false;
    }

    for(size_t i = 0; i < disposePageNo.size(); i ++) {
        try{
            this->bufMgr->disposePage(this->file, disposePageNo[i]);
        }catch(InvalidPageException e) {
            //The provided BlobFile class doesn't support dispose operation.
        }
    }

    return result;
}


template<class T>
const void BTreeIndex::deleteEntry_helper(T key, PageId curPageNo, NonLeafNode<T>* parentNode, 
        int keyIndexAtParent, int level, std::vector<PageId>& disposePageNo, std::set<PageId>& pinnedPage) {

    if(level == this->height){
        //Leaf
        deleteEntry_helper_leaf<T>(key, curPageNo, parentNode, keyIndexAtParent, disposePageNo, pinnedPage);
    }
    else {
        //Internal node

        Page* curPage = NULL;
        this->bufMgr->readPage(this->file, curPageNo, curPage);
        pinnedPage.insert(curPageNo);
        NonLeafNode<T>* node = (NonLeafNode<T>*)curPage;
        //Search page pointer for key. -1 because the last pair doesn't have a key. it only has a pageNo
        int i;
        for(i = 0; i < node->usage; i ++ ){
            if(smallerThan(key, node->pageKeyPairArray[i].key)) {
                break;
            }
        }
        //i-1 because the key to be deleted is between keyArray[i-1] and keyArray[i]
        //when deletion propagates, keyArray[i-1] is the one to be deleted (deleted by children)
        //Example: [10] 500 [20] 600, key = 550
        //Index:   i-1  i-1  i   i 
        //After redistribtute: [10] 400 [20] 600, key = 550
        //Index:                i-1 i-1  i    i 
        deleteEntry_helper<T>(key, node->pageKeyPairArray[i].pageNo, node, i-1, level+1, disposePageNo, pinnedPage);

        if(level == 0 && node->usage == 0) {
            //Root is empty: assign its only child as the new root
            disposePageNo.push_back(this->rootPageNum);
            this->rootPageNum = node->pageKeyPairArray[0].pageNo;
            this->height --;
        }
        else if(level != 0 && node->usage < this->nodeOccupancy/2 - 1) {
            if(keyIndexAtParent != -1) {
                //Normal case: redistribute/merge with left sibling

                //current node's keyIndex is the same as left sibling's pageNoIndex
                PageId sibPageNo = parentNode->pageKeyPairArray[keyIndexAtParent].pageNo;  
                Page* sibPage = NULL;
                this->bufMgr->readPage(this->file, sibPageNo, sibPage);
                pinnedPage.insert(sibPageNo);
                NonLeafNode<T>* sibNode = (NonLeafNode<T>*)sibPage;

                if(sibNode->usage >= this->nodeOccupancy/2) {
                    //pull the key value at keyIndex from parent
                    //insert the key to first position of curNode
                    //get the last pageNo from sib. insert it to the first position of curNode
                    //get the last key from sib. update parent with it
                    dprintf("internal node redistribute with left sib\n");

                    PageId insertionPageNo = sibNode->pageKeyPairArray[sibNode->usage].pageNo;
                    PageKeyPair<T> pageKeyPair;
                    pageKeyPair.set( insertionPageNo, parentNode->pageKeyPairArray[keyIndexAtParent].key);

                    //Shift all elements to right in curNode
                    node->usage ++;
                    for(int j = node->usage; j > 0; j -- ){
                        node->pageKeyPairArray[j].pageNo = node->pageKeyPairArray[j-1].pageNo;
                        assignKey(node->pageKeyPairArray[j].key, node->pageKeyPairArray[j-1].key);
                    }

                    node->pageKeyPairArray[0].pageNo = pageKeyPair.pageNo;
                    assignKey(node->pageKeyPairArray[0].key, pageKeyPair.key);

                    //update parent with the last key from sib. 
                    assignKey(parentNode->pageKeyPairArray[keyIndexAtParent].key, 
                            sibNode->pageKeyPairArray[sibNode->usage-1].key);

                    //shrink sib page
                    sibNode->usage --;

                    //Return. parent will handle its own process
                }
                else {
                    //merge with sibling
                    //pull the key value at keyIndex from parent
                    //delete the page ptr at keyIndex+1

                    dprintf("internal node merge with left sib\n");

                    //insert the keyFromParent (k_) to the last pos
                    //      k1      k2      k_
                    //  p1      p2      p3
                    assignKey(sibNode->pageKeyPairArray[sibNode->usage].key, parentNode->pageKeyPairArray[keyIndexAtParent].key);
                    sibNode->usage ++;
                    deleteEntryFromNonLeaf(keyIndexAtParent, parentNode);       //delete the key and rhs page ptr

                    //insert the pageKeyPair from current node to sib
                    //      k1      k2      k_      k'1     k'2
                    //  p1      p2      p4      p'1     p'2     p'3
                    for(int i = 0; i < node->usage; i ++) {
                        sibNode->pageKeyPairArray[sibNode->usage].pageNo = node->pageKeyPairArray[i].pageNo;
                        assignKey(sibNode->pageKeyPairArray[sibNode->usage].key, node->pageKeyPairArray[i].key);
                        sibNode->usage ++;
                    }
                    sibNode->pageKeyPairArray[sibNode->usage].pageNo = node->pageKeyPairArray[node->usage].pageNo;

                    disposePageNo.push_back(curPageNo);

                    //return. parent will handle its own process
                }
                this->bufMgr->unPinPage(this->file, sibPageNo, true);
                pinnedPage.erase(sibPageNo);
            }
            else {
                //Special case: leftmost page ptr in the parent node
                //Use right sibling 
                
                //currentNode's pageNoIndex = keyIndexAtParent + 1
                //right sibling's pageNoIndex = keyIndexAtParent + 2
                
                //Illustration (parent node):
                //      k1      k2      k3
                //  p1      p2      p3      p4
                //  keyIndexAtParent: 1
                //  curPageNo: p2 (keyIndexAtParent + 1)
                //  sibPageNo: p3 (keyIndexAtParent + 2)
                
                PageId sibPageNo = parentNode->pageKeyPairArray[keyIndexAtParent+2].pageNo;  
                Page* sibPage = NULL;
                this->bufMgr->readPage(this->file, sibPageNo, sibPage);
                pinnedPage.insert(sibPageNo);
                NonLeafNode<T>* sibNode = (NonLeafNode<T>*)sibPage;

                if(sibNode->usage >= this->nodeOccupancy/2) {
                    //append parent's pageKeyArray[keyIndex+1].key to curNode
                    //get the first pageNo from sib, append it to curNode
                    //get the first key from sib, update parent's pageKeyArray[keyIndex+1].key

                    dprintf("internal node redistribute with right sib\n");

                    PageId insertionPageNo = sibNode->pageKeyPairArray[0].pageNo;

                    //Append insertion key and pageNo
                    assignKey(node->pageKeyPairArray[node->usage].key, parentNode->pageKeyPairArray[keyIndexAtParent+1].key);

                    node->pageKeyPairArray[node->usage+1].pageNo = insertionPageNo;
                    node->usage ++;

                    //update parent with the first key from sib. 
                    assignKey(parentNode->pageKeyPairArray[keyIndexAtParent+1].key, 
                            sibNode->pageKeyPairArray[0].key);

                    //shift all elements in sib to left
                    for(int i = 0; i < sibNode->usage; i ++) {
                        sibNode->pageKeyPairArray[i].pageNo = sibNode->pageKeyPairArray[i+1].pageNo;
                        assignKey(sibNode->pageKeyPairArray[i].key, sibNode->pageKeyPairArray[i+1].key);
                    }
                    //shrink sib page
                    sibNode->usage --;

                    //Return. parent will handle its own process
                }
                else {
                    //sibling merges into curNode
                    //pull the key value at keyIndex from parent (sibling's key)
                    //delete the page ptr at keyIndex+2 (sibling's page pointer)
                    
                    //Status of parent node:
                    //      k1      k2      k3
                    //  p1      p2      p3      p4
                    //  keyIndexAtParent: 1
                    //  curPageNo: p2
                    //  sibPageNo (to be deleted): p3
                    //  key to be deleted: k2

                    dprintf("internal node merge with right sib\n");

                    //Status of curNode:
                    //insert the key from parent to cur node
                    //      k1      k2      k_ 
                    //  p1      p2      p3    
                    assignKey(node->pageKeyPairArray[node->usage].key, parentNode->pageKeyPairArray[keyIndexAtParent+1].key);
                    node->usage ++;
                    deleteEntryFromNonLeaf(keyIndexAtParent+1, parentNode);       //delete the key and rhs page ptr

                    //Status of curNode:
                    //insert the pageKeyPair from sib to cur node
                    //      k1      k2      k_      k'1     k'2
                    //  p1      p2      p3      p'1     p'2     p'3
                    for(int i = 0; i < sibNode->usage; i ++) {
                        node->pageKeyPairArray[node->usage].pageNo = sibNode->pageKeyPairArray[i].pageNo;
                        assignKey(node->pageKeyPairArray[node->usage].key, sibNode->pageKeyPairArray[i].key);
                        node->usage ++;
                    }
                    node->pageKeyPairArray[node->usage].pageNo = sibNode->pageKeyPairArray[sibNode->usage].pageNo;

                    disposePageNo.push_back(sibPageNo);

                    //return. parent will handle its own process
                }
                this->bufMgr->unPinPage(this->file, sibPageNo, true);
                pinnedPage.erase(sibPageNo);
            }
        }
        else {
            //NonLeaf usage > occupancy/2. Do nothing
        }
        this->bufMgr->unPinPage(this->file, curPageNo, true);
        pinnedPage.erase(curPageNo);
    }
}
    
template<class T>
const void BTreeIndex::deleteEntry_helper_leaf(T key, PageId curPageNo, NonLeafNode<T>* parentNode, 
        int keyIndexAtParent, std::vector<PageId>& disposePageNo, std::set<PageId>& pinnedPage) {

    Page* curPage = NULL;
    this->bufMgr->readPage(this->file, curPageNo, curPage);
    pinnedPage.insert(curPageNo);
    LeafNode<T>* node = (LeafNode<T>*)curPage;

    //delete the entry from node
    if(deleteEntryFromLeaf(key, node) == false) {
        throw DeletionKeyNotFoundException();
    }
    else {
        if(this->height != 0 && node->usage < this->leafOccupancy/2) {
            if(keyIndexAtParent != -1) {
                //Redistribute/Merge with left sibling except leftmost node
            
                Page* sibPage = NULL;
                PageId sibPageNo = parentNode->pageKeyPairArray[keyIndexAtParent].pageNo;  
                this->bufMgr->readPage(this->file, sibPageNo, sibPage);
                pinnedPage.insert(sibPageNo);
                LeafNode<T>* sibNode = (LeafNode<T>*)sibPage;

                if(sibNode->usage > this->leafOccupancy/2) {
                    //Redistribute
                    dprintf("leaf redistribute with left sib\n");
                    int redistFromPos = sibNode->usage - 1;
                    RIDKeyPair<T> ridKeyPair = sibNode->ridKeyPairArray[redistFromPos];
                    sibNode->usage --;  //delete the redistributed entry

                    insertEntryInLeaf<T>(ridKeyPair.key, ridKeyPair.rid, node);

                    //Update the key of parent
                    assignKey(parentNode->pageKeyPairArray[keyIndexAtParent].key, ridKeyPair.key);

                    //All Done
                }
                else {
                    //Merge into left sibling
                    dprintf("leaf merge with left sib\n");
                    for(int i = 0; i < node->usage; i ++) {
                        sibNode->ridKeyPairArray[sibNode->usage].rid = node->ridKeyPairArray[i].rid;
                        assignKey(sibNode->ridKeyPairArray[sibNode->usage].key, node->ridKeyPairArray[i].key);
                        sibNode->usage ++;
                    }

                    dprintf("keyIndexAtParent: %d\n", keyIndexAtParent);
                    //Delete key from parent
                    deleteEntryFromNonLeaf(keyIndexAtParent, parentNode);

                    //Set left and right sib pointers
                    sibNode->rightSibPageNo = node->rightSibPageNo;

                    //Dispose curPage
                    disposePageNo.push_back(curPageNo);

                    //Return. Parent will handle its own redistribution/merging
                }
                this->bufMgr->unPinPage(this->file, sibPageNo, true);
                pinnedPage.erase(sibPageNo);
            }
            else {
                //Special case: leftmost node must redistribute/merge with right sibling
                Page* sibPage = NULL;
                PageId sibPageNo = node->rightSibPageNo;
                this->bufMgr->readPage(this->file, sibPageNo, sibPage);
                pinnedPage.insert(sibPageNo);
                LeafNode<T>* sibNode = (LeafNode<T>*)sibPage;
                if(sibNode->usage > this->leafOccupancy/2) {
                    //Redistribute
                    dprintf("special: leaf redistribute with right sib\n");
                    int redistFromPos = 0;
                    RIDKeyPair<T> ridKeyPair;
                    ridKeyPair.rid = sibNode->ridKeyPairArray[redistFromPos].rid;
                    assignKey(ridKeyPair.key, sibNode->ridKeyPairArray[redistFromPos].key);

                    deleteEntryFromLeaf<T>(ridKeyPair.key, sibNode);

                    node->ridKeyPairArray[node->usage].rid = ridKeyPair.rid;
                    assignKey(node->ridKeyPairArray[node->usage].key, ridKeyPair.key);

                    node->usage ++;

                    //Update the key of parent
                    //Since curNode is the leftmost, its right sibling must have the same parent
                    //The position of key to be updated is the index of the key of curNode + 1
                    assignKey(parentNode->pageKeyPairArray[keyIndexAtParent+1].key, 
                            sibNode->ridKeyPairArray[0].key);

                    //All Done
                }
                else {
                    //Sibling merges into curNode
                    dprintf("special: leaf merge with right sib\n");
                    for(int i = 0; i < sibNode->usage; i ++) {
                        node->ridKeyPairArray[node->usage].rid = sibNode->ridKeyPairArray[i].rid;
                        assignKey(node->ridKeyPairArray[node->usage].key, sibNode->ridKeyPairArray[i].key);
                        node->usage ++;
                    }
                    //Delete sibling's key from parent
                    //Note that sibling's key index must be at current key index + 1
                    deleteEntryFromNonLeaf<T>(keyIndexAtParent+1, parentNode);
                    
                    //Set left and right sib pointers
                    node->rightSibPageNo = sibNode->rightSibPageNo;

                    //Dispose right sibling
                    disposePageNo.push_back(sibPageNo);

                    //Done with curNode. Parent will handle its own redistribution/merging
                }
                this->bufMgr->unPinPage(this->file, sibPageNo, true);
                pinnedPage.erase(sibPageNo);
            }
        }
        else {
            //Leaf usage > occupancy/2. Do nothing
        }
    }
    this->bufMgr->unPinPage(this->file, curPageNo, true);
    pinnedPage.erase(curPageNo);
}

template<class T>
const bool BTreeIndex::deleteEntryFromLeaf(T key, LeafNode<T>*  node) {
    int i = 0;
    for(i = 0; i < node->usage; i ++ ){
        if(equals(key, node->ridKeyPairArray[i].key)) {
            break;
        }
    }

    //Key Not found
    if(i == node->usage) { return false; }

    //Shift all elements after this position
    for(int j = i; j < node->usage - 1; j ++){
        node->ridKeyPairArray[j].rid = node->ridKeyPairArray[j+1].rid;
        assignKey(node->ridKeyPairArray[j].key, node->ridKeyPairArray[j+1].key);
    }

    node->usage --;
    return true;
}

template<class T>
const void BTreeIndex::deleteEntryFromNonLeaf(const int keyIndex, NonLeafNode<T>* node) {
    //Simply shift all elements after this position to left

    //Note the skew between key and pageNo index
    //Example: keyIndex is 1, k1 and p2 will be deleted
    //      k0      [k1]      k2
    //  p0      p1      [p2]       p3

    for(int j = keyIndex; j < node->usage - 1; j ++){
        //Sometimes the child is the leftmost of the parent. keyIndex can be -1
        if(j >= 0) {
            assignKey(node->pageKeyPairArray[j].key, node->pageKeyPairArray[j+1].key);
        }
        node->pageKeyPairArray[j+1].pageNo = node->pageKeyPairArray[j+2].pageNo;
    }
    node->usage --;
}

// -----------------------------------------------------------------------------
// Validation functions
// -----------------------------------------------------------------------------
const bool BTreeIndex::validate(bool showInfo) {
    if(showInfo) std::cout<<"\n========= Validation Result ==========\n";

    std::set<PageId> pinnedPage;
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
            dprintf("Buffer manager is not clean:\n");
            this->bufMgr->printSelfPinned();
            throw ValidationFailedException();
        }
    }
    catch(ValidationFailedException e) {
        std::set<PageId>::iterator it;
        for (it = pinnedPage.begin(); it != pinnedPage.end(); ++it) {
            PageId pageNo = *it;
            this->bufMgr->unPinPage(this->file, pageNo, false);
        }
        pinnedPage.clear();
        std::cout<<"======================================\n";
        dprintf("Validation failed\n");
        std::cout<<"======================================\n\n";
        return false;
    }

    if(showInfo) {
        dprintf("Validation passed\n");
        std::cout<<"======================================\n\n";
    }
    return true;
}

template<class T>
const void BTreeIndex::validate_helper(PageId curPageNo, int level, std::set<PageId>& pinnedPage) {
    if(level == this->height) {
        validate_helper_leaf<T>(curPageNo, pinnedPage);
    }
    else {
        Page* curPage = NULL;
        this->bufMgr->readPage(this->file, curPageNo, curPage);
        pinnedPage.insert(curPageNo);
        NonLeafNode<T>* node = (NonLeafNode<T>*)curPage;

        if((level != 0) && (node->usage < this->nodeOccupancy/2-1 || node->usage > this->nodeOccupancy)) {
            dprintf("Internal Page #%d usage invalid\n", curPageNo);
            dprintf("Usage: %d, nodeOccupancy: %d\n", node->usage, this->nodeOccupancy);
            throw ValidationFailedException();
        }

        //Validation
        for(int i = 0; i <= node->usage; i ++) {
            PageId childPageNo = node->pageKeyPairArray[i].pageNo;
            if(childPageNo == 0) {
                dprintf("Page #%d pageNo index #%d pageNo is 0\n", curPageNo, i);
                throw ValidationFailedException();
            }

            Page* childPage = NULL;
            this->bufMgr->readPage(this->file, childPageNo, childPage);
            pinnedPage.insert(childPageNo);

            T lowKey, highKey;
            if(level == this->height - 1) {
                LeafNode<T>* childNode = (LeafNode<T>*)childPage;

                if(childNode->usage < this->leafOccupancy/2 || childNode->usage > this->leafOccupancy) {
                    dprintf("Leaf Page #%d usage invalid\n", childPageNo);
                    dprintf("Usage: %d, leafOccupancy: %d\n", node->usage, this->leafOccupancy);
                    throw ValidationFailedException();
                }

                lowKey = childNode->ridKeyPairArray[0].key;
                highKey = childNode->ridKeyPairArray[childNode->usage-1].key;
            }
            else {
                NonLeafNode<T>* childNode = (NonLeafNode<T>*)childPage;
            
                //if occupancy is 6, then the resulting two nodes will both have usage of 2. Hence -1 is needed
                if(childNode->usage < this->nodeOccupancy/2-1 || childNode->usage > this->nodeOccupancy) {
                    dprintf("Internal Page #%d usage invalid\n", childPageNo);
                    dprintf("Usage: %d, nodeOccupancy: %d\n", childNode->usage, this->nodeOccupancy);
                    throw ValidationFailedException();
                }

                lowKey = childNode->pageKeyPairArray[0].key;
                highKey = childNode->pageKeyPairArray[childNode->usage-1].key;
                if(lowKey > highKey) {
                    dprintf("Page #%d lowKey > highKey\n", childPageNo);
                    throw ValidationFailedException();
                }
            }

            if(i != node->usage && !smallerThan(highKey, node->pageKeyPairArray[i].key)) {
                dprintf("Child Page #%d highKey >= parent rhs key\n", childPageNo);
                std::cout<<"highKey: "<<highKey<<", parent rhs key: "<<node->pageKeyPairArray[i].key<<"\n";
                throw ValidationFailedException();
            }
            if(i != 0 && smallerThan(lowKey, node->pageKeyPairArray[i-1].key)) {
                dprintf("Child Page #%d lowKey < parent lhs key\n", childPageNo);
                std::cout<<"lowKey: "<<lowKey<<", parent lhs key: "<<node->pageKeyPairArray[i-1].key<<"\n";
                throw ValidationFailedException();
            }
            this->bufMgr->unPinPage(this->file, childPageNo, false);
            pinnedPage.erase(childPageNo);


            //Recursively validate child node
            validate_helper<T>(childPageNo, level+1, pinnedPage);
        }

        //Validation for this node completed
        this->bufMgr->unPinPage(this->file, curPageNo, false);
        pinnedPage.erase(curPageNo);
    }
}

template<class T>
const void BTreeIndex::validate_helper_leaf(PageId curPageNo, std::set<PageId>& pinnedPage) {
    Page* curPage = NULL;
    this->bufMgr->readPage(this->file, curPageNo, curPage);
    pinnedPage.insert(curPageNo);
    LeafNode<T>* node = (LeafNode<T>*)curPage;

    if((this->height != 0) && (node->usage < this->leafOccupancy/2 || node->usage > this->leafOccupancy)) {
        dprintf("Leaf Page #%d usage invalid\n", curPageNo);
        dprintf("Usage: %d, leafOccupancy: %d\n", node->usage, this->leafOccupancy);
        throw ValidationFailedException();
    }

    for(int i = 1; i < node->usage; i ++) {
        if(smallerThan(node->ridKeyPairArray[i].key, node->ridKeyPairArray[i-1].key)) {
            dprintf("Leaf Page #%d invalid key order\n", curPageNo);
            throw ValidationFailedException();
        }
    }

    this->bufMgr->unPinPage(this->file, curPageNo, false);
    pinnedPage.erase(curPageNo);
}

const bool BTreeIndex::isEmpty() {
    if(this->height != 0) return false;
    bool empty = false;

    Page* rootPage;
    this->bufMgr->readPage(this->file, this->rootPageNum, rootPage);
    if(this->attributeType == INTEGER) {
        LeafNode<int>* rootPageInt = (LeafNode<int>*)rootPage;
        empty = (rootPageInt->usage == 0);
    }
    else if(this->attributeType == DOUBLE) {
        LeafNode<double>* rootPageInt = (LeafNode<double>*)rootPage;
        empty = (rootPageInt->usage == 0);
    }
    else {
        LeafNode<char*>* rootPageInt = (LeafNode<char*>*)rootPage;
        empty = (rootPageInt->usage == 0);
    }
    this->bufMgr->unPinPage(this->file, this->rootPageNum, false);
    return empty;
}


// -----------------------------------------------------------------------------
// Debugging functions
// -----------------------------------------------------------------------------

const void BTreeIndex::printMeta() 
{
    this->bufMgr->printSelfNonNull();
    Page* metaInfoPage = NULL;
    this->bufMgr->readPage(this->file, this->headerPageNum, metaInfoPage);
    struct IndexMetaInfo* indexMetaInfo = (struct IndexMetaInfo*)(metaInfoPage);

    std::cout<<"\n========== Index Meta Info ==========\n";

    std::cout<<"RootPageNo: "<<indexMetaInfo->rootPageNo<<std::endl;
    std::cout<<"Height: "<<indexMetaInfo->height<<std::endl;
    std::cout<<"AttrByteOffset: "<<indexMetaInfo->attrByteOffset<<std::endl;
    std::cout<<"RelationName: "<<indexMetaInfo->relationName<<std::endl;
    std::cout<<"AttrType: "<<indexMetaInfo->attrType<<std::endl;

    this->bufMgr->unPinPage(this->file, this->headerPageNum, false);

    std::cout<<"=====================================\n\n";
}

const int BTreeIndex::dprintf(const char *format, ...)
{
    int done = 0;
#ifdef DEBUG
    va_list arg;
    va_start (arg, format);
    done = vfprintf(stdout, format, arg);
    va_end (arg);
#endif
    return done;
}


const void BTreeIndex::dumpAllLevels()  
{
#ifdef DEBUG
    for(int i = 0; i <= this->height; i ++) {
        dprintf("Dumping Level %d\n", i);
        dumpLevel(i);
    }
#endif
}

const void BTreeIndex::dumpLevel(int dumpLevel)  
{
#ifdef DEBUG
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
#endif
}


template<class T>
const void BTreeIndex::dumpLevel1(PageId curPageNo, int curLevel, int dumpLevel) 
{
#ifdef DEBUG
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
#endif
}

template<char*>
const void BTreeIndex::dumpLevel1(PageId curPageNo, int curLevel, int dumpLevel) 
{
#ifdef DEBUG
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
                dprintf("[%d] %s ", curNode->pageKeyPairArray[i].pageNo, curNode->pageKeyPairArray[i].key);
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
#endif
}

template<class T>
const void BTreeIndex::dumpLeaf() 
{
#ifdef DEBUG
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
            dprintf("\n\n##### Warning: curPageNo %d is too large, stopping \n\n\n", curPageNo);
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
#endif
}

template<char*>
const void BTreeIndex::dumpLeaf() 
{
#ifdef DEBUG
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
            dprintf("%s ", curNode->ridKeyPairArray[i].key);
        }
        std::cout<<" u:"<<curNode->usage<<"";
        std::cout<<"\n";
        PageId tmp = curPageNo;
        curPageNo = curNode->rightSibPageNo;
        this->bufMgr->unPinPage(this->file, tmp, false);
    }
#endif
}



}
