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
#include "exceptions/end_of_file_exception.h"

#include <string>
#include <sstream>


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
            //cast the root as a new leaf node
            struct LeafNodeInt* rootNode = (struct LeafNodeInt*)rootPage;

            //Init the new node
            for(int i = 0; i < this->leafOccupancy; i ++ ){
                rootNode->keyArray[i] = 0;
            }
            rootNode->rightSibPageNo = 0;
            rootNode->usage = 0;
            rootNode->parent = 0;


        } else {
            printf("Btree constructor for double and string not implemented yet\n");
            return;
        }

        //Release meta info page
        this->bufMgr->unPinPage(this->file, metaInfoPageNo, true);
        //Release root node
        this->bufMgr->unPinPage(this->file, this->rootPageNum, true);
    }

    printMeta();

    printf("BTreeIndex: constructor finished\n");
}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{ }


// -----------------------------------------------------------------------------
// Helper function. Insert an entry to a non-full leaf node. 
// Shift elements as needed, but no splitting will happen
// -----------------------------------------------------------------------------
const void BTreeIndex::insertEntryLeafNotFull(const void* key, const RecordId rid, void* node) {
    if(this->attributeType == INTEGER) {
        struct LeafNodeInt* curNode = (struct LeafNodeInt*)node;

        assert(curNode->usage < this->nodeOccupancy);

        int keyVal = *(int*)key;
        int i = 0;
        for(i = 0; i < curNode->usage + 1; i ++ ){
            if(i == curNode->usage || keyVal < curNode->keyArray[i]) {
                //Rightmost position is reached (including when there's no key in the node)
                // or the suitable insertion position is found
                break;
            }
        }

        //Shift all elements after this position
        for(int j = curNode->usage; j > i; j -- ){
            curNode->keyArray[j] = curNode->keyArray[j-1];
            curNode->ridArray[j] = curNode->ridArray[j-1];
        }

        curNode->keyArray[i] = keyVal;
        curNode->ridArray[i] = rid;

        curNode->usage ++;
    }
    else {
        printf("ERROR: insertEntryNonLeafNotFull not implemented for non-int type\n");
    }
}

// -----------------------------------------------------------------------------
// Helper function. Insert an entry to a non-full internal node. 
// Shift elements as needed, but no splitting will happen
// The new pageNo will always be on the right-hand-side of the new key
// -----------------------------------------------------------------------------
const void BTreeIndex::insertEntryNonLeafNotFull(const void* key, const PageId pageNo, void* node) {
    if(this->attributeType == INTEGER) {
        struct NonLeafNodeInt* curNode = (struct NonLeafNodeInt*)node;

        assert(curNode->usage < this->nodeOccupancy);

        int keyVal = *(int*)key;
        int i = 0;
        for(i = 0; i < curNode->usage + 1; i ++ ){
            if(i == curNode->usage || keyVal < curNode->keyArray[i]) {
                //Rightmost position is reached (including when there's no key in the node)
                // or the suitable insertion position is found
                break;
            }
        }

        //Shift all elements after this position
        for(int j = curNode->usage; j > i; j -- ){
            curNode->keyArray[j] = curNode->keyArray[j-1];
            curNode->pageNoArray[j+1] = curNode->pageNoArray[(j+1) -1];   //shifting the rhs ptr of the key
        }
        curNode->keyArray[i] = keyVal;
        curNode->pageNoArray[i+1] = pageNo;    //insert the ptr to rhs

        curNode->usage ++;
    }
    else {
        printf("ERROR: insertEntryNonLeafNotFull not implemented for non-int type\n");
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

const void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
    printf("BTreeIndex: inserting entry...\n");

    Datatype attrType = this->attributeType;

    Page* curPage;
    PageId curPageNo = this->rootPageNum;
    PageId prevPageNo = curPageNo;
    this->bufMgr->readPage(this->file, curPageNo, curPage);

    int curHeight = 0;
    //Search until leaf. curPage will be the insertion node
    while(curHeight++ < this->height) {
        if(attrType == INTEGER) {
            prevPageNo = curPageNo;

            struct NonLeafNodeInt* node = (struct NonLeafNodeInt*)curPage;
            printf("Internal node: %d\n", curPageNo);
            printNode(node, curPageNo);
            int keyVal = *(int*)key;

            //Find the first pointer s.t. all keys to the left are smaller than inserting key
            for(int i = 0; i < node->usage + 1; i ++ ){
                if(i == node->usage || keyVal < node->keyArray[i]) {
                    //Rightmost position is reached (including when there's no key in the node)
                    // or the suitable insertion position is found
                    curPageNo = node->pageNoArray[i];
                    break;
                }
            }

            //Release all non-leaf pages. Keep leaf pages pinned for later used
            this->bufMgr->unPinPage(this->file, prevPageNo, false);

            //Read the next page (internal or leaf)
            this->bufMgr->readPage(this->file, curPageNo, curPage);

        } else {
            //TODO: Not implemented
            printf("Insertion not implemented for double and string type");
            return;
        }
    }

    printf("BTreeIndex: insertion: leaf page no: %d\n", curPageNo);

    //Reached leaf. Find the correct position
    if(attrType == INTEGER) {
        struct LeafNodeInt* curNode = (struct LeafNodeInt*)curPage;
        int keyVal = *(int*)key;

        //Split and copy/push up
        assert(curNode->usage <= this->leafOccupancy);
        if(curNode->usage == this->leafOccupancy) {
            printf("BTreeIndex: insertion: Node is full. splitting...\n");

            PageId newPageNo = 0;
            Page* newPage = NULL;
            this->bufMgr->allocPage(this->file, newPageNo, newPage);

            //Initialization
            LeafNodeInt* newNode = (LeafNodeInt*)newPage;
            for(int i = 0; i < this->leafOccupancy; i ++ ){
                newNode->keyArray[i] = 0;
            }
            newNode->parent = curNode->parent;


            //redistribute
            int cnt = 0;
            for(int i = this->leafOccupancy/2; i < this->leafOccupancy; i ++) {
                //newNode is on the rhs, curNode is on the lhs
                newNode->keyArray[cnt] = curNode->keyArray[i];
                newNode->ridArray[cnt] = curNode->ridArray[i];

                curNode->keyArray[i] = 0;
                cnt ++;
            }

            //set usage
            newNode->usage = cnt;
            curNode->usage = this->leafOccupancy - cnt;
            printf("usage: lhs %d, rhs %d\n", curNode->usage, newNode->usage);

            if(keyVal >= newNode->keyArray[0]) {
                //insert the new node (rhs)
                insertEntryLeafNotFull(key, rid, (void*)newNode);
            }
            else {
                //insert the current node (lhs)
                insertEntryLeafNotFull(key, rid, (void*)curNode);
            }

            //set sib pointers. note: order is important
            newNode->rightSibPageNo = curNode->rightSibPageNo;
            curNode->rightSibPageNo = newPageNo;

            printf("Redistribution result: \n");
            printNode(curNode, curPageNo);
            printNode(newNode, newPageNo);
            
            //copy up
            int upKey = newNode->keyArray[0];
            PageId upPageNo = newPageNo;    //the page pointer should reside on the key's right

            //Jobs with the new node is done. Release the new node
            this->bufMgr->unPinPage(this->file, newPageNo, true);

            PageId parentPageNo = curNode->parent;
            Page* parentPage = NULL;

            bool inProcess = true;

            //Recursively Copy/Push up
            while(inProcess) {
                if(parentPageNo == 0) {
                    //Reached the top of tree. Create a new root and increment tree height
                    this->bufMgr->allocPage(this->file, parentPageNo, parentPage);
                    struct NonLeafNodeInt* parentNode = (struct NonLeafNodeInt*)parentPage;

                    for(int i = 0; i < this->nodeOccupancy; i ++ ){
                        parentNode->keyArray[i] = 0;
                    }
                    parentNode->usage = 0;
                    parentNode->parent = 0;
                    parentNode->pageNoArray[0] = curPageNo;

                    curNode->parent = newNode->parent = parentPageNo;

                    this->rootPageNum = parentPageNo;
                    this->height ++;
                } 
                else {
                    this->bufMgr->readPage(this->file, parentPageNo, parentPage);
                }

                struct NonLeafNodeInt* parentNode = (struct NonLeafNodeInt*)parentPage;

                if(parentNode->usage == this->nodeOccupancy) {
                    //TODO: Parent node is full. Need one more split & redistribution
                
                }
                else {
                    //Parent node is not full. Insert the upKey and done

                    insertEntryNonLeafNotFull((void*)&upKey, upPageNo, (void*)parentNode);

                    inProcess = false;
                }

                printNode(parentNode, parentPageNo);

                //Release parent node
                this->bufMgr->unPinPage(this->file, parentPageNo, true);
            }

            //curNode will be released towards the end of function
        }
        else {      
            //usage < leafOccupancy. Insert the new key-rid to the leaf and done
            
            printf("BTreeIndex: insertion: Node is not full. inserting...\n");

            insertEntryLeafNotFull(key, rid, (void*)curNode);
        }

        printNode(curNode, curPageNo);

    } else {
        //TODO: Not implemented
        printf("Insertion not implemented for double and string type");
    }

    this->bufMgr->unPinPage(this->file, curPageNo, true);

    printf("BTreeIndex: insertion complete \n");
}

// -----------------------------------------------------------------------------
// BTreeIndex::printNode
// -----------------------------------------------------------------------------
//
const void BTreeIndex::printNode(NonLeafNodeInt* node, PageId pageNo)
{
    std::cout<<"\n========== Node "<<pageNo<<" ==========\n";
    std::cout<<"Parent: "<<node->parent<<std::endl;
    std::cout<<"Occupancy: "<<INTARRAYNONLEAFSIZE<<std::endl;
    std::cout<<"Usage: "<<node->usage<<std::endl;
    std::cout<<"Content: "<<std::endl;
    for(int i = 0; i < node->usage; i ++) {
        std::cout<<" ["<<node->pageNoArray[i]<<"] ";
        std::cout<<node->keyArray[i];
    }
    std::cout<<" ["<<node->pageNoArray[node->usage]<<"]";
    std::cout<<std::endl;
    std::cout<<"===============================\n\n";
}


const void BTreeIndex::printNode(LeafNodeInt* node, PageId pageNo)
{
    std::cout<<"\n========== Node "<<pageNo<<" ==========\n";
    std::cout<<"Parent: "<<node->parent<<std::endl;
    std::cout<<"Occupancy: "<<INTARRAYLEAFSIZE<<std::endl;
    std::cout<<"Usage: "<<node->usage<<std::endl;
    std::cout<<"SibPage: "<<node->rightSibPageNo<<std::endl;

    std::cout<<"Keys: "<<std::endl;
    for(int i = 0; i < node->usage; i ++) {
        std::cout<<node->keyArray[i]<<" ";
    }
    std::cout<<std::endl;
    std::cout<<"===============================\n\n";
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
// BTreeIndex::dumpLevel
// -----------------------------------------------------------------------------

const void BTreeIndex::dumpLevel(int dumpLevel)  
{
    dumpLevel1(this->rootPageNum, 0, dumpLevel);
}


const void BTreeIndex::dumpLevel1(PageId curPageNo, int curLevel, int dumpLevel) 
{
    if(curLevel > dumpLevel) {
        return;
    }
    Page* curPage = NULL;

    if(curPageNo != 0) {
        this->bufMgr->readPage(this->file, curPageNo, curPage);
        struct NonLeafNodeInt* curNode = (struct NonLeafNodeInt*)curPage;
        if(curLevel == dumpLevel) {
            printNode(curNode, curPageNo);
        }
        else {
            for(int i = 0; i < curNode->usage + 1; i ++) {
                dumpLevel1(curNode->pageNoArray[i], curLevel + 1, dumpLevel);
            }
        }
        this->bufMgr->unPinPage(this->file, curPageNo, false);
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::dumpLeaf
// -----------------------------------------------------------------------------

const void BTreeIndex::dumpLeaf() 
{

    PageId curPageNo = this->rootPageNum;
    Page* curPage = NULL;
    for(int i = 0; i < this->height; i ++) {
        this->bufMgr->readPage(this->file, curPageNo, curPage);
        struct NonLeafNodeInt* curNode = (struct NonLeafNodeInt*)curPage;
        PageId tmp = curPageNo;
        curPageNo = curNode->pageNoArray[0];
        this->bufMgr->unPinPage(this->file, tmp, false);
    }

    while(curPageNo != 0) {
        this->bufMgr->readPage(this->file, curPageNo, curPage);
        struct LeafNodeInt* curNode = (struct LeafNodeInt*)curPage;
        printNode(curNode, curPageNo);
        PageId tmp = curPageNo;
        curPageNo = curNode->rightSibPageNo;
        this->bufMgr->unPinPage(this->file, tmp, false);
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

}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

const void BTreeIndex::scanNext(RecordId& outRid) 
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
const void BTreeIndex::endScan() 
{

}

}
