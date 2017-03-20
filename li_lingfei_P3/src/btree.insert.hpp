#include <iostream>
#include <string>
#include "string.h"
#include <sstream>
#include <vector>

#include "types.h"
#include "page.h"
#include "file.h"
#include "buffer.h"
#include "btree.h"

namespace badgerdb
{



template<class T>
const void BTreeIndex::insertEntry_template(T key, const RecordId rid) 
{
    PageKeyPair<T> ret = this->insertEntry_helper(key, rid, this->rootPageNum, 0);
    this->createNewRoot<T>(ret);
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
            printf("splitting...\n");
            PageId newPageNo;
            Page* newPage;
            this->bufMgr->allocPage(this->file, newPageNo, newPage);
            LeafNode<T>* newNode = (LeafNode<T>*)newPage;
            printf("new leaf: %d\n", newPageNo);

            //redistribute
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
        NonLeafNode<T>* node = (NonLeafNode<T>*)curPage;
        int i;
        for(i = 0; i < node->usage; i ++ ){
            if(smallerThan(key, node->pageKeyPairArray[i].key)) {
                break;
            }
        }
        PageId childPageNo = node->pageKeyPairArray[i].pageNo;
        PageKeyPair<T> pushUp = this->insertEntry_helper(key, rid, childPageNo, level + 1);
        if(pushUp.pageNo != 0) {
            insertEntryInNonLeaf(pushUp.key, pushUp.pageNo, node);
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

        printf("new root page no: %d\n", rootPageNo);
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
    std::cout<<"inserting leaf key: "<<key<<std::endl;
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

    node->usage ++;
}


template<class T>
const void BTreeIndex::insertEntryInNonLeaf(T key, const PageId pageNo, NonLeafNode<T>* node) {
    std::cout<<"inserting internal key: "<<key<<" pageNo "<<pageNo<<std::endl;
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


}
