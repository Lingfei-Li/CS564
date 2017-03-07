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
	const PageKeyPair<T> BTreeIndex::insertEntry_helper(T key, const RecordId rid, PageId curPageNo, int level) {
        Page* curPage = NULL;
        this->bufMgr->readPage(this->file, curPageNo, curPage);

        PageKeyPair<T> ret;
        ret.set(0, 0);

        if(level == this->height){
            //Base case: Reached leaf
            LeafNode<T>* node = (LeafNode<T>*)curPage;

            insertEntryInLeaf<T>(key, rid, node);

            //Split
            if(node->usage == this->leafOccupancy) {
                PageId newPageNo;
                Page* newPage;
                this->bufMgr->allocPage(this->file, newPageNo, newPage);
                LeafNode<T>* newNode = (LeafNode<T>*)newPage;

                //redistribute
                int cnt = 0;
                for(int i = this->leafOccupancy/2; i < this->leafOccupancy; i ++) {
                    //newNode is rhs, node is lhs
                    newNode->ridKeyPairArray[cnt] = node->ridKeyPairArray[i];
                    cnt ++;
                }

                //set usage
                newNode->usage = cnt;
                node->usage = this->leafOccupancy - cnt;

                //set sib pointers. note: order is important
                newNode->rightSibPageNo = node->rightSibPageNo;
                newNode->leftSibPageNo = curPageNo;
                node->rightSibPageNo = newPageNo;

                //Update right node's left sib
                Page* rightSibPage;
                if(newNode->rightSibPageNo != 0) {
                    this->bufMgr->readPage(this->file, newNode->rightSibPageNo, rightSibPage);
                    LeafNode<T>* rightSibNode = (LeafNode<T>*)rightSibPage;
                    rightSibNode->leftSibPageNo = newPageNo;
                    this->bufMgr->unPinPage(this->file, newNode->rightSibPageNo, true);
                }

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
                //TODO: change the comparison method for char*
                if(key < node->pageKeyPairArray[i].key) {
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
                        newNode->pageKeyPairArray[cnt] = node->pageKeyPairArray[i];
                        cnt ++;
                    }
                    newNode->pageKeyPairArray[cnt] = node->pageKeyPairArray[this->nodeOccupancy];

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
    const void BTreeIndex::insertEntryInLeaf(T key, const RecordId rid, LeafNode<T>* node) {
        int i = 0;
        for(i = 0; i < node->usage; i ++ ){
            if(key < node->ridKeyPairArray[i].key) {
                break;
            }
        }

        //Shift all elements after this position
        for(int j = node->usage; j > i; j -- ){
            node->ridKeyPairArray[j] = node->ridKeyPairArray[j-1];
        }
        node->ridKeyPairArray[i].key = key;
        node->ridKeyPairArray[i].rid = rid;

        node->usage ++;
    }


    template<class T>
    const void BTreeIndex::insertEntryInNonLeaf(T key, const PageId pageNo, NonLeafNode<T>* node) {
        int i = 0;
        for(i = 0; i < node->usage; i ++ ){
            if(key < node->pageKeyPairArray[i].key) {
                break;
            }
        }

        //Shift all elements after this position
        for(int j = node->usage; j > i; j -- ){
            node->pageKeyPairArray[j].key = node->pageKeyPairArray[j - 1].key;
            node->pageKeyPairArray[j+1].pageNo = node->pageKeyPairArray[j+1-1].pageNo;
        }

        node->pageKeyPairArray[i].key = key;
        node->pageKeyPairArray[i+1].pageNo = pageNo;

        node->usage ++;
    }
    


}
