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
    const void BTreeIndex::deleteEntry_helper(T key, PageId curPageNo, NonLeafNode<T>* parentNode, 
            int keyIndexAtParent, int level, std::vector<PageId>& disposePageNo, std::stack<PageId>& pinnedPage) {

        if(level == this->height){
            //Leaf
            deleteEntry_helper_leaf<T>(key, curPageNo, parentNode, keyIndexAtParent, disposePageNo, pinnedPage);
        }
        else {
            //Internal node

            Page* curPage = NULL;
            this->bufMgr->readPage(this->file, curPageNo, curPage);
            pinnedPage.push(curPageNo);
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
                    pinnedPage.push(sibPageNo);
                    NonLeafNode<T>* sibNode = (NonLeafNode<T>*)sibPage;

                    if(sibNode->usage >= this->nodeOccupancy/2) {
                        //pull the key value at keyIndex from parent
                        //insert the key to first position of curNode
                        //get the last pageNo from sib. insert it to the first position of curNode
                        //get the last key from sib. update parent with it
                        printf("internal node redistribute with left sib\n");

                        PageId insertionPageNo = sibNode->pageKeyPairArray[sibNode->usage].pageNo;
                        T insertionKey = parentNode->pageKeyPairArray[keyIndexAtParent].key;
                        PageKeyPair<T> pageKeyPair;
                        pageKeyPair.set( insertionPageNo, insertionKey );

                        //Shift all elements to right in curNode
                        node->usage ++;
                        for(int j = node->usage; j > 0; j -- ){
                            node->pageKeyPairArray[j] = node->pageKeyPairArray[j-1];
                        }

                        node->pageKeyPairArray[0] = pageKeyPair;

                        //update parent with the last key from sib. 
//                        parentNode->pageKeyPairArray[keyIndexAtParent].key = sibNode->pageKeyPairArray[sibNode->usage-1].key;
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

                        printf("internal node merge with left sib\n");

                        T keyFromParent = parentNode->pageKeyPairArray[keyIndexAtParent].key;
                        deleteEntryFromNonLeaf(keyIndexAtParent, parentNode);       //delete the key and rhs page ptr

                        //insert the keyFromParent (k_) to the last pos
                        //      k1      k2      k_
                        //  p1      p2      p3
//                        sibNode->pageKeyPairArray[sibNode->usage].key = keyFromParent;
                        assignKey(sibNode->pageKeyPairArray[sibNode->usage].key, keyFromParent);
                        sibNode->usage ++;

                        //insert the pageKeyPair from current node to sib
                        //      k1      k2      k_      k'1     k'2
                        //  p1      p2      p4      p'1     p'2     p'3
                        for(int i = 0; i < node->usage; i ++) {
                            sibNode->pageKeyPairArray[sibNode->usage] = node->pageKeyPairArray[i];
                            sibNode->usage ++;
                        }
                        sibNode->pageKeyPairArray[sibNode->usage].pageNo = node->pageKeyPairArray[node->usage].pageNo;

                        disposePageNo.push_back(curPageNo);

                        //return. parent will handle its own process
                    }
                    this->bufMgr->unPinPage(this->file, sibPageNo, true);
                    pinnedPage.pop();
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
                    pinnedPage.push(sibPageNo);
                    NonLeafNode<T>* sibNode = (NonLeafNode<T>*)sibPage;

                    if(sibNode->usage >= this->nodeOccupancy/2) {
                        //append parent's pageKeyArray[keyIndex+1].key to curNode
                        //get the first pageNo from sib, append it to curNode
                        //get the first key from sib, update parent's pageKeyArray[keyIndex+1].key

                        printf("internal node redistribute with right sib\n");

                        T insertionKey = parentNode->pageKeyPairArray[keyIndexAtParent+1].key;
                        PageId insertionPageNo = sibNode->pageKeyPairArray[0].pageNo;

                        //Append insertion key and pageNo
//                        node->pageKeyPairArray[node->usage].key = insertionKey;
                        assignKey(node->pageKeyPairArray[node->usage].key, insertionKey);

                        node->pageKeyPairArray[node->usage+1].pageNo = insertionPageNo;
                        node->usage ++;

                        //update parent with the first key from sib. 
//                        parentNode->pageKeyPairArray[keyIndexAtParent+1].key = sibNode->pageKeyPairArray[0].key;
                        assignKey(parentNode->pageKeyPairArray[keyIndexAtParent+1].key, 
                                sibNode->pageKeyPairArray[0].key);

                        //shift all elements in sib to left
                        for(int i = 0; i < sibNode->usage; i ++) {
                            sibNode->pageKeyPairArray[i] = sibNode->pageKeyPairArray[i+1];
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

                        printf("internal node merge with right sib\n");

                        T keyFromParent = parentNode->pageKeyPairArray[keyIndexAtParent+1].key;
                        deleteEntryFromNonLeaf(keyIndexAtParent+1, parentNode);       //delete the key and rhs page ptr

                        //Status of curNode:
                        //insert the key from parent to cur node
                        //      k1      k2      k_ 
                        //  p1      p2      p3    
//                        node->pageKeyPairArray[node->usage].key = keyFromParent;
                        assignKey(node->pageKeyPairArray[node->usage].key, keyFromParent);
                        node->usage ++;

                        //Status of curNode:
                        //insert the pageKeyPair from sib to cur node
                        //      k1      k2      k_      k'1     k'2
                        //  p1      p2      p3      p'1     p'2     p'3
                        for(int i = 0; i < sibNode->usage; i ++) {
                            node->pageKeyPairArray[node->usage] = sibNode->pageKeyPairArray[i];
                            node->usage ++;
                        }
                        node->pageKeyPairArray[node->usage].pageNo = sibNode->pageKeyPairArray[sibNode->usage].pageNo;

                        disposePageNo.push_back(sibPageNo);

                        //return. parent will handle its own process
                    }
                    this->bufMgr->unPinPage(this->file, sibPageNo, true);
                    pinnedPage.pop();
                }
            }
            else {
                //NonLeaf usage > occupancy/2. Do nothing
            }
            this->bufMgr->unPinPage(this->file, curPageNo, true);
            pinnedPage.pop();
        }
    }
    
    template<class T>
    const void BTreeIndex::deleteEntry_helper_leaf(T key, PageId curPageNo, NonLeafNode<T>* parentNode, 
            int keyIndexAtParent, std::vector<PageId>& disposePageNo, std::stack<PageId>& pinnedPage) {

        Page* curPage = NULL;
        this->bufMgr->readPage(this->file, curPageNo, curPage);
        pinnedPage.push(curPageNo);
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
                    this->bufMgr->readPage(this->file, node->leftSibPageNo, sibPage);
                    pinnedPage.push(node->leftSibPageNo);
                    LeafNode<T>* sibNode = (LeafNode<T>*)sibPage;

                    if(sibNode->usage > this->leafOccupancy/2) {
                        //Redistribute
                        printf("leaf redistribute with left sib\n");
                        int redistFromPos = sibNode->usage - 1;
                        RIDKeyPair<T> ridKeyPair = sibNode->ridKeyPairArray[redistFromPos];
                        sibNode->usage --;  //delete the redistributed entry

                        insertEntryInLeaf<T>(ridKeyPair.key, ridKeyPair.rid, node);

                        //Update the key of parent
//                        parentNode->pageKeyPairArray[keyIndexAtParent].key = ridKeyPair.key;
                        assignKey(parentNode->pageKeyPairArray[keyIndexAtParent].key, ridKeyPair.key);

                        //All Done
                    }
                    else {
                        //Merge into left sibling
                        printf("leaf merge with left sib\n");
                        for(int i = 0; i < node->usage; i ++) {
                            sibNode->ridKeyPairArray[sibNode->usage] = node->ridKeyPairArray[i];
                            sibNode->usage ++;
                        }

                        printf("keyIndexAtParent: %d\n", keyIndexAtParent);
                        //Delete key from parent
                        deleteEntryFromNonLeaf(keyIndexAtParent, parentNode);

                        //Set left and right sib pointers
                        sibNode->rightSibPageNo = node->rightSibPageNo;
                        if(node->rightSibPageNo != 0) {
                            Page* rightSibPage = NULL;
                            this->bufMgr->readPage(this->file, node->rightSibPageNo, rightSibPage);
                            LeafNode<T>* rightSibNode = (LeafNode<T>*)rightSibPage;
                            rightSibNode->leftSibPageNo = node->leftSibPageNo;
                            this->bufMgr->unPinPage(this->file, node->rightSibPageNo, true);
                        }

                        //Dispose curPage
                        disposePageNo.push_back(curPageNo);

                        //Return. Parent will handle its own redistribution/merging
                    }
                    this->bufMgr->unPinPage(this->file, node->leftSibPageNo, true);
                    pinnedPage.pop();
                }
                else {
                    //Special case: leftmost node must redistribute/merge with right sibling
                    Page* sibPage = NULL;
                    PageId sibPageNo = node->rightSibPageNo;
                    this->bufMgr->readPage(this->file, sibPageNo, sibPage);
                    pinnedPage.push(sibPageNo);
                    LeafNode<T>* sibNode = (LeafNode<T>*)sibPage;
                    if(sibNode->usage > this->leafOccupancy/2) {
                        //Redistribute
                        printf("special: leaf redistribute with right sib\n");
                        int redistFromPos = 0;
                        RIDKeyPair<T> ridKeyPair = sibNode->ridKeyPairArray[redistFromPos];
                        deleteEntryFromLeaf<T>(ridKeyPair.key, sibNode);

                        node->ridKeyPairArray[node->usage] = ridKeyPair;
                        node->usage ++;

                        //Update the key of parent
                        //Since curNode is the leftmost, its right sibling must have the same parent
                        //The position of key to be updated is the index of the key of curNode + 1
//                        parentNode->pageKeyPairArray[keyIndexAtParent+1].key = sibNode->ridKeyPairArray[0].key;
                        assignKey(parentNode->pageKeyPairArray[keyIndexAtParent+1].key, 
                                sibNode->ridKeyPairArray[0].key);

                        //All Done
                    }
                    else {
                        //Sibling merges into curNode
                        printf("special: leaf merge with right sib\n");
                        for(int i = 0; i < sibNode->usage; i ++) {
                            node->ridKeyPairArray[node->usage] = sibNode->ridKeyPairArray[i];
                            node->usage ++;
                        }
                        //Delete sibling's key from parent
                        //Note that sibling's key index must be at current key index + 1
                        deleteEntryFromNonLeaf<T>(keyIndexAtParent+1, parentNode);
                        
                        //Set left and right sib pointers
                        node->rightSibPageNo = sibNode->rightSibPageNo;

                        Page* rightSibSibPage = NULL;
                        PageId rightSibSibPageNo = sibNode->rightSibPageNo;
                        if(rightSibSibPageNo != 0) {
                            this->bufMgr->readPage(this->file, rightSibSibPageNo, rightSibSibPage);
                            LeafNode<T>* rightSibSibNode = (LeafNode<T>*)rightSibSibPage;
                            rightSibSibNode->leftSibPageNo = curPageNo;
                            this->bufMgr->unPinPage(this->file, rightSibSibPageNo, true);
                        }

                        //Dispose right sibling
                        disposePageNo.push_back(sibPageNo);

                        //Done with curNode. Parent will handle its own redistribution/merging
                    }
                    this->bufMgr->unPinPage(this->file, sibPageNo, true);
                    pinnedPage.pop();
                }
            }
            else {
                //Leaf usage > occupancy/2. Do nothing
            }
        }
        this->bufMgr->unPinPage(this->file, curPageNo, true);
        pinnedPage.pop();
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
            node->ridKeyPairArray[j] = node->ridKeyPairArray[j+1];
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
//                node->pageKeyPairArray[j].key = node->pageKeyPairArray[j+1].key;
                assignKey(node->pageKeyPairArray[j].key, node->pageKeyPairArray[j+1].key);
            }
            node->pageKeyPairArray[j+1].pageNo = node->pageKeyPairArray[j+2].pageNo;
        }
        node->usage --;
    }

}
