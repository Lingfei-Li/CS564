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
    const void BTreeIndex::validate_helper(PageId curPageNo, int level, std::stack<PageId>& pinnedPage) {
        if(level == this->height) {
            validate_helper_leaf<T>(curPageNo, pinnedPage);
        }
        else {
            Page* curPage = NULL;
            this->bufMgr->readPage(this->file, curPageNo, curPage);
            pinnedPage.push(curPageNo);
            NonLeafNode<T>* node = (NonLeafNode<T>*)curPage;

            if((level != 0) && (node->usage < this->nodeOccupancy/2-1 || node->usage > this->nodeOccupancy)) {
                printf("Internal Page #%d usage invalid\n", curPageNo);
                printf("Usage: %d, nodeOccupancy: %d\n", node->usage, this->nodeOccupancy);
                throw ValidationFailedException();
            }

            //Validation
            for(int i = 0; i <= node->usage; i ++) {
                PageId childPageNo = node->pageKeyPairArray[i].pageNo;
                if(childPageNo == 0) {
                    printf("Page #%d pageNo index #%d pageNo is 0\n", curPageNo, i);
                    throw ValidationFailedException();
                }

                Page* childPage = NULL;
                this->bufMgr->readPage(this->file, childPageNo, childPage);
                pinnedPage.push(childPageNo);

                T lowKey, highKey;
                if(level == this->height - 1) {
                    LeafNode<T>* childNode = (LeafNode<T>*)childPage;

                    if(childNode->usage < this->leafOccupancy/2 || childNode->usage > this->leafOccupancy) {
                        printf("Leaf Page #%d usage invalid\n", childPageNo);
                        printf("Usage: %d, leafOccupancy: %d\n", node->usage, this->leafOccupancy);
                        throw ValidationFailedException();
                    }

                    lowKey = childNode->ridKeyPairArray[0].key;
                    highKey = childNode->ridKeyPairArray[childNode->usage-1].key;
                }
                else {
                    NonLeafNode<T>* childNode = (NonLeafNode<T>*)childPage;
                
                    //if occupancy is 6, then the resulting two nodes will both have usage of 2. Hence -1 is needed
                    if(childNode->usage < this->nodeOccupancy/2-1 || childNode->usage > this->nodeOccupancy) {
                        printf("Internal Page #%d usage invalid\n", childPageNo);
                        printf("Usage: %d, nodeOccupancy: %d\n", childNode->usage, this->nodeOccupancy);
                        throw ValidationFailedException();
                    }

                    lowKey = childNode->pageKeyPairArray[0].key;
                    highKey = childNode->pageKeyPairArray[childNode->usage-1].key;
                    if(lowKey > highKey) {
                        printf("Page #%d lowKey > highKey\n", childPageNo);
                        throw ValidationFailedException();
                    }
                }

                if(i != node->usage && !smallerThan(highKey, node->pageKeyPairArray[i].key)) {
                    printf("Child Page #%d highKey >= parent rhs key\n", childPageNo);
                    std::cout<<"highKey: "<<highKey<<", parent rhs key: "<<node->pageKeyPairArray[i].key<<"\n";
                    throw ValidationFailedException();
                }
                if(i != 0 && smallerThan(lowKey, node->pageKeyPairArray[i-1].key)) {
                    printf("Child Page #%d lowKey < parent lhs key\n", childPageNo);
                    std::cout<<"lowKey: "<<lowKey<<", parent lhs key: "<<node->pageKeyPairArray[i-1].key<<"\n";
                    throw ValidationFailedException();
                }
                this->bufMgr->unPinPage(this->file, childPageNo, false);
                pinnedPage.pop();


                //Recursively validate child node
                validate_helper<T>(childPageNo, level+1, pinnedPage);
            }

            //Validation for this node completed
            this->bufMgr->unPinPage(this->file, curPageNo, false);
            pinnedPage.pop();
        }
    }

    template<class T>
    const void BTreeIndex::validate_helper_leaf(PageId curPageNo, std::stack<PageId>& pinnedPage) {
        Page* curPage = NULL;
        this->bufMgr->readPage(this->file, curPageNo, curPage);
        pinnedPage.push(curPageNo);
        LeafNode<T>* node = (LeafNode<T>*)curPage;

        if((this->height != 0) && (node->usage < this->leafOccupancy/2 || node->usage > this->leafOccupancy)) {
            printf("Leaf Page #%d usage invalid\n", curPageNo);
            printf("Usage: %d, leafOccupancy: %d\n", node->usage, this->leafOccupancy);
            throw ValidationFailedException();
        }

        for(int i = 1; i < node->usage; i ++) {
            if(smallerThan(node->ridKeyPairArray[i].key, node->ridKeyPairArray[i-1].key)) {
                printf("Leaf Page #%d invalid key order\n", curPageNo);
                throw ValidationFailedException();
            }
        }

        this->bufMgr->unPinPage(this->file, curPageNo, false);
        pinnedPage.pop();
    }
}
