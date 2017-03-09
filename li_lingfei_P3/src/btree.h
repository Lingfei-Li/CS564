/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#pragma once

#include <iostream>
#include <string>
#include "string.h"
#include <sstream>
#include <vector>
#include <stack>
#include <algorithm>

#include "types.h"
#include "page.h"
#include "file.h"
#include "buffer.h"

namespace badgerdb
{

/**
 * @brief Datatype enumeration type.
 */
enum Datatype
{
	INTEGER = 0,
	DOUBLE = 1,
	STRING = 2
};

/**
 * @brief Scan operations enumeration. Passed to BTreeIndex::startScan() method.
 */
enum Operator
{ 
	LT, 	/* Less Than */
	LTE,	/* Less Than or Equal to */
	GTE,	/* Greater Than or Equal to */
	GT		/* Greater Than */
};

/**
 * @brief Size of String key.
 */
const  int STRINGSIZE = 10;


/* Assignment for structures*/
inline void assignKey( int& dst, int src) {
    dst = src;
}
inline void assignKey( double& dst, double src) {
    dst = src;
}
inline void assignKey( char dst[STRINGSIZE+1], char* src) {
    strncpy(dst, src, STRINGSIZE);
    dst[STRINGSIZE] = '\0';
}


/**
 * @brief Structure to store a key-rid pair. It is used to pass the pair to functions that 
 * add to or make changes to the leaf node pages of the tree. Is templated for the key member.
 */
template <class T>
class RIDKeyPair{
public:
	RecordId rid;
	T key;

	void set( RecordId r, T k)
	{
		rid = r;
        assignKey(key, k);
	}
};

template <>
class RIDKeyPair<char*>{
public:
	RecordId rid;
	char key[STRINGSIZE+1];     //Extra space for null terminator

	void set( RecordId r, char* k)
	{
		rid = r;
        assignKey(key, k);
	}
};

/**
 * @brief Structure to store a key page pair which is used to pass the key and page to functions that make 
 * any modifications to the non leaf pages of the tree.
*/
template <class T>
class PageKeyPair{
public:
	PageId pageNo;
	T key;

    PageKeyPair(): pageNo(0) { }
    PageKeyPair(PageId p): pageNo(p) { }

	void set( int p, T k)
	{
		pageNo = p;
        assignKey(key, k);
	}
};

template <>
class PageKeyPair<char*>{
public:
	PageId pageNo;
	char key[STRINGSIZE+1];

    PageKeyPair(): pageNo(0) { }
    PageKeyPair(PageId p): pageNo(p) { }

	void set( int p, char* k)
	{
		pageNo = p;
        assignKey(key, k);
	}
};




/**
 * @brief Overloaded operator to compare the key values of two rid-key pairs
 * and if they are the same compares to see if the first pair has
 * a smaller rid.pageNo value.
*/
template <class T>
bool operator<( const RIDKeyPair<T>& r1, const RIDKeyPair<T>& r2 )
{
	if( r1.key != r2.key )
		return r1.key < r2.key;
	else
		return r1.rid.page_number < r2.rid.page_number;
}



/**
 * @brief Number of key slots in B+Tree leaf for INTEGER key.
 */
//                                               sibling ptr      usage              key               rid
const  int INTARRAYLEAFSIZE = ( Page::SIZE - sizeof( PageId ) - sizeof(int) ) / ( sizeof( int ) + sizeof( RecordId ) );

/**
 * @brief Number of key slots in B+Tree leaf for DOUBLE key.
 */
//                                                 sibling ptr      usage              key               rid
const  int DOUBLEARRAYLEAFSIZE = ( Page::SIZE - sizeof( PageId ) - sizeof(int) ) / ( sizeof( double ) + sizeof( RecordId ) );

/**
 * @brief Number of key slots in B+Tree leaf for STRING key.
 */
//                                                 sibling ptr        usage                  key                   rid
const  int STRINGARRAYLEAFSIZE = ( Page::SIZE - sizeof( PageId ) - sizeof(int) ) / ( (STRINGSIZE+1) * sizeof(char) + sizeof( RecordId ) );

/**
 * @brief Number of key slots in B+Tree non-leaf for INTEGER key.
 */
//                                                     level     usage                pageKeyPair
const  int INTARRAYNONLEAFSIZE = ( Page::SIZE - sizeof( int ) - sizeof(int)) / ( sizeof(PageKeyPair<int>) ) - 1;    //-1 for the extra page ptr

/**
 * @brief Number of key slots in B+Tree leaf for DOUBLE key.
 */
//                                                        level       usage            PageKeyPair          -1 due to structure padding
const  int DOUBLEARRAYNONLEAFSIZE = (( Page::SIZE - sizeof( int ) -  sizeof(int)) / ( sizeof(PageKeyPair<double> ) )) - 2;

/**
 * @brief Number of key slots in B+Tree leaf for STRING key.
 */
//                                                        level        extra pageNo      usage            PageKeyPair
const  int STRINGARRAYNONLEAFSIZE = ( Page::SIZE - sizeof( int ) - sizeof( PageId ) - sizeof(int)) / ( (STRINGSIZE+1)*sizeof(char) + sizeof(PageId) ) - 1;


/**
 * @brief The meta page, which holds metadata for Index file, is always first page of the btree index file and is cast
 * to the following structure to store or retrieve information from it.
 * Contains the relation name for which the index is created, the byte offset
 * of the key value on which the index is made, the type of the key and the page no
 * of the root page. Root page starts as page 2 but since a split can occur
 * at the root the root page may get moved up and get a new page no.
*/
struct IndexMetaInfo{
  /**
   * Name of base relation.
   */
	char relationName[20];

  /**
   * Offset of attribute, over which index is built, inside the record stored in pages.
   */
	int attrByteOffset;

  /**
   * Type of the attribute over which index is built.
   */
	Datatype attrType;

  /**
   * Page number of root page of the B+ Tree inside the file index file.
   */
	PageId rootPageNo;

  /**
   * Height of the B+-tree
   */
	int height;
};

/*
Each node is a page, so once we read the page in we just cast the pointer to the page to this struct and use it to access the parts
These structures basically are the format in which the information is stored in the pages for the index file depending on what kind of 
node they are. The level memeber of each non leaf structure seen below is set to 1 if the nodes 
at this level are just above the leaf nodes. Otherwise set to 0.
*/

template<typename T>
struct NonLeafNode {
    //                                                   usage                  PageKeyPair 
    const static int ARRAYNONLEAFSIZE = ( Page::SIZE - sizeof(int)) / ( sizeof( PageKeyPair<T>) );
    //Note: this->nodeOccupancy will be smaller than this size

    int usage = 0;

    PageKeyPair<T> pageKeyPairArray[ARRAYNONLEAFSIZE];

};

template<typename T>
struct LeafNode {
    //                                                 sibling ptr        usage                  RIDKeyPair
    const  static int ARRAYLEAFSIZE = ( Page::SIZE - 2*sizeof( PageId ) - sizeof(int) ) / ( sizeof(RIDKeyPair<T>) );

    int usage = 0;

    RIDKeyPair<T> ridKeyPairArray[ARRAYLEAFSIZE];

    PageId rightSibPageNo;

    //TODO: delete left sib
    PageId leftSibPageNo;

};


/**
 * @brief BTreeIndex class. It implements a B+ Tree index on a single attribute of a
 * relation. This index supports only one scan at a time.
*/
class BTreeIndex {

 private:

  /**
   * File object for the index file.
   */
	File		*file;

  /**
   * Buffer Manager Instance.
   */
	BufMgr	*bufMgr;

  /**
   * Page number of meta page.
   */
	PageId	headerPageNum;

  /**
   * page number of root page of B+ tree inside index file.
   */
	PageId	rootPageNum;

  /**
   * Datatype of attribute over which index is built.
   */
	Datatype	attributeType;

  /**
   * Offset of attribute, over which index is built, inside records. 
   */
	int 		attrByteOffset;

  /**
   * Number of keys in leaf node, depending upon the type of key.
   */
	int		leafOccupancy;

  /**
   * Number of keys in non-leaf node, depending upon the type of key.
   */
	int		nodeOccupancy;

  /**
   * Height of the B+-tree
   */
	int height;


	// MEMBERS SPECIFIC TO SCANNING

  /**
   * True if an index scan has been started.
   */
	bool		scanExecuting;

  /**
   * Index of next entry to be scanned in current leaf being scanned.
   */
	int		nextEntry;

  /**
   * Page number of starting page being scanned.
   */
    std::vector<PageId>	scannedPageNum;

  /**
   * Page number of current page being scanned.
   */
	PageId	currentPageNum;

  /**
   * Current Page being scanned.
   */
	Page		*currentPageData;

  /**
   * Low INTEGER value for scan.
   */
	int		lowValInt;

  /**
   * Low DOUBLE value for scan.
   */
	double	lowValDouble;

  /**
   * Low STRING value for scan.
   */
	std::string	lowValString;

  /**
   * High INTEGER value for scan.
   */
	int		highValInt;

  /**
   * High DOUBLE value for scan.
   */
	double	highValDouble;

  /**
   * High STRING value for scan.
   */
	std::string highValString;
	
  /**
   * Low Operator. Can only be GT(>) or GTE(>=).
   */
	Operator	lowOp;

  /**
   * High Operator. Can only be LT(<) or LTE(<=).
   */
	Operator	highOp;

	
 public:

	BTreeIndex(const std::string & relationName, std::string & outIndexName,
						BufMgr *bufMgrIn,	const int attrByteOffset,	const Datatype attrType);

  /**
   * BTreeIndex Destructor. 
	 * End any initialized scan, flush index file, after unpinning any pinned pages, from the buffer manager
	 * and delete file instance thereby closing the index file.
	 * Destructor should not throw any exceptions. All exceptions should be caught in here itself. 
	 * */
	~BTreeIndex();


    template<class T>
	const void createNewRoot(PageKeyPair<T>& ret);

	const void* insertEntry1(const void* key, const RecordId rid, PageId curPageNo, int level);

	const void startScan(const void* lowVal, const Operator lowOp, const void* highVal, const Operator highOp);

	const void scanNext(RecordId& outRid);  // returned record id

	const void endScan();

	const void printMeta();

    const void dumpAllLevels();

    const void dumpLevel(int dumpLevel);

    template<class T>
    const void dumpLevel1(PageId curPageNo, int curLevel, int dumpLevel);

    template<char*>
    const void dumpLevel1(PageId curPageNo, int curLevel, int dumpLevel);

    template<class T>
	const void dumpLeaf();

    template<char*>
	const void dumpLeaf();

    /* Comparator */
    /* Relativity comparisons */
    const bool smallerThan(int lhs, int rhs) { return lhs < rhs; }
    const bool smallerThan(double lhs, double rhs) { return lhs < rhs; }
    const bool smallerThan(char* lhs, char* rhs) { return strncmp(lhs, rhs, STRINGSIZE) == -1; }

    /* Equality comparisons */
    const bool equals(int lhs, int rhs) { return lhs == rhs; }
    const bool equals(double lhs, double rhs) { return fabs(lhs-rhs) < 0.00001; }
    const bool equals(char* lhs, char* rhs) { return strncmp(lhs, rhs, 10) == 0; }

    /* Insertion. Template functions are implemented in btree.insertion.hpp */
	const void insertEntry(const void* key, const RecordId rid);

    template<class T>
	const void insertEntry_template(T key, const RecordId rid);

    template<class T>
	const PageKeyPair<T> insertEntry_helper(T key, const RecordId rid, PageId curPageNo, int level);

    template<class T>
    const void insertEntryInLeaf(T key, const RecordId rid, LeafNode<T>* node);

    template<class T>
    const void insertEntryInNonLeaf(T key, const PageId pageNo, NonLeafNode<T>* node);

    /* Deletion. Template functions are implemented in btree.delete.hpp */
    class DeletionKeyNotFoundException{ };

    const bool deleteEntry(const void *key);

    template<class T>
    const void deleteEntry_helper(T key, PageId curPageNo, NonLeafNode<T>* parentNode, 
            int keyIndexAtParent, int level, std::vector<PageId>& disposePageNo, std::stack<PageId>& pinnedPage);

    template<class T>
    const void deleteEntry_helper_leaf(T key, PageId curPageNo, NonLeafNode<T>* parentNode, 
            int keyIndexAtParent, std::vector<PageId>& disposePageNo, std::stack<PageId>& pinnedPage);

    template<class T>
    const bool deleteEntryFromLeaf(T key, LeafNode<T>* node);

    template<class T>
    const void deleteEntryFromNonLeaf(const int keyIndex, NonLeafNode<T>* node);


    /* B+Tree structure Validator. Template functions are implemented in btree.validate.hpp */
    class ValidationFailedException{ };

    const bool validate(bool showInfo);

    template<class T>
    const void validate_helper(PageId curPageNo, int level, std::stack<PageId>& pinnedPage);

    template<class T>
    const void validate_helper_leaf(PageId curPageNo, std::stack<PageId>& pinnedPage);

};

}

#include "btree.insert.hpp"
#include "btree.delete.hpp"
#include "btree.validate.hpp"
#include "btree.comparator.hpp"
