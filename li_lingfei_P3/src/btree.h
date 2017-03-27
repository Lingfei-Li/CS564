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
#include <set>
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

	void set( int p, T k)
	{
		pageNo = p;
        assignKey(key, k);
	}
};

//Specialized template for string datatype
template <>
class PageKeyPair<char*>{
public:
	PageId pageNo;
	char key[STRINGSIZE+1];

    PageKeyPair(): pageNo(0) { }

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
//                                               sibling ptr      usage              RIDKeyPair
const  int INTARRAYLEAFSIZE = ( Page::SIZE - sizeof( PageId ) - sizeof(int) ) / ( sizeof(RIDKeyPair<int>) );

/**
 * @brief Number of key slots in B+Tree leaf for DOUBLE key.
 */
//                                                 sibling ptr      usage              RIDKeyPair
const  int DOUBLEARRAYLEAFSIZE = ( Page::SIZE - sizeof( PageId ) - sizeof(int) ) / ( sizeof(RIDKeyPair<double>) );

/**
 * @brief Number of key slots in B+Tree leaf for STRING key.
 */
//                                                 sibling ptr        usage                  RIDKeyPair
const  int STRINGARRAYLEAFSIZE = ( Page::SIZE - sizeof( PageId ) - sizeof(int) ) / ( sizeof(RIDKeyPair<char*>) );

/**
 * @brief Number of key slots in B+Tree non-leaf for INTEGER key.
 */
//                                                 usage                pageKeyPair
const  int INTARRAYNONLEAFSIZE = ( Page::SIZE -  sizeof(int)) / ( sizeof(PageKeyPair<int>) ) - 1;    //-1 for the extra page ptr

/**
 * @brief Number of key slots in B+Tree leaf for DOUBLE key.
 */
//                                                     usage            PageKeyPair          -2 due to structure padding and extra page ptr
const  int DOUBLEARRAYNONLEAFSIZE = ( Page::SIZE - sizeof(int)) / ( sizeof(PageKeyPair<double> ) ) - 2;

/**
 * @brief Number of key slots in B+Tree leaf for STRING key.
 */
//                                                   usage            PageKeyPair                          -1 for the extra page ptr
const  int STRINGARRAYNONLEAFSIZE = ( Page::SIZE - sizeof(int)) / ( sizeof(PageKeyPair<char*>)) - 1;


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

/**
 * @brief Structure for all non-leaf nodes
*/
template<typename T>
struct NonLeafNode {
    //                                                   usage                  PageKeyPair 
    const static int ARRAYNONLEAFSIZE = ( Page::SIZE - sizeof(int)) / ( sizeof( PageKeyPair<T>) );
    //Note: this is one pair larger than this->nodeOccupancy to incoporate the extra page ptr

    int usage = 0;

    PageKeyPair<T> pageKeyPairArray[ARRAYNONLEAFSIZE];

};

/**
 * @brief Structure for all leaf nodes
 */
template<typename T>
struct LeafNode {
    //                                                sibling ptr        usage                  RIDKeyPair
    const static int ARRAYLEAFSIZE = ( Page::SIZE - sizeof( PageId ) - sizeof(int) ) / ( sizeof(RIDKeyPair<T>) );

    int usage = 0;

    RIDKeyPair<T> ridKeyPairArray[ARRAYLEAFSIZE];

    PageId rightSibPageNo;
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
	char lowValString[15];

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
	char highValString[15];
	
  /**
   * Low Operator. Can only be GT(>) or GTE(>=).
   */
	Operator	lowOp;

  /**
   * High Operator. Can only be LT(<) or LTE(<=).
   */
	Operator	highOp;
	
	// -----------------------------------------------------------------------------
    // Private functions
    // -----------------------------------------------------------------------------

    /* Relativity comparisons */
    const bool smallerThan(int lhs, int rhs) { 
        return lhs < rhs; 
    }
    const bool smallerThan(double lhs, double rhs) { 
        return !equals(lhs, rhs) && (lhs < rhs);
    }
    const bool smallerThan(char* lhs, char* rhs) { 
        return strncmp(lhs, rhs, STRINGSIZE) < 0; 
    }

    /* Equality comparisons */
    const bool equals(int lhs, int rhs) { 
        return lhs == rhs; 
    }
    const bool equals(double lhs, double rhs) { 
        return fabs(lhs-rhs) < 0.00001; 
    }
    const bool equals(char* lhs, char* rhs) { 
        return strncmp(lhs, rhs, STRINGSIZE) == 0; 
    }

    template<class T>
    const bool smallerThanOrEquals(T lhs, T rhs) { 
        return smallerThan(lhs, rhs) || equals(lhs, rhs); 
    }

    /**
     * Create a new root with the copy-up entry if appropriate
     * */
    template<class T>
	const void createNewRoot(PageKeyPair<T>& ret);

    /**
     * Helper function for insertion. 
     * Returns the copy-up (or push-up) key.
     * */
    template<class T>
	const PageKeyPair<T> insertEntry_helper(T key, const RecordId rid, PageId curPageNo, int level);

    /**
     * Inserts the key and rid to the correct position in the given leaf node
     * */
    template<class T>
    const void insertEntryInLeaf(T key, const RecordId rid, LeafNode<T>* node);

    /**
     * Inserts the key and pageNo to the correct position in the given internal node
     * */
    template<class T>
    const void insertEntryInNonLeaf(T key, const PageId pageNo, NonLeafNode<T>* node);

    /**
     * Helper function for startScan
     * */
    template<class T>
	const void startScan_helper(T lowVal, const Operator lowOp, T highVal, const Operator highOp);

    /**
     * Helper function for scanNext
     * */
    template<class T>
    const void scanNext_helper(RecordId& outRid, T lowVal, T highVal);

    /**
     * Helper function for deletion.
     * */
    template<class T>
    const void deleteEntry_helper(T key, PageId curPageNo, NonLeafNode<T>* parentNode, 
            int keyIndexAtParent, int level, std::vector<PageId>& disposePageNo, std::set<PageId>& pinnedPage);

    /**
     * Helper function for deletion for leaf nodes. Separated from deleteEntry_helper to make the codes cleaner
     * */
    template<class T>
    const void deleteEntry_helper_leaf(T key, PageId curPageNo, NonLeafNode<T>* parentNode, 
            int keyIndexAtParent, std::vector<PageId>& disposePageNo, std::set<PageId>& pinnedPage);

    /**
     * Delete the entry from an leaf node
     * */
    template<class T>
    const bool deleteEntryFromLeaf(T key, LeafNode<T>* node);

    /**
     * Delete the entry from an internal node
     * */
    template<class T>
    const void deleteEntryFromNonLeaf(const int keyIndex, NonLeafNode<T>* node);

    /*
     * Tree structure validator helpers
     * */
    template<class T>
    const void validate_helper(PageId curPageNo, int level, std::set<PageId>& pinnedPage);

    template<class T>
    const void validate_helper_leaf(PageId curPageNo, std::set<PageId>& pinnedPage);

    /* 
     * Debugging functions
     * */
    template<class T>
    const void dumpLevel1(PageId curPageNo, int curLevel, int dumpLevel);

    template<char*>
    const void dumpLevel1(PageId curPageNo, int curLevel, int dumpLevel);

    template<class T>
	const void dumpLeaf();

    template<char*>
	const void dumpLeaf();

    const void dumpLevel(int dumpLevel);

	
 public:

    // -----------------------------------------------------------------------------
    // Constructor / Destructor and helpers
    // -----------------------------------------------------------------------------
	/**
   * BTreeIndex Constructor. 
	 * Check to see if the corresponding index file exists. If so, open the file.
	 * If not, create it and insert entries for every tuple in the base relation using FileScan class.
   *
   * @param relationName        Name of file.
   * @param outIndexName        Return the name of index file.
   * @param bufMgrIn						Buffer Manager Instance
   * @param attrByteOffset			Offset of attribute, over which index is to be built, in the record
   * @param attrType						Datatype of attribute over which index is built
   * @throws  BadIndexInfoException     If the index file already exists for the corresponding attribute, but values in metapage(relationName, attribute byte offset, attribute type etc.) do not match with values received through constructor parameters.
   */
	BTreeIndex(const std::string& relationName, std::string& outIndexName,
						BufMgr* bufMgrIn, const int attrByteOffset, const Datatype attrType);

    const void createIndexFromRelation(const std::string& relationName);

	/**
   * BTreeIndex Destructor. 
	 * End any initialized scan, flush index file, after unpinning any pinned pages, from the buffer manager
	 * and delete file instance thereby closing the index file.
	 * Destructor should not throw any exceptions. All exceptions should be caught in here itself. 
	 * */
	~BTreeIndex();


    /**
	 * Insert a new entry using the pair <value,rid>. 
	 * Start from root to recursively find out the leaf to insert the entry in. The insertion may cause splitting of leaf node.
	 * This splitting will require addition of new leaf page number entry into the parent non-leaf, which may in-turn get split.
	 * This may continue all the way upto the root causing the root to get split. If root gets split, metapage needs to be changed accordingly.
	 * Make sure to unpin pages as soon as you can.
   * @param key			Key to insert, pointer to integer/double/char string
   * @param rid			Record ID of a record whose entry is getting inserted into the index.
	**/
	const void insertEntry(const void* key, const RecordId rid);

	
	/**
	 * Begin a filtered scan of the index.  For instance, if the method is called 
	 * using ("a",GT,"d",LTE) then we should seek all entries with a value 
	 * greater than "a" and less than or equal to "d".
	 * If another scan is already executing, that needs to be ended here.
	 * Set up all the variables for scan. Start from root to find out the leaf page that contains the first RecordID
	 * that satisfies the scan parameters. Keep that page pinned in the buffer pool.
   * @param lowVal	Low value of range, pointer to integer / double / char string
   * @param lowOp		Low operator (GT/GTE)
   * @param highVal	High value of range, pointer to integer / double / char string
   * @param highOp	High operator (LT/LTE)
   * @throws  BadOpcodesException If lowOp and highOp do not contain one of their their expected values 
   * @throws  BadScanrangeException If lowVal > highval
	 * @throws  NoSuchKeyFoundException If there is no key in the B+ tree that satisfies the scan criteria.
	**/
	const void startScan(const void* lowVal, const Operator lowOp, const void* highVal, const Operator highOp);

	/**
	 * Fetch the record id of the next index entry that matches the scan.
	 * Return the next record from current page being scanned. If current page has been scanned to its entirety, move on to the right sibling of current page, if any exists, to start scanning that page. Make sure to unpin any pages that are no longer required.
   * @param outRid	RecordId of next record found that satisfies the scan criteria returned in this
	 * @throws ScanNotInitializedException If no scan has been initialized.
	 * @throws IndexScanCompletedException If no more records, satisfying the scan criteria, are left to be scanned.
	**/
	const void scanNext(RecordId& outRid);

	
	/**
	 * Terminate the current scan. Unpin any pinned pages. Reset scan specific variables.
	 * @throws ScanNotInitializedException If no scan has been initialized.
	**/
	const void endScan();

	/**
	 * Indicates that the key is not found in the index. Deletion is terminated
	**/
    class DeletionKeyNotFoundException{ };

	/**
	 * Delete the given key from the index.
	 * @param key the key to be deleted
	 * @throws DeletionKeyNotFoundException If the key is not present in the index
	**/
    const bool deleteEntry(const void *key);

    
	/**
	 * Indicates that the tree sturcture is not valid for B+-tree. Validation process is terminated
	**/
    class ValidationFailedException{ };
	
	/**
	 * Validate the tree structure. 
	 * @param showInfo If true, then the information about the tree will be printed during validation. If false, the validation will execute silently until it sees invalid structure.
	 * @throws ValidationFailedException If the tree structure is invalid
	**/
    const bool validate(bool showInfo);
	
	/**
	 * Return a boolean indicating if the tree has no entry in it
	**/
    const bool isEmpty();

    // -----------------------------------------------------------------------------
    // Debugging functions
    // -----------------------------------------------------------------------------
	/**
	 * Print the meta info
	**/
	const void printMeta();

	/**
	 * Print the given string only when DEBUG is set
	**/
    const int dprintf (const char *format, ...);

	/**
	 * Visualize the tree structure
	**/
    const void dumpAllLevels();

};

}


