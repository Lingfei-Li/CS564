/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <vector>
#include "btree.h"
#include "page.h"
#include "filescan.h"
#include "page_iterator.h"
#include "file_iterator.h"
#include "exceptions/insufficient_space_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/end_of_file_exception.h"

#define checkPassFail(a, b) 																				\
{																																		\
	if(a == b)																												\
		std::cout << "\nTest passed at line no:" << __LINE__ << "\n";		\
	else																															\
	{																																	\
		std::cout << "\nTest FAILS at line no:" << __LINE__;						\
		std::cout << "\nExpected no of records:" << b;									\
		std::cout << "\nActual no of records found:" << a;							\
		std::cout << std::endl;																					\
		exit(1);																												\
	}																																	\
}

using namespace badgerdb;

// -----------------------------------------------------------------------------
// Globals
// -----------------------------------------------------------------------------
int testNum = 1;
const std::string relationName = "relA";
//If the relation size is changed then the second parameter 2 chechPassFail may need to be changed to number of record that are expected to be found during the scan, else tests will erroneously be reported to have failed.
const int	relationSize = 5000;
std::string intIndexName, doubleIndexName, stringIndexName;

// This is the structure for tuples in the base relation

typedef struct tuple {
	int i;
	double d;
	char s[64];
} RECORD;

PageFile* file1;
RecordId rid;
RECORD record1;
std::string dbRecord1;

std::vector<int> insertedKeysInt;
std::vector<double> insertedKeysDouble;
std::vector<std::string> insertedKeysStr;

BufMgr * bufMgr = new BufMgr(100);

// -----------------------------------------------------------------------------
// Forward declarations
// -----------------------------------------------------------------------------

void createRelationForward();
void createRelationBackward();
void createRelationRandom();
void createRelationForwardNegative();
void createRelationBackwardNegative();
void createRelationRandomNegative();
void intTests();
void intTestsNegative();
int intScan(BTreeIndex *index, int lowVal, Operator lowOp, int highVal, Operator highOp);
void indexTests();
void indexTestsNegative();
void doubleTests();
void doubleTestsNegative();
int doubleScan(BTreeIndex *index, double lowVal, Operator lowOp, double highVal, Operator highOp);
void stringTests();
int stringScan(BTreeIndex *index, int lowVal, Operator lowOp, int highVal, Operator highOp);
void test1();
void test2();
void test3();
void negtest1();
void negtest2();
void negtest3();
void errorTests();
void deleteRelation();
void checkDeletionPassFail(bool result, int line);
void checkDeletionPassFail1(bool result, int line, size_t index);

int main(int argc, char **argv)
{
	if( argc != 2 )
	{
		std::cout << "Expects one argument as a number between 1 to 3 to choose datatype of key.\n";
		std::cout << "For INTEGER keys run as: ./badgerdb_main 1\n";
		std::cout << "For DOUBLE keys run as: ./badgerdb_main 2\n";
		std::cout << "For STRING keys run as: ./badgerdb_main 3\n";
		delete bufMgr;
		return 1;
	}

	sscanf(argv[1],"%d",&testNum);

	switch(testNum)
	{
		case 1:
			std::cout << "leaf size:" << INTARRAYLEAFSIZE << " non-leaf size:" << INTARRAYNONLEAFSIZE << std::endl;
			break;
		case 2:
			std::cout << "leaf size:" << DOUBLEARRAYLEAFSIZE << " non-leaf size:" << DOUBLEARRAYNONLEAFSIZE << std::endl;
			break;
		case 3:
			std::cout << "leaf size:" << STRINGARRAYLEAFSIZE << " non-leaf size:" << STRINGARRAYNONLEAFSIZE << std::endl;
			break;
	}


  // Clean up from any previous runs that crashed.
  try
	{
    File::remove(relationName);
  }
	catch(FileNotFoundException)
	{
  }

	{
		// Create a new database file.
		PageFile new_file = PageFile::create(relationName);

		// Allocate some pages and put data on them.
		for (int i = 0; i < 20; ++i)
		{
			PageId new_page_number;
			Page new_page = new_file.allocatePage(new_page_number);

    	sprintf(record1.s, "%05d string record", i);
    	record1.i = i;
    	record1.d = (double)i;
    	std::string new_data(reinterpret_cast<char*>(&record1), sizeof(record1));

			new_page.insertRecord(new_data);
			new_file.writePage(new_page_number, new_page);
		}

	}
	// new_file goes out of scope here, so file is automatically closed.

	{
		FileScan fscan(relationName, bufMgr);

		try
		{
			RecordId scanRid;
			while(1)
			{
				fscan.scanNext(scanRid);
				//Assuming RECORD.i is our key, lets extract the key, which we know is INTEGER and whose byte offset is also know inside the record. 
				std::string recordStr = fscan.getRecord();
				const char *record = recordStr.c_str();
				int key = *((int *)(record + offsetof (RECORD, i)));
				std::cout << "Extracted : " << key << std::endl;
			}
		}
		catch(EndOfFileException e)
		{
			std::cout << "Read all records" << std::endl;
		}
	}
	// filescan goes out of scope here, so relation file gets closed.

	File::remove(relationName);

	test1();
	test2();
	test3();
	negtest1();
	negtest2();
	negtest3();
	errorTests();

	delete bufMgr;
	return 0;
}

void test1()
{
	// Create a relation with tuples valued 0 to relationSize and perform index tests 
	// on attributes of all three types (int, double, string)
	std::cout << "---------------------" << std::endl;
	std::cout << "createRelationForward" << std::endl;
	createRelationForward();
	indexTests();
	deleteRelation();
}

void test2()
{
	// Create a relation with tuples valued 0 to relationSize in reverse order and perform index tests 
	// on attributes of all three types (int, double, string)
	std::cout << "----------------------" << std::endl;
	std::cout << "createRelationBackward" << std::endl;
	createRelationBackward();
	indexTests();
	deleteRelation();
}

void test3()
{
	// Create a relation with tuples valued 0 to relationSize in random order and perform index tests 
	// on attributes of all three types (int, double, string)
	std::cout << "--------------------" << std::endl;
	std::cout << "createRelationRandom" << std::endl;
	createRelationRandom();
	indexTests();
	deleteRelation();
}

void negtest1()
{
	// Create a relation with tuples valued -relationSize to relationSize and perform index tests 
	// on attributes of all three types (int, double, string)
	std::cout << "--------------------" << std::endl;
	std::cout << "createRelationForwardNegative" << std::endl;
	createRelationForwardNegative();
    indexTestsNegative();
	deleteRelation();
}

void negtest2()
{
	// Create a relation with tuples valued -relationSize to relationSize in reversed order and perform index tests 
	// on attributes of all three types (int, double, string)
	std::cout << "--------------------" << std::endl;
	std::cout << "createRelationBackwardNegative" << std::endl;
	createRelationBackwardNegative();
    indexTestsNegative();
	deleteRelation();
}
void negtest3()
{
	// Create a relation with tuples valued -relationSize to relationSize in random order and perform index tests 
	// on attributes of all three types (int, double, string)
	std::cout << "--------------------" << std::endl;
	std::cout << "createRelationRandomNegative" << std::endl;
	createRelationRandomNegative();
    indexTestsNegative();
	deleteRelation();
}
// -----------------------------------------------------------------------------
// createRelationForward
// -----------------------------------------------------------------------------

void createRelationForward()
{
	std::vector<RecordId> ridVec;
  // destroy any old copies of relation file
	try
	{
		File::remove(relationName);
	}
	catch(FileNotFoundException e)
	{
	}

  file1 = new PageFile(relationName, true);

  // initialize all of record1.s to keep purify happy
  memset(record1.s, ' ', sizeof(record1.s));
	PageId new_page_number;
  Page new_page = file1->allocatePage(new_page_number);

  insertedKeysInt.resize(0);
  insertedKeysDouble.resize(0);
  insertedKeysStr.resize(0);

  // Insert a bunch of tuples into the relation.
  for(int i = 0; i < relationSize; i++ )
	{
    sprintf(record1.s, "%05d string record", i);
    record1.i = i;
    record1.d = (double)i;
    std::string new_data(reinterpret_cast<char*>(&record1), sizeof(record1));

    insertedKeysInt.push_back(record1.i);
    insertedKeysDouble.push_back(record1.d);
    insertedKeysStr.push_back(record1.s);

		while(1)
		{
			try
			{
    		new_page.insertRecord(new_data);
				break;
			}
			catch(InsufficientSpaceException e)
			{
				file1->writePage(new_page_number, new_page);
  			new_page = file1->allocatePage(new_page_number);
			}
		}
  }

	file1->writePage(new_page_number, new_page);
}

void createRelationForwardNegative()
{
	std::vector<RecordId> ridVec;
  // destroy any old copies of relation file
	try
	{
		File::remove(relationName);
	}
	catch(FileNotFoundException e)
	{
	}

  file1 = new PageFile(relationName, true);

  // initialize all of record1.s to keep purify happy
  memset(record1.s, ' ', sizeof(record1.s));
	PageId new_page_number;
  Page new_page = file1->allocatePage(new_page_number);

  insertedKeysInt.resize(0);
  insertedKeysDouble.resize(0);
  insertedKeysStr.resize(0);

  // Insert a bunch of tuples into the relation.
  for(int i = 0; i < relationSize*2; i++ )
	{
    sprintf(record1.s, "%05d string record", i-relationSize);
    record1.i = i-relationSize;
    record1.d = (double)(i-relationSize);
    std::string new_data(reinterpret_cast<char*>(&record1), sizeof(record1));

    insertedKeysInt.push_back(record1.i);
    insertedKeysDouble.push_back(record1.d);
    insertedKeysStr.push_back(record1.s);

		while(1)
		{
			try
			{
    		new_page.insertRecord(new_data);
				break;
			}
			catch(InsufficientSpaceException e)
			{
				file1->writePage(new_page_number, new_page);
  			new_page = file1->allocatePage(new_page_number);
			}
		}
  }

	file1->writePage(new_page_number, new_page);
}

// -----------------------------------------------------------------------------
// createRelationBackward
// -----------------------------------------------------------------------------


void createRelationBackward()
{
  // destroy any old copies of relation file
	try
	{
		File::remove(relationName);
	}
	catch(FileNotFoundException e)
	{
	}
  file1 = new PageFile(relationName, true);

  // initialize all of record1.s to keep purify happy
  memset(record1.s, ' ', sizeof(record1.s));
	PageId new_page_number;
  Page new_page = file1->allocatePage(new_page_number);

  insertedKeysInt.resize(0);
  insertedKeysDouble.resize(0);
  insertedKeysStr.resize(0);

  // Insert a bunch of tuples into the relation.
  for(int i = relationSize - 1; i >= 0; i-- )
	{
    sprintf(record1.s, "%05d string record", i);
    record1.i = i;
    record1.d = (double)i;

    insertedKeysInt.push_back(record1.i);
    insertedKeysDouble.push_back(record1.d);
    insertedKeysStr.push_back(record1.s);

    std::string new_data(reinterpret_cast<char*>(&record1), sizeof(RECORD));

		while(1)
		{
			try
			{
    		new_page.insertRecord(new_data);
				break;
			}
			catch(InsufficientSpaceException e)
			{
				file1->writePage(new_page_number, new_page);
  			new_page = file1->allocatePage(new_page_number);
			}
		}
  }

	file1->writePage(new_page_number, new_page);
}
void createRelationBackwardNegative()
{
  // destroy any old copies of relation file
	try
	{
		File::remove(relationName);
	}
	catch(FileNotFoundException e)
	{
	}
  file1 = new PageFile(relationName, true);

  // initialize all of record1.s to keep purify happy
  memset(record1.s, ' ', sizeof(record1.s));
	PageId new_page_number;
  Page new_page = file1->allocatePage(new_page_number);

  insertedKeysInt.resize(0);
  insertedKeysDouble.resize(0);
  insertedKeysStr.resize(0);

  // Insert a bunch of tuples into the relation.
  for(int i = relationSize*2 - 1; i >= 0; i-- )
	{
    sprintf(record1.s, "%05d string record", i-relationSize);
    record1.i = i-relationSize;
    record1.d = (double)(i-relationSize);

    insertedKeysInt.push_back(record1.i);
    insertedKeysDouble.push_back(record1.d);
    insertedKeysStr.push_back(record1.s);

    std::string new_data(reinterpret_cast<char*>(&record1), sizeof(RECORD));

		while(1)
		{
			try
			{
    		new_page.insertRecord(new_data);
				break;
			}
			catch(InsufficientSpaceException e)
			{
				file1->writePage(new_page_number, new_page);
  			new_page = file1->allocatePage(new_page_number);
			}
		}
  }

	file1->writePage(new_page_number, new_page);
}
// -----------------------------------------------------------------------------
// createRelationRandom
// -----------------------------------------------------------------------------

void createRelationRandom()
{
  // destroy any old copies of relation file
	try
	{
		File::remove(relationName);
	}
	catch(FileNotFoundException e)
	{
	}
  file1 = new PageFile(relationName, true);

  // initialize all of record1.s to keep purify happy
  memset(record1.s, ' ', sizeof(record1.s));
	PageId new_page_number;
  Page new_page = file1->allocatePage(new_page_number);

  insertedKeysInt.resize(0);
  insertedKeysDouble.resize(0);
  insertedKeysStr.resize(0);

  // insert records in random order

  std::vector<int> intvec(relationSize);
  for( int i = 0; i < relationSize; i++ )
  {
    intvec[i] = i;
  }

  long pos;
  int val;
	int i = 0;
  while( i < relationSize )
  {
    pos = random() % (relationSize-i);
    val = intvec[pos];
    sprintf(record1.s, "%05d string record", val);
    record1.i = val;
    record1.d = val;

    insertedKeysInt.push_back(record1.i);
    insertedKeysDouble.push_back(record1.d);
    insertedKeysStr.push_back(record1.s);

    std::string new_data(reinterpret_cast<char*>(&record1), sizeof(RECORD));

		while(1)
		{
			try
			{
    		new_page.insertRecord(new_data);
				break;
			}
			catch(InsufficientSpaceException e)
			{
      	file1->writePage(new_page_number, new_page);
  			new_page = file1->allocatePage(new_page_number);
			}
		}

		int temp = intvec[relationSize-1-i];
		intvec[relationSize-1-i] = intvec[pos];
		intvec[pos] = temp;
		i++;
  }
  
	file1->writePage(new_page_number, new_page);
}

// -----------------------------------------------------------------------------
// createRelationRandomNegative
// -----------------------------------------------------------------------------

void createRelationRandomNegative()
{
  // destroy any old copies of relation file
	try
	{
		File::remove(relationName);
	}
	catch(FileNotFoundException e)
	{
	}
  file1 = new PageFile(relationName, true);

  // initialize all of record1.s to keep purify happy
  memset(record1.s, ' ', sizeof(record1.s));
	PageId new_page_number;
  Page new_page = file1->allocatePage(new_page_number);

  insertedKeysInt.resize(0);
  insertedKeysDouble.resize(0);
  insertedKeysStr.resize(0);

  // insert records in random order

  std::vector<int> intvec(relationSize*2);
  for( int i = 0; i < relationSize*2; i++ )
  {
    intvec[i] = i-relationSize;
  }

  long pos;
  int val;
	int i = 0;
  while( i < relationSize*2 )
  {
    pos = random() % (relationSize*2-i);
    val = intvec[pos];
    sprintf(record1.s, "%05d string record", val);
    record1.i = val;
    record1.d = val;

    insertedKeysInt.push_back(record1.i);
    insertedKeysDouble.push_back(record1.d);
    insertedKeysStr.push_back(record1.s);

    std::string new_data(reinterpret_cast<char*>(&record1), sizeof(RECORD));

		while(1)
		{
			try
			{
    		new_page.insertRecord(new_data);
				break;
			}
			catch(InsufficientSpaceException e)
			{
      	file1->writePage(new_page_number, new_page);
  			new_page = file1->allocatePage(new_page_number);
			}
		}

		int temp = intvec[relationSize*2-1-i];
		intvec[relationSize*2-1-i] = intvec[pos];
		intvec[pos] = temp;
		i++;
  }
  
	file1->writePage(new_page_number, new_page);
}

// -----------------------------------------------------------------------------
// indexTests
// -----------------------------------------------------------------------------

void indexTests()
{
  if(testNum == 1)
  {
    intTests();
		try
		{
			File::remove(intIndexName);
		}
  	catch(FileNotFoundException e)
  	{
  	}
  }
  else if(testNum == 2)
  {
    doubleTests();
		try
		{
			File::remove(doubleIndexName);
		}
  	catch(FileNotFoundException e)
  	{
  	}
  }
  else if(testNum == 3)
  {
    stringTests();
		try
		{
			File::remove(stringIndexName);
		}
  	catch(FileNotFoundException e)
  	{
  	}
  }
  else {

    intTests();
		try
		{
			File::remove(intIndexName);
		}
  	catch(FileNotFoundException e)
  	{
  	}
    doubleTests();
		try
		{
			File::remove(doubleIndexName);
		}
  	catch(FileNotFoundException e)
  	{
  	}
    stringTests();
		try
		{
			File::remove(stringIndexName);
		}
  	catch(FileNotFoundException e)
  	{
  	}
  
  }
}
void indexTestsNegative()
{
  if(testNum == 1)
  {
    intTestsNegative();
		try
		{
			File::remove(intIndexName);
		}
  	catch(FileNotFoundException e)
  	{
  	}
  }
  else if(testNum == 2)
  {
    doubleTestsNegative();
		try
		{
			File::remove(doubleIndexName);
		}
  	catch(FileNotFoundException e)
  	{
  	}
  }
  else {
    intTestsNegative();
		try
		{
			File::remove(intIndexName);
		}
  	catch(FileNotFoundException e)
  	{
  	}
  
    doubleTestsNegative();
		try
		{
			File::remove(doubleIndexName);
		}
  	catch(FileNotFoundException e)
  	{
  	}
  
  }
}

// -----------------------------------------------------------------------------
// intTests
// -----------------------------------------------------------------------------

void intTests()
{
  std::cout << "Create a B+ Tree index on the integer field" << std::endl;
  BTreeIndex index(relationName, intIndexName, bufMgr, offsetof(tuple,i), INTEGER);

	// run some tests
	checkPassFail(intScan(&index,25,GT,40,LT), 14)
	checkPassFail(intScan(&index,20,GTE,35,LTE), 16)
	checkPassFail(intScan(&index,-3,GT,3,LT), 3)
	checkPassFail(intScan(&index,996,GT,1001,LT), 4)
	checkPassFail(intScan(&index,0,GT,1,LT), 0)
	checkPassFail(intScan(&index,300,GT,400,LT), 99)
	checkPassFail(intScan(&index,3000,GTE,4000,LT), 1000)


    for(size_t i = 0; i < insertedKeysInt.size(); i ++) {
        checkDeletionPassFail(index.deleteEntry((void*)&insertedKeysInt[i]), __LINE__);
    }

    if(index.isEmpty() == false) {
        std::cout << "\nDeletion failed at line no:" << __LINE__ << "\n";
        std::cout << "\nEntries left in the index" << __LINE__ << "\n";
        exit(1);
    }
    std::cout << "\nDeletion passed at line no:" << __LINE__ << "\n";

}

void intTestsNegative()
{
  std::cout << "Create a B+ Tree index on the integer field" << std::endl;
  BTreeIndex index(relationName, intIndexName, bufMgr, offsetof(tuple,i), INTEGER);

	// run some tests
	checkPassFail(intScan(&index,25,GT,40,LT), 14)
	checkPassFail(intScan(&index,20,GTE,35,LTE), 16)
	checkPassFail(intScan(&index,996,GT,1001,LT), 4)
	checkPassFail(intScan(&index,0,GT,1,LT), 0)
	checkPassFail(intScan(&index,300,GT,400,LT), 99)
	checkPassFail(intScan(&index,3000,GTE,4000,LT), 1000)

	checkPassFail(intScan(&index,-40,GT,-25,LT), 14)
	checkPassFail(intScan(&index,-35,GTE,-20,LTE), 16)
	checkPassFail(intScan(&index,-1001,GT,-996,LT), 4)
	checkPassFail(intScan(&index,-1,GT,0,LT), 0)
	checkPassFail(intScan(&index,-400,GT,-300,LT), 99)
	checkPassFail(intScan(&index,-4000,GTE,-3000,LT), 1000)

	checkPassFail(intScan(&index,-3,GT,3,LT), 5)
	checkPassFail(intScan(&index,-40,GT,25,LT), 64)
	checkPassFail(intScan(&index,-35,GTE,20,LTE), 56)
	checkPassFail(intScan(&index,-1001,GT,996,LT), 1996)
	checkPassFail(intScan(&index,-400,GT,300,LT), 699)
	checkPassFail(intScan(&index,-4000,GTE,3000,LT), 7000)

    for(size_t i = 0; i < insertedKeysInt.size(); i ++) {
        checkDeletionPassFail(index.deleteEntry((void*)&insertedKeysInt[i]), __LINE__);
    }

    if(index.isEmpty() == false) {
        std::cout << "\nDeletion failed at line no:" << __LINE__ << "\n";
        std::cout << "\nEntries left in the index" << __LINE__ << "\n";
        exit(1);
    }
    std::cout << "\nDeletion passed at line no:" << __LINE__ << "\n";
}


int intScan(BTreeIndex * index, int lowVal, Operator lowOp, int highVal, Operator highOp)
{
  RecordId scanRid;
	Page *curPage;

  std::cout << "Scan for ";
  if( lowOp == GT ) { std::cout << "("; } else { std::cout << "["; }
  std::cout << lowVal << "," << highVal;
  if( highOp == LT ) { std::cout << ")"; } else { std::cout << "]"; }
  std::cout << std::endl;

  int numResults = 0;
	
	try
	{
  	index->startScan(&lowVal, lowOp, &highVal, highOp);
	}
	catch(NoSuchKeyFoundException e)
	{
    std::cout << "No Key Found satisfying the scan criteria." << std::endl;
		return 0;
	}

	while(1)
	{
		try
		{
			index->scanNext(scanRid);
			bufMgr->readPage(file1, scanRid.page_number, curPage);
			RECORD myRec = *(reinterpret_cast<const RECORD*>(curPage->getRecord(scanRid).data()));
			bufMgr->unPinPage(file1, scanRid.page_number, false);

			if( numResults < 5 )
			{
				std::cout << "at:" << scanRid.page_number << "," << scanRid.slot_number;
				std::cout << " -->:" << myRec.i << ":" << myRec.d << ":" << myRec.s << ":" <<std::endl;
			}
			else if( numResults == 5 )
			{
				std::cout << "..." << std::endl;
			}
		}
		catch(IndexScanCompletedException e)
		{
			break;
		}

		numResults++;
	}

  if( numResults >= 5 )
  {
    std::cout << "Number of results: " << numResults << std::endl;
  }
  index->endScan();
  std::cout << std::endl;

	return numResults;
}

// -----------------------------------------------------------------------------
// doubleTests
// -----------------------------------------------------------------------------
void doubleTests()
{
  std::cout << "Create a B+ Tree index on the double field" << std::endl;
  BTreeIndex index(relationName, doubleIndexName, bufMgr, offsetof(tuple,d), DOUBLE);

	// run some tests
	checkPassFail(doubleScan(&index,25,GT,40,LT), 14)
	checkPassFail(doubleScan(&index,20,GTE,35,LTE), 16)
	checkPassFail(doubleScan(&index,-3,GT,3,LT), 3)
	checkPassFail(doubleScan(&index,996,GT,1001,LT), 4)
	checkPassFail(doubleScan(&index,0,GT,1,LT), 0)
	checkPassFail(doubleScan(&index,300,GT,400,LT), 99)
	checkPassFail(doubleScan(&index,3000,GTE,4000,LT), 1000)

    for(size_t i = 0; i < insertedKeysDouble.size(); i ++) {
        checkDeletionPassFail(index.deleteEntry((void*)&insertedKeysDouble[i]), __LINE__);
    }

    if(index.isEmpty() == false) {
        std::cout << "\nDeletion failed at line no:" << __LINE__ << "\n";
        std::cout << "\nEntries left in the index" << __LINE__ << "\n";
        exit(1);
    }
    std::cout << "\nDeletion passed at line no:" << __LINE__ << "\n";
}

void doubleTestsNegative()
{
  std::cout << "Create a B+ Tree index on the double field" << std::endl;
  BTreeIndex index(relationName, doubleIndexName, bufMgr, offsetof(tuple,d), DOUBLE);

	// run some tests
	checkPassFail(doubleScan(&index,25,GT,40,LT), 14)
	checkPassFail(doubleScan(&index,20,GTE,35,LTE), 16)
	checkPassFail(doubleScan(&index,996,GT,1001,LT), 4)
	checkPassFail(doubleScan(&index,0,GT,1,LT), 0)
	checkPassFail(doubleScan(&index,300,GT,400,LT), 99)
	checkPassFail(doubleScan(&index,3000,GTE,4000,LT), 1000)

	checkPassFail(doubleScan(&index,-40,GT,-25,LT), 14)
	checkPassFail(doubleScan(&index,-35,GTE,-20,LTE), 16)
	checkPassFail(doubleScan(&index,-1001,GT,-996,LT), 4)
	checkPassFail(doubleScan(&index,-1,GT,0,LT), 0)
	checkPassFail(doubleScan(&index,-400,GT,-300,LT), 99)
	checkPassFail(doubleScan(&index,-4000,GTE,-3000,LT), 1000)

	checkPassFail(doubleScan(&index,-3,GT,3,LT), 5)
	checkPassFail(doubleScan(&index,-40,GT,25,LT), 64)
	checkPassFail(doubleScan(&index,-35,GTE,20,LTE), 56)
	checkPassFail(doubleScan(&index,-1001,GT,996,LT), 1996)
	checkPassFail(doubleScan(&index,-400,GT,300,LT), 699)
	checkPassFail(doubleScan(&index,-4000,GTE,3000,LT), 7000)

    for(size_t i = 0; i < insertedKeysDouble.size(); i ++) {
        checkDeletionPassFail(index.deleteEntry((void*)&insertedKeysDouble[i]), __LINE__);
    }

    if(index.isEmpty() == false) {
        std::cout << "\nDeletion failed at line no:" << __LINE__ << "\n";
        std::cout << "\nEntries left in the index" << __LINE__ << "\n";
        exit(1);
    }
    std::cout << "\nDeletion passed at line no:" << __LINE__ << "\n";
}

int doubleScan(BTreeIndex * index, double lowVal, Operator lowOp, double highVal, Operator highOp)
{
  RecordId scanRid;
	Page *curPage;

  std::cout << "Scan for ";
  if( lowOp == GT ) { std::cout << "("; } else { std::cout << "["; }
  std::cout << lowVal << "," << highVal;
  if( highOp == LT ) { std::cout << ")"; } else { std::cout << "]"; }
  std::cout << std::endl;

  int numResults = 0;

	try
	{
  	index->startScan(&lowVal, lowOp, &highVal, highOp);
	}
	catch(NoSuchKeyFoundException e)
	{
    std::cout << "No Key Found satisfying the scan criteria." << std::endl;
		return 0;
	}

	while(1)
	{
		try
		{
			index->scanNext(scanRid);
			bufMgr->readPage(file1, scanRid.page_number, curPage);
			RECORD myRec = *(reinterpret_cast<const RECORD*>(curPage->getRecord(scanRid).data()));
			bufMgr->unPinPage(file1, scanRid.page_number, false);

			if( numResults < 5 )
			{
				std::cout << "rid:" << scanRid.page_number << "," << scanRid.slot_number;
				std::cout << " -->:" << myRec.i << ":" << myRec.d << ":" << myRec.s << ":" <<std::endl;
			}
			else if( numResults == 5 )
			{
				std::cout << "..." << std::endl;
			}
		}
		catch(IndexScanCompletedException e)
		{
			break;
		}

		numResults++;
	}

  if( numResults >= 5 )
  {
    std::cout << "Number of results: " << numResults << std::endl;
  }
  index->endScan();
  std::cout << std::endl;

	return numResults;
}

// -----------------------------------------------------------------------------
// stringTests
// -----------------------------------------------------------------------------
void stringTests()
{
  std::cout << "Create a B+ Tree index on the string field" << std::endl;
  BTreeIndex index(relationName, stringIndexName, bufMgr, offsetof(tuple,s), STRING);

	// run some tests
	checkPassFail(stringScan(&index,25,GT,40,LT), 14)
	checkPassFail(stringScan(&index,20,GTE,35,LTE), 16)
	checkPassFail(stringScan(&index,-3,GT,3,LT), 3)
	checkPassFail(stringScan(&index,996,GT,1001,LT), 4)
	checkPassFail(stringScan(&index,0,GT,1,LT), 0)
	checkPassFail(stringScan(&index,300,GT,400,LT), 99)
	checkPassFail(stringScan(&index,3000,GTE,4000,LT), 1000)

    for(size_t i = 0; i < insertedKeysStr.size(); i ++) {
        char key[20];
        sprintf(key, "%05d string record", (int)i);
        checkDeletionPassFail1(index.deleteEntry((void*)key), __LINE__, i);
    }

    if(index.isEmpty() == false) {
        std::cout << "\nDeletion failed at line no:" << __LINE__ << "\n";
        std::cout << "\nEntries left in the index" << __LINE__ << "\n";
        exit(1);
    }
    std::cout << "\nDeletion passed at line no:" << __LINE__ << "\n";
}

int stringScan(BTreeIndex * index, int lowVal, Operator lowOp, int highVal, Operator highOp)
{
  RecordId scanRid;
	Page *curPage;

  std::cout << "Scan for ";
  if( lowOp == GT ) { std::cout << "("; } else { std::cout << "["; }
  std::cout << lowVal << "," << highVal;
  if( highOp == LT ) { std::cout << ")"; } else { std::cout << "]"; }
  std::cout << std::endl;

  char lowValStr[100];
  sprintf(lowValStr,"%05d string record",lowVal);
  char highValStr[100];
  sprintf(highValStr,"%05d string record",highVal);

  int numResults = 0;

	try
	{
  	index->startScan(lowValStr, lowOp, highValStr, highOp);
	}
	catch(NoSuchKeyFoundException e)
	{
    std::cout << "No Key Found satisfying the scan criteria." << std::endl;
		return 0;
	}

	while(1)
	{
		try
		{
			index->scanNext(scanRid);
			bufMgr->readPage(file1, scanRid.page_number, curPage);
			RECORD myRec = *(reinterpret_cast<const RECORD*>(curPage->getRecord(scanRid).data()));
			bufMgr->unPinPage(file1, scanRid.page_number, false);

			if( numResults < 20 )
			{
				std::cout << "rid:" << scanRid.page_number << "," << scanRid.slot_number;
				std::cout << " -->:" << myRec.i << ":" << myRec.d << ":" << myRec.s << ":" <<std::endl;
			}
			else if( numResults == 20 )
			{
				std::cout << "..." << std::endl;
			}
		}
		catch(IndexScanCompletedException e)
		{
			break;
		}

		numResults++;
	}

  if( numResults >= 5 )
  {
    std::cout << "Number of results: " << numResults << std::endl;
  }
  index->endScan();
  std::cout << std::endl;

	return numResults;
}

// -----------------------------------------------------------------------------
// errorTests
// -----------------------------------------------------------------------------

void errorTests()
{
	std::cout << "Error handling tests" << std::endl;
	std::cout << "--------------------" << std::endl;
	// Given error test

	try
	{
		File::remove(relationName);
	}
	catch(FileNotFoundException e)
	{
	}

  file1 = new PageFile(relationName, true);
	
  // initialize all of record1.s to keep purify happy
  memset(record1.s, ' ', sizeof(record1.s));
	PageId new_page_number;
  Page new_page = file1->allocatePage(new_page_number);

  // Insert a bunch of tuples into the relation.
	for(int i = 0; i <10; i++ ) 
	{
    sprintf(record1.s, "%05d string record", i);
    record1.i = i;
    record1.d = (double)i;
    std::string new_data(reinterpret_cast<char*>(&record1), sizeof(record1));

		while(1)
		{
			try
			{
    		new_page.insertRecord(new_data);
				break;
			}
			catch(InsufficientSpaceException e)
			{
				file1->writePage(new_page_number, new_page);
  			new_page = file1->allocatePage(new_page_number);
			}
		}
  }

	file1->writePage(new_page_number, new_page);

  BTreeIndex index(relationName, intIndexName, bufMgr, offsetof(tuple,i), INTEGER);
	
	int int2 = 2;
	int int5 = 5;

	// Scan Tests
	std::cout << "Call endScan before startScan" << std::endl;
	try
	{
		index.endScan();
		std::cout << "ScanNotInitialized Test 1 Failed." << std::endl;
	}
	catch(ScanNotInitializedException e)
	{
		std::cout << "ScanNotInitialized Test 1 Passed." << std::endl;
	}
	
	std::cout << "Call scanNext before startScan" << std::endl;
	try
	{
		RecordId foo;
		index.scanNext(foo);
		std::cout << "ScanNotInitialized Test 2 Failed." << std::endl;
	}
	catch(ScanNotInitializedException e)
	{
		std::cout << "ScanNotInitialized Test 2 Passed." << std::endl;
	}
	
	std::cout << "Scan with bad lowOp" << std::endl;
	try
	{
  	index.startScan(&int2, LTE, &int5, LTE);
		std::cout << "BadOpcodesException Test 1 Failed." << std::endl;
	}
	catch(BadOpcodesException e)
	{
		std::cout << "BadOpcodesException Test 1 Passed." << std::endl;
	}
	
	std::cout << "Scan with bad highOp" << std::endl;
	try
	{
  	index.startScan(&int2, GTE, &int5, GTE);
		std::cout << "BadOpcodesException Test 2 Failed." << std::endl;
	}
	catch(BadOpcodesException e)
	{
		std::cout << "BadOpcodesException Test 2 Passed." << std::endl;
	}


	std::cout << "Scan with bad range" << std::endl;
	try
	{
  	index.startScan(&int5, GTE, &int2, LTE);
		std::cout << "BadScanrangeException Test 1 Failed." << std::endl;
	}
	catch(BadScanrangeException e)
	{
		std::cout << "BadScanrangeException Test 1 Passed." << std::endl;
	}

	deleteRelation();
}

void deleteRelation()
{
	if(file1)
	{
		bufMgr->flushFile(file1);
		delete file1;
		file1 = NULL;
	}
	try
	{
		File::remove(relationName);
	}
	catch(FileNotFoundException e)
	{
	}
}

void checkDeletionPassFail(bool result, int line) {
    if(result == false) {
        std::cout<<"Deletion failed. Key not found.\n";
        std::cout<<"Line "<<line;
        std::cout<<std::endl;
        exit(1);
    }
}
void checkDeletionPassFail1(bool result, int line, size_t index) {
    if(result == false) {
        std::cout<<"Deletion failed. Key not found.\n";
        std::cout<<"Line "<<line;
        std::cout<<"\nIndex: "<<index;
        std::cout<<"\nint key: "<<insertedKeysInt[index];
        std::cout<<"\ndouble key: "<<insertedKeysDouble[index];
        std::cout<<"\nstring key: "<<insertedKeysStr[index];
        std::cout<<std::endl;

        exit(1);
    }
}


