/**
 * @author Lingfei Li (Student Id: 9074068637)
 */

#include <memory>
#include <iostream>
#include <cstring>
#include <cassert>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb { 

BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs) {
  bufDescTable = new BufDesc[bufs];

  for (FrameId i = 0; i < bufs; i++) 
  {
  	bufDescTable[i].frameNo = i;
  	bufDescTable[i].valid = false;
  }

  bufPool = new Page[bufs];

  int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

  clockHand = bufs - 1;
}


BufMgr::~BufMgr() {
    for(unsigned frameNo = 0; frameNo < numBufs; frameNo ++) {
        BufDesc* bd = &bufDescTable[frameNo];
        if(bd->valid == false) {
            continue;
        }

        //If the page is dirty, flush the page to disk
        if(bd->dirty == true) {
            bd->file->writePage(bufPool[frameNo]);
            bd->dirty = false;
        }

        //Remove the page from hashTable
        hashTable->remove(bd->file, bd->pageNo);

        //Invoke Clear() method of bufDesc of the frame
        bd->Clear();
    }
    delete(hashTable);
    delete(bufDescTable);
}

/**
* Advance clock to next frame in the buffer pool
*/
void BufMgr::advanceClock()
{
    clockHand = (clockHand + 1) % numBufs;
}

/**
* Allocate a free frame.  
*
* @param frame   	Frame reference, frame ID of allocated frame returned via this variable
* @throws BufferExceededException If no such buffer is found which can be allocated
*/
void BufMgr::allocBuf(FrameId & frame) 
{
    unsigned pinnedFrames = 0;

    BufDesc* bd = NULL;
    while(true) {
        advanceClock();
        if(pinnedFrames == numBufs) {
            throw BufferExceededException();
            return;
        }

        bd = &bufDescTable[clockHand];

        if(bd->valid == false) {
            break;
        }
        if(bd->refbit == true) {
            bd->refbit = false;
            continue;
        }
        if(bd->pinCnt != 0) {
            pinnedFrames ++;
            continue;
        }
        if(bd->dirty == true) {
            //If the page is dirty, flush the page to disk
            bd->file->writePage(bufPool[clockHand]);
            bd->dirty = false;
        }
        break;
    }

    assert(bd != NULL);

    if(bd->valid == true) {
        //remove the hash table entry
        hashTable->remove(bd->file, bd->pageNo);
    }
    frame = clockHand;
    bd->Clear();
    //Caller calls Set() on the new frame
}

	
/**
* Reads the given page from the file into a frame and returns the pointer to page.
* If the requested page is already present in the buffer pool pointer to that frame is returned
* otherwise a new frame is allocated from the buffer pool for reading the page.
*
* @param file   	File object
* @param PageNo  Page number in the file to be read
* @param page  	Reference to page pointer. Used to fetch the Page object in which requested page from file is read in.
*/
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
    FrameId frameNo = -1;
    if(hashTable->lookup(file, pageNo, frameNo)) {
        //Reqested page is present in the buffer pool 

        bufDescTable[frameNo].refbit = true;
        bufDescTable[frameNo].pinCnt ++;

        page = &bufPool[frameNo];
    }
    else {
        //Requested page is not present. 
        //Allocate a new buffer frame and reads the file content into it
        

        //reads the file content into the new buffer frame
        Page tmpPage = file->readPage(pageNo);

        //allocate a new frame in the buffer pool for the new file page
        allocBuf(frameNo);

        //Set() on the newFrame
        bufDescTable[frameNo].Set(file, pageNo);

        //copy from tmpPage to buffer pool
        bufPool[frameNo] = tmpPage;

        //insert an entry to hashTable
        hashTable->insert(file, pageNo, frameNo);

        page = &bufPool[frameNo];
    }
}


/**
* Unpin a page from memory since it is no longer required for it to remain in memory.
*
* @param file   	File object
* @param PageNo  Page number
* @param dirty		True if the page to be unpinned needs to be marked dirty	
* @throws  PageNotPinnedException If the page is not already pinned
*/
void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{

    FrameId frameNo = -1;
    if(hashTable->lookup(file, pageNo, frameNo) == false) {
        //Page not found in the hash table. Do nothing
        return;
    }

    if(frameNo < 0 || frameNo >= numBufs) {
        //Invalid frame number
        return;
    }

    BufDesc& bd = bufDescTable[frameNo];
    if(bd.valid == false) {
        //Page is not valid
        return;
    }

    if(bd.pinCnt == 0) {
        //Pin Count of the frame is already zero
        throw PageNotPinnedException(file->filename(), pageNo, frameNo);
        return;
    }

    if(dirty == true) {
        //Set dirty bit if required
        bd.dirty = true;
    }

    bd.pinCnt --;
}

/**
* Writes out all dirty pages of the file to disk.
* All the frames assigned to the file need to be unpinned from buffer pool before this function can be successfully called.
* Otherwise Error returned.
*
* @param file   	File object
* @throws  PagePinnedException If any page of the file is pinned in the buffer pool 
* @throws BadBufferException If any frame allocated to the file is found to be invalid
*/
void BufMgr::flushFile(const File* file) 
{
    for(unsigned frameNo = 0; frameNo < numBufs; frameNo ++) {
        BufDesc* bd = &bufDescTable[frameNo];
        if(bd->file == file) {
            if(bd->valid == false) {
                throw BadBufferException(frameNo, bd->dirty, bd->valid, bd->refbit);
            }
            if(bd->pinCnt != 0) {
                throw PagePinnedException(bd->file->filename(), bd->pageNo, frameNo);
            }

            //If the page is dirty, flush the page to disk
            if(bd->dirty == true) {
                bd->file->writePage(bufPool[frameNo]);
                bd->dirty = false;
            }

            //Remove the entry from hashTable
            hashTable->remove(file, bd->pageNo);

            //Invoke Clear() method of bufDesc of the frame
            bd->Clear();
        }
    }
}

/**
* Allocates a new, empty page in the file and returns the Page object.
* The newly allocated page is also assigned a frame in the buffer pool.
*
* @param file   	File object
* @param PageNo  Page number. The number assigned to the page in the file is returned via this reference.
* @param page  	Reference to page pointer. The newly allocated in-memory Page object is returned via this reference.
*/
void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{

    //Allocate a new page in the file
    //May throw exception if file is full
    Page newFilePage = file->allocatePage();
    pageNo = newFilePage.page_number();

    //allocate a new frame in the buffer pool
    FrameId frameNo;
    allocBuf(frameNo);

    //Copy the new page to buffer pool
    bufPool[frameNo] = newFilePage;

    //Insert an entry to hashTable
    hashTable->insert(file, pageNo, frameNo);

    //Initialize buffer description entry
    bufDescTable[frameNo].Set(file, pageNo);

    //Return the new page
    page = &bufPool[frameNo];
}

/**
* Delete page from file and also from buffer pool if present.
* Since the page is entirely deleted from file, its unnecessary to see if the page is dirty.
*
* @param file   	File object
* @param PageNo  Page number
*/
void BufMgr::disposePage(File* file, const PageId pageNo)
{
    FrameId frameNo = -1;
    if(hashTable->lookup(file, pageNo, frameNo) == true) {
        if(frameNo < 0 || frameNo >= numBufs) {
            //Invalid frame number
            return;
        }
        bufDescTable[frameNo].Clear();
        hashTable->remove(file, pageNo);
    }
    file->deletePage(pageNo);
}

/**
* Print member variable values. 
*/
void BufMgr::printSelf(void) 
{
  BufDesc* tmpbuf;
	int validFrames = 0;
  
  for (std::uint32_t i = 0; i < numBufs; i++)
	{
        if(i == clockHand) {
            std::cout<<"* -> ";
        }
  	    tmpbuf = &(bufDescTable[i]);
		std::cout << "FrameNo:" << i << " ";
		tmpbuf->Print();
		std::cout << "\tpage No:" << bufPool[i].page_number() << " \n";


  	if (tmpbuf->valid == true)
    	validFrames++;
  }

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}
