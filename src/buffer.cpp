/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University
 * of Wisconsin-Madison.
 */

#include "buffer.h"

#include <iostream>
#include <memory>

#include "exceptions/bad_buffer_exception.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/hash_not_found_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"

namespace badgerdb {

constexpr int HASHTABLE_SZ(int bufs) { return ((int)(bufs * 1.2) & -2) + 1; }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(std::uint32_t bufs)
    : numBufs(bufs),
      hashTable(HASHTABLE_SZ(bufs)),
      bufDescTable(bufs),
      bufPool(bufs) {
  for (FrameId i = 0; i < bufs; i++) {
    bufDescTable[i].frameNo = i;
    bufDescTable[i].valid = false;
  }

  clockHand = bufs - 1;
}


/**
 * Advance clock to next frame in the buffer pool.
 * */
void BufMgr::advanceClock() {
  clockHand = (clockHand + 1) % numBufs;
}

/**
 * 
 * Allocates a free frame using the clock algorithm; if necessary, writing a dirty page back to disk.
Throws BufferExceededException if all buffer frames are pinned. This private method will get
called by the readPage() and allocPage() methods described below. Make sure that if the buffer
frame allocated had a valid page in it, you remove the appropriate entry from the hash table.
Note that the frame variable is a reference, so the function works by assigning the number to
this variable.
 * 
 * 
 * */
void BufMgr::allocBuf(FrameId& frame) {
	for(uint32_t i = 0; i<2*numBufs; i++){
  		//First advance the clock like the algorithm says
  		//We advance clockHand and then when we find a valid 
  		//frame we set the frame that was asked to be allocated with the FrameId value of clockHand at the very end
  		advanceClock();
  		//Check to see if this specific frame is a valid set (in the buffer pool), if not, we set it to be valid
  		//then set the frame value as the current clockHand since this is the frame we will use
  		if(!bufDescTable[clockHand].valid){
			frame = clockHand;
			return;
		}else if(bufDescTable[clockHand].refbit){
        		bufDescTable[clockHand].refbit = false;
		}else if(bufDescTable[clockHand].pinCnt == 0){
			if(bufDescTable[clockHand].dirty){
        			bufDescTable[clockHand].file.writePage(bufPool[clockHand]);
			}
			frame = clockHand;
			return;
		}
	}
	throw BufferExceededException();
}

/*
Read a page from the buffer pool.
Two Cases: (a) if its already in the pool, set refbit, increment pin count an return frame handle.
(b) Page is not in the buffer pool. Call allocBuf() to allocate a buffer frame and then
call the method file.readPage() to read the page from disk into the buffer pool frame.
Next, insert the page into the hashtable. Finally, invoke Set() on the frame to set it up
properly. Set() will leave the pinCnt for the page set to 1. Return a pointer to the frame
containing the page via the page parameter.
*/
void BufMgr::readPage(File& file, const PageId pageNo, Page*& page) {
  FrameId frameId;
  try {
    // lookup frame
    hashTable.lookup(file, pageNo, frameId);

    // FOUND
    // set the refbit and inc pinCnt
    bufDescTable[frameId]->refbit = true;
    bufDescTable[frameId]->pinCnt++;
    
    // return address to page by reference
    page = &bufpool[frameId];
  } catch(HashNotFoundException &e){
    // NOT FOUND
    // allocate frame and get the page address
    allocBuf(frameId);

    // read page from file
    Page newPage = file.readPage(pageNo);

    // insert into the table and set the hash
    hashTable.insert(file,pageNo,frameId);
    bufDescTable[frameId]->Set(file, pageNo);

    // set the frame as the read page
    page = &bufPool[frameId];
    *page = newPage;
	}
}

/*Decrements the pinCnt of the frame containing (file, PageNo) and, if dirty == true, sets the dirty bit. Throws PAGENOTPINNED if the pin count is already 0. Does nothing if page is not found in the hash table lookup. 
 *
 */
void BufMgr::unPinPage(File& file, const PageId pageNo, const bool dirty) {
	FrameId frameNo;
	try{
		hashTable.lookup(file, pageNo, frameNo);
		if(bufDescTable[frameNo].pinCnt == 0){
			throw PageNotPinnedException(file.filename(),pageNo, frameNo);
		}
		bufDescTable[frameNo].pinCnt--;
		if(dirty){
			bufDescTable[frameNo].dirty = true;
		}
	}
	catch(HashNotFoundException &e){
	}
}


void BufMgr::allocPage(File& file, PageId& pageNo, Page*& page) {
  FrameId frameId;

  // new space for page in the file
  Page newPage = file.allocatePage();

  // get the frame
  allocBuf(frameId);

  // insert into hashTable and set the frame
  hashTable.insert(file, newPage.page_number(), frameId);
  bufDescTable[frameId]->Set(file, newPage.page_number());

  // return values. Set Page number and page in pool, then return
  pageNo = newPage.page_number();
  page = &bufPool[frameId];
  *page = newPage;
}

/**
 * 
 * Scan bufTable for pages belonging to the file, for each page encountered it should: (a) if the
page is dirty, call file.writePage() to flush the page to disk and then set the dirty bit for the page
to false, (b) remove the page from the hashtable (whether the page is clean or dirty) and (c)
invoke the Clear() method of BufDesc for the page frame. Throws PagePinnedException if some
page of the file is pinned. Throws BadBufferException if an invalid page belonging to the file is
encountered.
 * */
void BufMgr::flushFile(File& file) {

  //loops through all the frames in the buffer pool, whether they are in file or not
  for(FrameId frame = 0; frame < numBufs; frame++){

    //now we check for whether or not the page in the frame is in the file, 
    //if it is then we check if it has been edited
    if(bufDescTable[frame].file  == file){

      if(bufDescTable[frame].pinCnt > 0){
        throw PagePinnedException(bufDescTable[frame].file.filename(), bufDescTable[frame].pageNo, bufDescTable[frame].frameNo);
        continue;
      }

      if(!bufDescTable[frame].valid){
        throw BadBufferException(bufDescTable[frame].frameNo, bufDescTable[frame].dirty, false, bufDescTable[frame].refbit);
      }
      
      //if page is dirty, use writePage() to write to disk
      if(bufDescTable[frame].dirty){
        bufDescTable[frame].file.writePage(bufPool[frame]);
        bufDescTable[frame].dirty = false;
      }

      //removes page whether dirty or clean
      hashTable.remove(file, bufDescTable[frame].pageNo);

      //clears the page frame
      bufDescTable[frame].clear();
    }

  }


}

/*
 *
 *This method deletes a particular page from file. Before deleting the page from file, it makes sure that if the page to be deleted is allocated a frame in the buffer pool, that frame is freed and correspondingly entry from hash table is also removed.
 *
 *
 */
void BufMgr::disposePage(File& file, const PageId pageNo) {
	FrameId frameNo;
	try{
                hashTable.lookup(file, pageNo, frameNo);
		bufDescTable[frameNo].clear();
		hashTable.remove(file, pageNo);

        }
        catch(HashNotFoundException &e){
        }
	file.deletePage(pageNo);
}

void BufMgr::printSelf(void) {
  int validFrames = 0;

  for (FrameId i = 0; i < numBufs; i++) {
    std::cout << "FrameNo:" << i << " ";
    bufDescTable[i].Print();

    if (bufDescTable[i].valid) validFrames++;
  }

  std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}  // namespace badgerdb
