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

  //First advance the clock like the algorithm says
  //We advance clockHand and then when we find a valid 
  //frame we set the frame that was asked to be allocated with the FrameId value of clockHand at the very end
  advanceClock();

  //Check to see if this specific frame is a valid set (in the buffer pool), if not, we set it to be valid
  //then set the frame value as the current clockHand since this is the frame we will use
  if(!bufDescTable[clockHand].valid){
    Set(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
    frame = clockHand;
  }

  else{

    //while this specific frame that clockHand is pointing to is valid, we check the remaining blocks in the algorithm
    while(bufDescTable[clockHand].valid){

      //if refbit is set to true then it is cleared and method is called recursively. This time it will bypass this check since
      //it is set to false
      if(bufDescTable[clockHand].refbit){
        bufDescTable[clockHand].refbit = false;
        allocBuf(bufDescTable[clockHand]);
      }

      if(bufDescTable[clockHand].pinCnt > 0){
        allocBuf(bufDescTable[clockHand]);
      }

      //if the page is dirty, we write it back to the disk
      if(bufDescTable[clockHand].dirty){
        unPinPage(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo, true);
        bufDescTable[clockHand].file.writePage(bufDescTable[clockHand].page_number);
      }

      if(!bufDescTable[clockHand].dirty){
        Set(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
        frame = clockHand;
      }

      advanceClock();
    }
  }

  //we set the frame that was asked to be allocated with the FrameId value of clockHand
  frame = clockHand;

}

void BufMgr::readPage(File& file, const PageId pageNo, Page*& page) {}

void BufMgr::unPinPage(File& file, const PageId pageNo, const bool dirty) {
	FrameId frameNo;
	try{
		hashTable.lookup(file, pageNo, frameNo);
		if(bufDescTable[frameNo].pinCnt == 0){
			throw PageNotPinnedException("buffer.cpp",pageNo, frameNo);
		}
		bufDescTable[frameNo].pinCnt--;
		if(dirty){
			bufDescTable[frameNo].dirty = true;
		}
	}
	catch(HashNotFoundException &e){
	}
}

void BufMgr::allocPage(File& file, PageId& pageNo, Page*& page) {}

void BufMgr::flushFile(File& file) {}

void BufMgr::disposePage(File& file, const PageId PageNo) {}

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
