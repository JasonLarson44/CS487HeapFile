package heap; 

import global.*;
import jdk.nashorn.internal.ir.WhileNode;

/**
 * <h3>Minibase Heap Files</h3>
 * A heap file is the simplest database file structure.  It is an unordered 
 * set of records, stored on a set of data pages. <br>
 * This class supports inserting, selecting, updating, and deleting
 * records.<br>
 * Normally each heap file has an entry in the database's file library.
 * Temporary heap files are used for external sorting and in other
 * relational operators. A temporary heap file does not have an entry in the
 * file library and is deleted when there are no more references to it. <br>
 * A sequential scan of a heap file (via the HeapScan class)
 * is the most basic access method.
 */
public class HeapFile implements GlobalConst {

  /** HFPage type for directory pages. */
  protected static final short DIR_PAGE = 10;

  /** HFPage type for data pages. */
  protected static final short DATA_PAGE = 11;

  // --------------------------------------------------------------------------

  /** Is this a temporary heap file, meaning it has no entry in the library? */
  protected boolean isTemp;

  /** The heap file name.  Null if a temp file, otherwise 
   * used for the file library entry. 
   */
  protected String fileName;

  /** First page of the directory for this heap file. */
  protected PageId headId;

  // --------------------------------------------------------------------------

  /**
   * If the given name is in the library, this opens the corresponding
   * heapfile; otherwise, this creates a new empty heapfile. 
   * A null name produces a temporary file which
   * requires no file library entry.
   */
  public HeapFile(String name) {

      PageId pId = Minibase.DiskManager.get_file_entry(name);
      this.fileName = name;
      this.isTemp = false;
      if(pId != null) //Heapfile was already in the file library
      {
          this.headId = pId; //Set this page id to be the page id of the located file
      }
      else //File was not in the file library
      {
          this.headId = Minibase.DiskManager.allocate_page();
          DirPage DirHead = new DirPage();
          Minibase.BufferManager.pinPage(this.headId, DirHead, GlobalConst.PIN_MEMCPY);
          DirHead.setCurPage(this.headId);
          DirHead.setType(DIR_PAGE);
          Minibase.BufferManager.unpinPage(this.headId, true);
          if(this.fileName != null)//Not a temp file, so add it to the file library
          {
              Minibase.DiskManager.add_file_entry(this.fileName, this.headId);
          }
          else
          {
              this.isTemp = true;
          }
      }
  } // public HeapFile(String name)

  /**
   * Called by the garbage collector when there are no more references to the
   * object; deletes the heap file if it's temporary.
   */
  protected void finalize() throws Throwable {

	    if(this.isTemp)
	    {
            deleteFile();
        }

  } // protected void finalize() throws Throwable

  /**
   * Deletes the heap file from the database, freeing all of its pages
   * and its library entry if appropriate.
   */
  public void deleteFile() {

	 PageId current = this.headId;
	 PageId next;
	 DirPage dirPage = new DirPage();
     PageId dataId;

	 while(current.pid > 0)
     {
         Minibase.BufferManager.pinPage(current, dirPage, GlobalConst.PIN_DISKIO);
         next = dirPage.getNextPage();
         Minibase.BufferManager.unpinPage(current, UNPIN_CLEAN);
         for(int i = 0; i < dirPage.MAX_ENTRIES; ++i)
         {
             dataId = dirPage.getPageId(i);
             if(dataId.pid > 0) //if valid
             {
                 deletePage(dataId, current, dirPage, i); //delete the page
                 //should delete directory on last data page deletion
             }
         }
         current = next;
     }
     if(!this.isTemp)//remove from file library
     {
         Minibase.DiskManager.delete_file_entry(this.fileName);
     }

  } // public void deleteFile()

  /**
   * Inserts a new record into the file and returns its RID.
   * Should be efficient about finding space for the record.
   * However, fixed length records inserted into an empty file
   * should be inserted sequentially.
   * Should create a new directory and/or data page only if
   * necessary.
   * 
   * @throws IllegalArgumentException if the record is too 
   * large to fit on one data page
   */
  public RID insertRecord(byte[] record) {
      short numEntries = 0;
      short newRecCnt = 0;
      DataPage data = new DataPage();
      PageId dataPID = new PageId();
      int recLen = record.length;
      RID newRid = new RID();
      if(record.length > GlobalConst.PAGE_SIZE)
      {
          throw new IllegalArgumentException("Record is too large to fit on one page");
      }
      DirPage directory = new DirPage();
      PageId nextPage = this.headId;
      while(nextPage.pid != -1) //start with first directory page and find the first data page with enough free space
      {
          Minibase.BufferManager.pinPage(nextPage, directory, GlobalConst.PIN_DISKIO);
          numEntries = directory.getEntryCnt();
          for(int i = 0; i < numEntries; ++i)//search for first entry with enough free space
          {
              if(directory.getFreeCnt(i) > recLen + directory.SLOT_SIZE) //We can add at this data page
              {
                  dataPID = directory.getPageId(i);
                  Minibase.BufferManager.pinPage(dataPID, data, GlobalConst.PIN_DISKIO);
                  newRid = data.insertRecord(record);
                  directory.setFreeCnt(i, data.getFreeSpace());
                  newRecCnt = directory.getRecCnt(i);
                  newRecCnt += 1;
                  directory.setRecCnt(i, newRecCnt);
                  Minibase.BufferManager.unpinPage(dataPID, true);
                  Minibase.BufferManager.unpinPage(nextPage, true);
                  return newRid;
              }
          }
          if(numEntries < directory.MAX_ENTRIES) //Room to create a new data page at numEntries + 1
          {
              dataPID = Minibase.DiskManager.allocate_page();
              Minibase.BufferManager.pinPage(dataPID, data, GlobalConst.PIN_MEMCPY);
              data.setCurPage(dataPID);
              numEntries += 1;
              directory.setEntryCnt(numEntries);
              directory.setPageId(numEntries -1, dataPID);//slots start at 0
              newRid = data.insertRecord(record);
              newRecCnt = directory.getRecCnt(numEntries -1);
              newRecCnt += 1;
              directory.setRecCnt(numEntries -1, newRecCnt);
              directory.setFreeCnt(numEntries -1, data.getFreeSpace());
              Minibase.BufferManager.unpinPage(dataPID, true);
              Minibase.BufferManager.unpinPage(nextPage, true);
              return newRid;
          }
          //If no room to add a data page, attempt to go to the next directory page
          Minibase.BufferManager.unpinPage(nextPage, false);
          nextPage = directory.getNextPage();
      }
      //If made it to end of the directory pages without inserting record, add a new directory page
      nextPage = directory.getCurPage(); //Get the page id of the last directory page in the linked list
      Minibase.BufferManager.pinPage(nextPage, directory, GlobalConst.PIN_DISKIO); //Pin the page again
      nextPage = Minibase.DiskManager.allocate_page();//allocate a page for our new directory page
      directory.setNextPage(nextPage);//Add the new directory page to the end of the linked list
      Minibase.BufferManager.unpinPage(directory.getCurPage(), true); //unpin
      directory = new DirPage();
      Minibase.BufferManager.pinPage(nextPage, directory, GlobalConst.PIN_MEMCPY); //Pin the new directory page
      directory.setCurPage(nextPage);
      //directory page is empty so allocate a new data page in slot 0
      dataPID = Minibase.DiskManager.allocate_page();
      Minibase.BufferManager.pinPage(dataPID, data, GlobalConst.PIN_MEMCPY);
      data.setCurPage(dataPID);
      numEntries = 1;
      directory.setEntryCnt(numEntries);
      directory.setPageId(0, dataPID);//slots start at 0
      newRid = data.insertRecord(record);
      directory.setFreeCnt(0, data.getFreeSpace());
      newRecCnt = 1; //First record on this page
      directory.setRecCnt(0, newRecCnt);
      Minibase.BufferManager.unpinPage(dataPID, true);
      Minibase.BufferManager.unpinPage(nextPage, true);
      return newRid;

   } // public RID insertRecord(byte[] record)

  /**
   * Reads a record from the file, given its rid.
   * 
   * @throws IllegalArgumentException if the rid is invalid
   */
  public byte[] selectRecord(RID rid) {
      PageId dataPID= rid.pageno;
      DataPage dataPage = new DataPage();
      byte[] record;

      Minibase.BufferManager.pinPage(dataPID, dataPage, GlobalConst.PIN_DISKIO);
      try {
          record = dataPage.selectRecord(rid);
      }catch (Exception e)
      {
          Minibase.BufferManager.unpinPage(dataPID, false);
          throw new IllegalArgumentException("Invalid RID");
      }
      Minibase.BufferManager.unpinPage(dataPID, false); //not dirty, only reading

      return record;

  } // public byte[] selectRecord(RID rid)

  /**
   * Updates the specified record in the heap file.
   * 
   * @throws IllegalArgumentException if the rid or new record is invalid
   */
  public void updateRecord(RID rid, byte[] newRecord) {
      PageId dataPID= rid.pageno;
      DataPage dataPage = new DataPage();

      Minibase.BufferManager.pinPage(dataPID, dataPage, GlobalConst.PIN_DISKIO);
      try {
          dataPage.updateRecord(rid, newRecord);
      }catch (Exception e)
      {
          Minibase.BufferManager.unpinPage(dataPID, UNPIN_DIRTY);
          throw new IllegalArgumentException("Invalid RID or new record");
      }
      Minibase.BufferManager.unpinPage(dataPID, UNPIN_DIRTY);


  } // public void updateRecord(RID rid, byte[] newRecord)

  /**
   * Deletes the specified record from the heap file.
   * Removes empty data and/or directory pages.
   * 
   * @throws IllegalArgumentException if the rid is invalid
   */
  public void deleteRecord(RID rid) {
      PageId dataPID= rid.pageno;
      DataPage dataPage = new DataPage();
      DirPage dirPage = new DirPage();
      PageId DirId = new PageId();
      int index = 0;
      Minibase.BufferManager.pinPage(dataPID, dataPage, GlobalConst.PIN_DISKIO);
      try {
          dataPage.deleteRecord(rid);
      }catch (Exception e)
      {
          Minibase.BufferManager.unpinPage(dataPID, UNPIN_DIRTY);
          throw new IllegalArgumentException("Invalid RID");
      }

      if(dataPage.getSlotCount() == 0) //empty data page
      {
          index = findDirEntry(dataPID, DirId, dirPage); //Find the directory page
          deletePage(dataPID, DirId, dirPage, index ); //Delete data page and possibly directory page if needed
      }
      Minibase.BufferManager.unpinPage(dataPID, UNPIN_DIRTY);

  } // public void deleteRecord(RID rid)

  /**
   * Gets the number of records in the file.
   */
  public int getRecCnt() {

	    int count = 0;
	    PageId nextPage = this.headId;
	    DirPage current = new DirPage();

	    while(nextPage.pid != -1)
        {
            Minibase.BufferManager.pinPage(nextPage, current, GlobalConst.PIN_DISKIO);
            for(int i = 0; i < current.MAX_ENTRIES; ++i)
            {
                count += current.getRecCnt(i);
            }
            Minibase.BufferManager.unpinPage(nextPage, false);
            nextPage = current.getNextPage();
        }
        return count;
  } // public int getRecCnt()

  /**
   * Initiates a sequential scan of the heap file.
   */
  public HeapScan openScan() {
    return new HeapScan(this);
  }

  /**
   * Returns the name of the heap file.
   */
  public String toString() {
    return fileName;
  }

  /**
   * Searches the directory for the first data page with enough free space to store a
   * record of the given size. If no suitable page is found, this creates a new
   * data page.
   * A more efficient implementation would start with a directory page that is in the
   * buffer pool.
   */
  protected PageId getAvailPage(int reclen) {
      PageId availPage = new PageId();
      PageId current = this.headId;
      DirPage dirPage = new DirPage();

      while(current.pid != -1) //Loop through Directory page linked list
      {
          Minibase.BufferManager.pinPage(current, dirPage, PIN_DISKIO);
          int i; //Going to use i after loop to add a new data page at next available slot
          for(i = 0; i < dirPage.getEntryCnt(); ++i)//for each dir page, loop through data pages
          {
              if(dirPage.getFreeCnt(i) >= reclen + HFPage.SLOT_SIZE) //record need reclen + slot size space
              {
                  availPage = dirPage.getPageId(i);
                  Minibase.BufferManager.unpinPage(current, UNPIN_CLEAN);
                  return availPage;
              }
          }
          Minibase.BufferManager.unpinPage(current, GlobalConst.UNPIN_CLEAN);
          current = dirPage.getNextPage();
      }
      availPage = insertPage();
      return availPage;
  } // protected PageId getAvailPage(int reclen)

  /**
   * Helper method for finding directory entries of data pages.
   * A more efficient implementation would start with a directory
   * page that is in the buffer pool.
   * 
   * @param pageno identifies the page for which to find an entry
   * @param dirId output param to hold the directory page's id (pinned)
   * @param dirPage output param to hold directory page contents
   * @return index of the data page's entry on the directory page
   */
  protected int findDirEntry(PageId pageno, PageId dirId, DirPage dirPage) {
      int index = -1; //return -1 if page is not found in heapfile
      PageId current = this.headId;
      DirPage temp = new DirPage();

      while(current.pid != -1)
      {
          Minibase.BufferManager.pinPage(current, temp, GlobalConst.PIN_DISKIO);
          for(int i = 0; i < temp.getEntryCnt(); ++i)
          {
              if(temp.getPageId(i).pid == pageno.pid)//data page belongs to this directory page at index i
              {
                  dirId.pid = temp.getCurPage().pid; //get directory page id
                  dirPage.copyPage(temp); //copy page contents
                  index = i;
                  Minibase.BufferManager.unpinPage(current, UNPIN_CLEAN);
                  return index;
              }
          }
          Minibase.BufferManager.unpinPage(current, UNPIN_CLEAN);
          current = temp.getNextPage();
      }
      return index;
  } // protected int findEntry(PageId pageno, PageId dirId, DirPage dirPage)

  /**
   * Updates the directory entry for the given data page.
   * If the data page becomes empty, remove it.
   * If this causes a dir page to become empty, remove it
   * @param pageno identifies the data page whose directory entry will be updated
   * @param deltaRec input change in number of records on that data page
   * @param freecnt input new value of freecnt for the directory entry
   */
  protected void updateDirEntry(PageId pageno, int deltaRec, int freecnt) {

      PageId dirId = new PageId();
      DirPage dirPage = new DirPage();
      short recCnt = 0;
      short freeCntShort = 0;
      freeCntShort += freecnt;
      int slotNo = findDirEntry(pageno, dirId, dirPage); //Find the directory page for this data page
      Minibase.BufferManager.pinPage(dirId, dirPage, PIN_DISKIO);//pin directory page
      recCnt = dirPage.getRecCnt(slotNo);
      recCnt += deltaRec;
      dirPage.setRecCnt(slotNo, recCnt);
      dirPage.setFreeCnt(slotNo, freeCntShort);
      Minibase.BufferManager.unpinPage(dirId, UNPIN_DIRTY);


  } // protected void updateEntry(PageId pageno, int deltaRec, int deltaFree)

  /**
   * Inserts a new empty data page and its directory entry into the heap file. 
   * If necessary, this also inserts a new directory page.
   * Leaves all data and directory pages unpinned
   * 
   * @return id of the new data page
   */
  protected PageId insertPage() {
      PageId newPage = new PageId();
      PageId current = this.headId; //Loop through directory pages
      DirPage dirPage = new DirPage();
      DataPage dataPage = new DataPage();
      short zero = 0;
      short entries = 0;
      short freeCnt = 0;

      //Allocate a new data page
      newPage = Minibase.DiskManager.allocate_page();
      Minibase.BufferManager.pinPage(newPage, dataPage, PIN_DISKIO);
      dataPage.initDefaults();
      dataPage.setCurPage(newPage);
      freeCnt = dataPage.getFreeSpace();
      dataPage.setType(DATA_PAGE);
      Minibase.BufferManager.unpinPage(newPage, UNPIN_DIRTY);

      while(current.pid != -1)//loop through dir pages
      {
          Minibase.BufferManager.pinPage(current, dirPage, PIN_DISKIO);
          entries = dirPage.getEntryCnt();
          if(entries < dirPage.MAX_ENTRIES)//If room to insert a data page here
          {
              for (int i = 0; i < dirPage.MAX_ENTRIES; ++i){
                  if (dirPage.getPageId(i).pid <= 0)//If there is no data page at this slot
                  {
                      //Add the data page to this directory page at the ith slot
                      dirPage.setPageId(i, newPage);
                      dirPage.setRecCnt(i, zero);
                      dirPage.setFreeCnt(i, freeCnt);
                      entries += 1;
                      dirPage.setEntryCnt(entries); //added a new entry

                      Minibase.BufferManager.unpinPage(current, UNPIN_DIRTY); //Added a new data page
                      return newPage;
                  }
              }
          }
          Minibase.BufferManager.unpinPage(current, GlobalConst.UNPIN_CLEAN);
          current = dirPage.getNextPage();
      }
      //If we haven't inserted the data page yet, then there is no room in any of the directory pages so add a new one
      current = dirPage.getCurPage(); //dirPage should still hold the data from the last directory page
      Minibase.BufferManager.pinPage(current, dirPage, GlobalConst.PIN_DISKIO); //Pin the last directory page
      PageId newDirId = Minibase.DiskManager.allocate_page();
      DirPage newDirPage = new DirPage();

      Minibase.BufferManager.pinPage(newDirId, newDirPage, PIN_DISKIO); //Pin the new directory page

      //Link the new directory page
      newDirPage.setPrevPage(current);//point back to last dir page
      dirPage.setNextPage(newDirId); //Point to the new dir page

      dirPage.setPageId(0, newPage);//add in slot 0
      dirPage.setRecCnt(0, zero);
      dirPage.setFreeCnt(0, freeCnt);
      entries = 1;
      dirPage.setEntryCnt(entries); //added a new entry
      Minibase.BufferManager.unpinPage(newDirId, UNPIN_DIRTY); //Added a new data page
      Minibase.BufferManager.unpinPage(current, UNPIN_DIRTY); //Changed next pointer

      return newPage;
  } // protected PageId insertPage()

  /**
   * Deletes the given data page and its directory entry from the heap file. If
   * appropriate, this also deletes the directory page.
   * 
   * @param pageno identifies the page to be deleted
   * @param dirId input param id of the directory page holding the data page's entry
   * @param dirPage input param to hold directory page contents
   * @param index input the data page's entry on the directory page
   */
  protected void deletePage(PageId pageno, PageId dirId, DirPage dirPage,
      int index) {
      short numEntries = 0;
      PageId previous = new PageId();
      PageId next = new PageId();
      DirPage prevPage = new DirPage();
      DirPage nextPage = new DirPage();

      DataPage dataPage = new DataPage();
      Minibase.BufferManager.pinPage(dirId, dirPage, GlobalConst.PIN_DISKIO);
      Minibase.BufferManager.freePage(pageno);
      numEntries = dirPage.getEntryCnt();
      dirPage.setPageId(index, new PageId());
      numEntries -= 1;
      if(numEntries == 0)//delete directory too
      {
          previous = dirPage.getPrevPage();
          next = dirPage.getPrevPage();

          if(previous.pid != -1){//This dir page is not the head of the list
              Minibase.BufferManager.pinPage(previous, prevPage, GlobalConst.PIN_DISKIO);
              prevPage.setNextPage(next); //link around dirpage we are deleting
              if(next.pid != -1)//If we need to link the next page's prev pointer
              {
                  Minibase.BufferManager.pinPage(next, nextPage, GlobalConst.PIN_DISKIO);
                  nextPage.setPrevPage(previous);
                  Minibase.BufferManager.unpinPage(next, GlobalConst.UNPIN_DIRTY);
              }
              Minibase.BufferManager.unpinPage(previous, GlobalConst.UNPIN_DIRTY);
          }
          else //Dir page to remove is head of the list
          {
              this.headId = dirPage.getNextPage(); //Set next page to be new head
              Minibase.BufferManager.pinPage(this.headId, nextPage, GlobalConst.PIN_DISKIO);
              nextPage.setPrevPage(new PageId()); //set to invalid page id
              Minibase.BufferManager.unpinPage(next, GlobalConst.UNPIN_DIRTY);
          }
          Minibase.BufferManager.unpinPage(dirId, UNPIN_DIRTY);
          Minibase.BufferManager.freePage(dirId);
      }
      else {
          dirPage.setEntryCnt(numEntries);
      }
  } // protected void deletePage(PageId, PageId, DirPage, int)

} // public class HeapFile implements GlobalConst
