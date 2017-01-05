package simpledb;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from disk. Access methods call into it to retrieve
 * pages, and it fetches pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking; when a transaction fetches a page, BufferPool which check that the
 * transaction has the appropriate locks to read/write the page.
 */
public class BufferPool {

	/** Bytes per page, including header. */
	public static final int PAGE_SIZE = 4096;

	/**
	 * Default number of pages passed to the constructor. This is used by other classes. BufferPool should use the
	 * numPages argument to the constructor instead.
	 */
	public static final int DEFAULT_PAGES = 50;

	/**
	 * The maximum number of pages in this buffer pool.
	 */
	protected int numPages;

	/**
	 * A Hashtable that associates PageIds with Pages.
	 */
	protected Hashtable<PageId, Page> pages;
	protected Hashtable<PageId, PageId> page;
	/**
	 * Creates a BufferPool that caches up to numPages pages.
	 * 
	 * @param numPages
	 *            maximum number of pages in this buffer pool.
	 */
	public BufferPool(int numPages) {
		this.numPages = numPages;
		pages = new Hashtable<PageId, Page>();
		page = new Hashtable<PageId, PageId>();
	}

	/**
	 * Retrieve the specified page with the associated permissions. Will acquire a lock and may block if that lock is
	 * held by another transaction.
	 * <p>
	 * The retrieved page should be looked up in the buffer pool. If it is present, it should be returned. If it is not
	 * present, it should be added to the buffer pool and returned. If there is insufficient space in the buffer pool,
	 * an page should be evicted and the new page should be added in its place.
	 * 
	 * @param tid
	 *            the ID of the transaction requesting the page
	 * @param pid
	 *            the ID of the requested page
	 * @param perm
	 *            the requested permissions on the page
	 */


	public Page getPage(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException,
			DbException {
		// some code goes here
		if(pages.containsKey(pid))
			return pages.get(pid);
		else{
		    if(pages.keySet().size() <  this.numPages){
	             Page currentPage = (Page) Database.getCatalog().getDbFile(pid.getTableId()).readPage(pid);
	             pages.put(pid, currentPage);
	    		return currentPage;
		    }else{
		    	throw new DbException(" Maximum size of page has been reached.");
		    }
		    }
	}
	
//		System.out.println("inside getpage method" + ppid.toString());
////		HeapPage pg;
//		PageId pid;
//		DbFile dbFile = null;
//		pid= page.get(ppid);
//		if(pid!=null){
//			page.get(ppid);
//		}
//		else{
//			page.put(ppid, ppid);
//		}
////		if(pages.containsKey(pid)){
////			HeapPage pg = (HeapPage) pages.get(pid);
////			return pg;
////		}
////		else
//			dbFile = Database.getCatalog().getDbFile(pid.getTableId());
//			Page page1= dbFile.readPage(pid);
//			
//			if(pages.keySet().size()>=numPages)
//			pages.put(pid, page1);
//		else
//			throw new UnsupportedOperationException("Implement this");
//		return page1;
//	}

	/**
	 * Releases the lock on a page. Calling this is very risky, and may result in wrong behavior. Think hard about who
	 * needs to call this and why, and why they can run the risk of calling it.
	 * 
	 * @param tid
	 *            the ID of the transaction requesting the unlock
	 * @param pid
	 *            the ID of the page to unlock
	 */
	public void releasePage(TransactionId tid, PageId pid) {
		// some code goes here
		// not necessary for assignment1|assignment2
	}

	/**
	 * Release all locks associated with a given transaction.
	 * 
	 * @param tid
	 *            the ID of the transaction requesting the unlock
	 */
	public void transactionComplete(TransactionId tid) throws IOException {
		// some code goes here
		// not necessary for assignment1|assignment2
	}

	/** Return true if the specified transaction has a lock on the specified page */
	public boolean holdsLock(TransactionId tid, PageId p) {
		// some code goes here
		// not necessary for assignment1|assignment2
		return false;
	}

	/**
	 * Commit or abort a given transaction; release all locks associated to the transaction.
	 * 
	 * @param tid
	 *            the ID of the transaction requesting the unlock
	 * @param commit
	 *            a flag indicating whether we should commit or abort
	 */
	public void transactionComplete(TransactionId tid, boolean commit) throws IOException {
		// some code goes here
		// not necessary for assignment1|assignment2
	}

	/**
	 * Add a tuple to the specified table behalf of transaction tid. Will acquire a write lock on the page the tuple is
	 * added to(Lock acquisition is not needed for assignment2). May block if the lock cannot be acquired.
	 * 
	 * Marks any pages that were dirtied by the operation as dirty by calling their markDirty bit, and updates cached
	 * versions of any pages that have been dirtied so that future requests see up-to-date pages.
	 * 
	 * @param tid
	 *            the transaction adding the tuple
	 * @param tableId
	 *            the table to add the tuple to
	 * @param t
	 *            the tuple to add
	 */
	public void insertTuple(TransactionId tid, int tableId, Tuple t) throws DbException, IOException,
			TransactionAbortedException {
		// some code goes here
		// not necessary for assignment1
	}

	/**
	 * Remove the specified tuple from the buffer pool. Will acquire a write lock on the page the tuple is removed from.
	 * May block if the lock cannot be acquired.
	 * 
	 * Marks any pages that were dirtied by the operation as dirty by calling their markDirty bit. Does not need to
	 * update cached versions of any pages that have been dirtied, as it is not possible that a new page was created
	 * during the deletion (note difference from addTuple).
	 * 
	 * @param tid
	 *            the transaction adding the tuple.
	 * @param t
	 *            the tuple to add
	 */
	public void deleteTuple(TransactionId tid, Tuple t) throws DbException, TransactionAbortedException {
		// some code goes here
		// not necessary for assignment1
		int tableId = t.getRecordId().getPageId().getTableId(); 
		DbFile file = Database.getCatalog().getDbFile(tableId);
		HeapFile hp = (HeapFile) file;
		hp.deleteTuple(tid, t);
	}

	/**
	 * Flush all dirty pages to disk. NB: Be careful using this routine -- it writes dirty data to disk so will break
	 * simpledb if running in NO STEAL mode.
	 */
	public synchronized void flushAllPages() throws IOException {
		// some code goes here
		// not necessary for assignment1
		for(PageId pid: pages.keySet()){
		    flushPage(pid);
		}
	}

	/**
	 * Remove the specific page id from the buffer pool. Needed by the recovery manager to ensure that the buffer pool
	 * doesn't keep a rolled back page in its cache.
	 */
	public synchronized void discardPage(PageId pid) {
		// some code goes here
		// only necessary for assignment5
	}

	/**
	 * Flushes a certain page to disk
	 * 
	 * @param pid
	 *            an ID indicating the page to flush
	 */
	private synchronized void flushPage(PageId pid) throws IOException {
		// some code goes here
		// not necessary for assignment1
		Page page = pages.get(pid);
		if(page.isDirty() != null){
			
		  HeapFile heapFile = (HeapFile) Database.getCatalog().getDbFile(pid.getTableId());
		  heapFile.writePage(page);
		  page.markDirty(false, null);
		  
		}
	}

	/**
	 * Write all pages of the specified transaction to disk.
	 */
	public synchronized void flushPages(TransactionId tid) throws IOException {
		// some code goes here
		// not necessary for assignment1|assignment2|assignment3
	}

	/**
	 * Discards a page from the buffer pool. Flushes the page to disk to ensure dirty pages are updated on disk.
	 */
	private synchronized void evictPage() throws DbException {
		// some code goes here
		// not necessary for assignment1
		for(PageId pid: pages.keySet()){
			if(pages.get(pid).isDirty() != null){
				try {
					flushPage(pid);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}	
				
			} else{
				pages.remove(pid);
			}
		}
	    
	}

}
