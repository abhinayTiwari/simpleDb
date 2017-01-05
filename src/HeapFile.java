package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples in no particular order. Tuples are
 * stored on pages, each of which is a fixed size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

	/**
	 * The File associated with this HeapFile.
	 */
	protected File file;

	/**
	 * The TupleDesc associated with this HeapFile.
	 */
	protected TupleDesc td;

	/**
	 * Constructs a heap file backed by the specified file.
	 * 
	 * @param f
	 *            the file that stores the on-disk backing store for this heap file.
	 */
	public HeapFile(File f, TupleDesc td) {
		this.file = f;
		this.td = td;
	}

	/**
	 * Returns the File backing this HeapFile on disk.
	 * 
	 * @return the File backing this HeapFile on disk.
	 */
	public File getFile() {
		return file;
	}

	/**
	 * Returns an ID uniquely identifying this HeapFile. Implementation note: you will need to generate this tableid
	 * somewhere ensure that each HeapFile has a "unique id," and that you always return the same value for a particular
	 * HeapFile. We suggest hashing the absolute file name of the file underlying the heapfile, i.e.
	 * f.getAbsoluteFile().hashCode().
	 * 
	 * @return an ID uniquely identifying this HeapFile.
	 */
	public int getId() {
		return file.getAbsoluteFile().hashCode();
	}

	/**
	 * Returns the TupleDesc of the table stored in this DbFile.
	 * 
	 * @return TupleDesc of this DbFile.
	 */
	public TupleDesc getTupleDesc() {
		return td;
	}

	// see DbFile.java for javadocs
	public Page readPage(PageId pid) {
		try {
			RandomAccessFile r= new RandomAccessFile(file, "r");
			int pgSize = pid.pageno()*BufferPool.PAGE_SIZE;
			byte[] data = new byte[BufferPool.PAGE_SIZE];
			r.seek(pgSize);
			r.read(data, 0, BufferPool.PAGE_SIZE);
			HeapPageId hid= (HeapPageId) pid;
			return new HeapPage(hid, data);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		throw new UnsupportedOperationException("Implement this");
	}

	// see DbFile.java for javadocs
	public void writePage(Page page) throws IOException {
		// some code goes here
		// not necessary for assignment1
	}

	/**
	 * Returns the number of pages in this HeapFile.
	 */
	public int numPages() {
		return (int) (file.length() / BufferPool.PAGE_SIZE);
	}

	// see DbFile.java for javadocs
	public ArrayList<Page> addTuple(TransactionId tid, Tuple t) throws DbException, IOException,
			TransactionAbortedException {
		// some code goes here
		// not necessary for assignment1
		return null;
	}

	// see DbFile.java for javadocs
	public Page deleteTuple(TransactionId tid, Tuple t) throws DbException, TransactionAbortedException {
		// some code goes here
		// not necessary for assignment1
		RecordId reCordId=t.getRecordId();
        PageId pageId= reCordId.getPageId();
        Page page= Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
        HeapPage heapPage = (HeapPage)page;
        heapPage.deleteTuple(t);
        
        page = Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
        return page;
    
	}

	// see DbFile.java for javadocs

	public DbFileIterator iterator(TransactionId tid) {
		HeapPage page;
		List<Tuple> l = new ArrayList<Tuple>();
 		for(int i = 0 ; i< numPages(); i++ ){
         
 		    try {
 		    	page=(HeapPage)(Database.getBufferPool().getPage(tid, new HeapPageId(getId(), i) , Permissions.READ_ONLY)); 
 		    	page.getPageData();
 		    	Iterator<Tuple> iter = page.iterator();
 		    	while(iter.hasNext()){
 		    		l.add(iter.next());
 		    	}
 		    	
			} catch (TransactionAbortedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (DbException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
 		}
 		return new MyDbIterator (l);
	}

}

//public class tupleItrate implements DbFileIterator{
 // int pgNumber = 0;
 // int tid;
 // PageId pid = new HeapPageId(getId(), pgNumber);
//Make HEapPageID object with tid, pgNumber return pid
// call open()

//}