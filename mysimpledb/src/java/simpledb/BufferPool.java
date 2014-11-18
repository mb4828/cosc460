package simpledb;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p/>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    private static LockManager lm;
    public static LockManager getLockManager() { return lm; }
	
    /**
     * Bytes per page, including header.
     */
    public static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;
    
    /**
     * Thread-safe hash map holding the buffer pool
     * key = PageId pageId
     * value = Page page
     */
    private ConcurrentHashMap<PageId,Page> bpool;
    
    /**
     * Thread-safe queue used to perform LRU page evictions.
     * Because most updates will affect the MRU page, the MRU page
     * will be stored at the HEAD of the queue and the LRU page will
     * be stored at the TAIL in order to increase efficiency.
     */
    private ConcurrentLinkedDeque<PageId> bqueue;
    
    /**
     * Maximum size that the buffer pool can be
     */
    private int maxsize;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    
    public BufferPool(int numPages) {
        bpool = new ConcurrentHashMap<PageId, Page>(numPages);
        bqueue = new ConcurrentLinkedDeque<PageId>();
        maxsize = numPages;
        lm = new LockManager();
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p/>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
    	
    	BufferPool.getLockManager().lockRequest(tid, pid, perm); 	// acquire lock on page
    	
    	synchronized (this) {
	        if (bpool.containsKey(pid))	{							// check if page is already in the buffer pool
	        	bqueue.remove(pid);									// update LRU queue to show that page is MRU
	        	bqueue.addFirst(pid);								// pid gets added to the HEAD of the queue
	        	return bpool.get(pid);								// return the page
	        }
	        
	        Catalog cat = Database.getCatalog();					// page is not in the buffer pool
	        DbFile db = cat.getDatabaseFile(pid.getTableId());		// retrieve the DbFile from catalog
	        Page pg = db.readPage(pid);								// read the required page from memory
	        
	        if (bpool.size() >= this.maxsize) {						// check if there is room in the buffer pool
	        	evictPage();										// buffer pool is full, so evict LRU page
	        }
	        
	        bpool.put(pid, pg);										// put the newly retrieved page in the buffer pool
	        bqueue.addFirst(pid);									// add the pid to the LRU queue
	        return pg;												// return the page to the caller
    	}
    }
    
    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) {
        BufferPool.getLockManager().lockRelease(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2|lab3|lab4                                                         // cosc460
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        return BufferPool.getLockManager().hasLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
            throws IOException {
    	if (commit) {
    		BufferPool.getLockManager().transactionCommit(tid);
    	} else {
    		BufferPool.getLockManager().transactionAbort(tid);
    	}
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed until lab5).                                  // cosc460
     * May block if the lock(s) cannot be acquired.
     * <p/>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have
     * been dirtied so that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        
    	DbFile dbf = Database.getCatalog().getDatabaseFile(tableId);	// retrieve database file
    	ArrayList<Page> dlist = dbf.insertTuple(tid, t);				// insert tuple
    	dirtyPageHelper(tid, dlist);									// update cache
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     * <p/>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have
     * been dirtied so that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
    	
    	int tableid = t.getRecordId().getPageId().getTableId();
    	DbFile dbf = Database.getCatalog().getDatabaseFile(tableid);	// retrieve database file
    	ArrayList<Page> dlist = dbf.deleteTuple(tid, t);				// delete tuple
    	dirtyPageHelper(tid, dlist);									// update cache
    }
    
    /**
     * Helper function for marking pages as dirty and updating their cache entry.
     * @param tid 	transaction id
     * @param dlist	list of pages to be updated
     */
    private void dirtyPageHelper(TransactionId tid, ArrayList<Page> dlist) {
    	ListIterator<Page> li = dlist.listIterator();
    	Page lp = null;
    	
    	while (li.hasNext()) {
    		lp = li.next();
    		lp.markDirty(true, tid);									// mark affected pages as dirty
    		
    		if (bpool.containsKey(lp.getId())) {						// update cache
    			bpool.replace(lp.getId(), lp);
    		} else {
    			bpool.put(lp.getId(), lp);
    		}
    	}
    }
    
    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
    	Enumeration<PageId> e = bpool.keys();
    	PageId pid = null;
    	
    	if (e.hasMoreElements()) {
    		pid = e.nextElement();
    	}
    	
    	while (e != null) {
    		HeapPage p = (HeapPage) bpool.get(pid);				// retrieve page from buffer pool
    		
    		if (p.isDirty() != null)							// check if page is dirty
        		flushPage( ((PageId) p.getId()) );				// flush the page
    		
    		if (e.hasMoreElements())							// get next pid
    			pid = e.nextElement();
    		else
    			break;
    	}
    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // only necessary for lab6                                                                            // cosc460
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
    	DbFile dbf = Database.getCatalog().getDatabaseFile(pid.getTableId());	// retrieve database file
    	HeapPage p = (HeapPage) bpool.get(pid);				// retrieve page from buffer pool
    	
    	if (p == null)
    		return;											// page wasn't in buffer pool so our work is done
    	
    	if (p.isDirty() == null)
    		return;											// page isn't dirty... we are done
    	
    	dbf.writePage(p);									// write page to disk
    	p.markDirty(false, null);							// set page as not dirty
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2|lab3|lab4                                                         // cosc460
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
    	Iterator<PageId> qit= bqueue.descendingIterator();		// in my implementation, the LRU page is at the tail
    	PageId pid = null;
    	
    	while (qit.hasNext()) {									// find the LRU page that is not dirty
    		pid = qit.next();
    		
    		if (bpool.get(pid).isDirty() == null) {
    			break;											// the page isn't dirty so we're done
    		}
    	}
    	
    	if (pid == null) {										// throw an exception if all pages are dirty
    		throw new DbException("all pages in buffer pool are dirty!");
    	}
    	
    	try {													// flush the page
    		flushPage(pid);
    	} catch (IOException e) {
    		throw new DbException("could not flush page");
    	}
    	
    	bqueue.remove(pid);										// remove it from the LRU queue and buffer pool
    	bpool.remove(pid);
    }

}
