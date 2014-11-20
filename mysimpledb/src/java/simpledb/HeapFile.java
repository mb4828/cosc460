package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see simpledb.HeapPage#HeapPage
 */
public class HeapFile implements DbFile {
	
    public class dbIterator implements DbFileIterator {
    	// database
    	private boolean closed = true;
    	private BufferPool bp = Database.getBufferPool();
    	
    	// page information
    	private int numpages = numPages();
    	private int tableId = getId();
    	private int nextpg = 0;
    	private PageId current_pid;
        private HeapPage current_page;
        private TransactionId tid;
        
        // tuple info
        private Iterator<Tuple> tupit;
        private Tuple nexttup;
    	
        public dbIterator(TransactionId tid) {
        	this.tid = tid;
        	loadNextPage();
        }
        
        public void open() {
        	closed = false;
        }
        
        public void close() {
        	closed = true;
        }

        private void loadNextPage() {
        	current_pid = new HeapPageId(this.tableId, this.nextpg);
        	
        	try {
        		current_page = (HeapPage) this.bp.getPage(tid, this.current_pid, Permissions.READ_ONLY);
        	} catch (TransactionAbortedException e) {
        		throw new RuntimeException("uh oh; this should never happen!");
        	} catch (DbException e) {
        		throw new RuntimeException("uh oh; this should never happen!");
        	}
        	
            tupit = current_page.iterator();
            nexttup = null;
            nextpg++;
        }
        
        public boolean hasNext() {
        	if (this.closed)
        		return false;
        	
        	//System.out.println("tupithn: "+tupit.hasNext()+", nextpg: "+nextpg+", numpgs: "+numpages);
        	
        	// do we still have tuples on this page?
        	if (nexttup != null) {
        		return true;
        		
        	} else if (tupit.hasNext()) {
        		nexttup = tupit.next();
        		return true;
        		
        	} else {
        		// do we still have pages left?
        		if (nextpg < numpages) {
        			loadNextPage();
        			return hasNext();
        			
        		} else {
        			return false;	// out of tuples and out of pages
        		}
        		
        	}
        }

        public Tuple next() throws TransactionAbortedException, DbException {
        	if (closed)
        		throw new NoSuchElementException("The iterator is closed");
            if (!hasNext())
                throw new NoSuchElementException();
            
            Tuple temp = nexttup;
            nexttup = null;
            return temp;
        }

        public void rewind() throws TransactionAbortedException, DbException {
        	if (closed)
        		throw new DbException("The iterator is closed");
        	nextpg = 0;
        	loadNextPage();
        }
    }
	
	File hf;
	TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.hf = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return this.hf;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return this.hf.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return this.td;
    }

    // see DbFile.java for javadocs
    public synchronized Page readPage(PageId pid) {
        if (pid.pageNumber() < 0 || pid.pageNumber() > this.numPages())
        	throw new NoSuchElementException("page "+pid.pageNumber()+" is out of bounds");
        
        int page_size = BufferPool.getPageSize();
        byte[] data = new byte[page_size];
        
        try {
	        // create a new buffered reader and buffer
	        InputStream in = new FileInputStream(this.hf);
	        
	        // skip to the page we want to read
	        in.skip(pid.pageNumber()*page_size);
	        
	        // read the page
	        in.read(data,0,page_size);
	        in.close();
	        
	        return new HeapPage((HeapPageId) pid, data);
	        
        } catch (IOException io) {
        	throw new IllegalStateException("something didn't work with reading the file!");
        }
    }

    // see DbFile.java for javadocs
    public synchronized void writePage(Page page) throws IOException {
    	RandomAccessFile out = new RandomAccessFile(this.hf, "rw");
    	byte[] data = ((HeapPage) page).getPageData();
    	int page_offset = page.getId().pageNumber()*BufferPool.getPageSize();
    	
    	if (page_offset >= out.length()) {
    		long newlength = out.length()+data.length+(out.length()-page_offset);
    		out.setLength(newlength);
    	}
    	
    	out.seek(page_offset);
    	out.write(data, 0, data.length);
    	out.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) (this.hf.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        ArrayList<Page> output = new ArrayList<Page>();
        
        BufferPool bp = Database.getBufferPool();
        PageId pid = null;
        HeapPage p = null;
        int numpages = numPages();
        int tableid = getId();
        
        int i=0;
        for (; i<numpages; i++) {							// iterate through the pages in the heapfile
        	boolean donotrelease = false;
        	pid = new HeapPageId(tableid, i);
        	
        	if (bp.holdsLock(tid, pid)) {
        		donotrelease = true;
        	}
        	
        	p = (HeapPage) bp.getPage(tid, pid, Permissions.READ_WRITE);
        	
        	if (p.getNumEmptySlots() > 0) {					// does the page have a free slot?
        		break;
        	}
        	
        	if (!donotrelease) {
    			bp.releasePage(tid, p.getId());				// release page we didn't touch
    		}
        }
        
        if (i<numpages) {									// we found an empty slot on a pre-existing page
        	p.insertTuple(t);
        	//System.out.println("inserted tuple on page "+pid.pageNumber() + ", " + p.getNumEmptySlots() + " slots left");
        } else {											// no free slots; need to create a new page
        	pid = new HeapPageId(tableid, i);
        	p = new HeapPage((HeapPageId) pid, HeapPage.createEmptyPageData());
        	writePage(p);									// write the page to the heapfile
        	p = (HeapPage) bp.getPage(tid, pid, Permissions.READ_WRITE);		// call getpage to load the page into the buffer
        	p.insertTuple(t);
        	
        	//System.out.println("inserted tuple on a NEW PAGE");
        }
        
        output.add(p);
        return output;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        ArrayList<Page> output = new ArrayList<Page>();
        
        PageId pid = t.getRecordId().getPageId();
        BufferPool bp = Database.getBufferPool();
        HeapPage p = (HeapPage) bp.getPage(tid, pid, Permissions.READ_WRITE);

		p.deleteTuple(t);
		p.markDirty(true, tid);
        
        output.add(p);
        return output;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
    	return new dbIterator(tid);
    }

}

