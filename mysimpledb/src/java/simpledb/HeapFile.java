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
        
        // tuple info
        private Iterator<Tuple> tupit;
    	
        public dbIterator() throws TransactionAbortedException, DbException {
        	loadNextPage();
        }
        
        public void open() {
        	this.closed = false;
        }
        
        public void close() {
        	this.closed = true;
        }

        private void loadNextPage() throws TransactionAbortedException, DbException {
        	this.current_pid = new HeapPageId(this.tableId, this.nextpg);
            this.current_page = (HeapPage) this.bp.getPage(null, this.current_pid, null);
            this.tupit = this.current_page.iterator();
            nextpg++;
        }
        
        public boolean hasNext() throws DbException {
        	if (this.closed)
        		return false;
        	
        	//System.out.println("tupithn: "+tupit.hasNext()+", nextpg: "+nextpg+", numpgs: "+numpages);
            if (this.tupit.hasNext() || (this.nextpg < this.numpages))
            	return true;
            return false;
        }

        public Tuple next() throws TransactionAbortedException, DbException {
        	if (this.closed)
        		throw new NoSuchElementException("The iterator is closed");
            if (!hasNext())
                throw new NoSuchElementException();
            
            if (this.tupit.hasNext()) {
            	// request the next tuple from the iterator
            	//System.out.println("NEXT");
            	return this.tupit.next();
            } else {
            	// request the next page, first tuple
            	//System.out.println("NEXT PAGE");
            	loadNextPage();
            	return this.tupit.next();
            }
        }

        public void rewind() throws TransactionAbortedException, DbException {
        	if (this.closed)
        		throw new DbException("The iterator is closed");
        	this.nextpg = 0;
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
    public Page readPage(PageId pid) {
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
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
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
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        try {
			return new dbIterator();
        } catch (TransactionAbortedException e) {
			throw new IllegalStateException("Could not create dbfileiterator");
        } catch (DbException e) {
        	throw new IllegalStateException("Could not create dbfileiterator");
        }
    }

}

