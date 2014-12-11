package simpledb;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * @author mhay
 */
class LogFileRecovery {

    private final RandomAccessFile readOnlyLog;

    /**
     * Helper class for LogFile during rollback and recovery.
     * This class given a read only view of the actual log file.
     *
     * If this class wants to modify the log, it should do something
     * like this:  Database.getLogFile().logAbort(tid);
     *
     * @param readOnlyLog a read only copy of the log file
     */
    public LogFileRecovery(RandomAccessFile readOnlyLog) {
        this.readOnlyLog = readOnlyLog;
    }

    /**
     * Print out a human readable representation of the log
     */
    public void print() throws IOException {
        // since we don't know when print will be called, we can save our current location in the file
        // and then jump back to it after printing
        Long currentOffset = readOnlyLog.getFilePointer();

        readOnlyLog.seek(0);
        long lastCheckpoint = readOnlyLog.readLong(); // ignore this
        System.out.println("BEGIN LOG FILE");
        while (readOnlyLog.getFilePointer() < readOnlyLog.length()) {
            int type = readOnlyLog.readInt();
            long tid = readOnlyLog.readLong();
            switch (type) {
                case LogType.BEGIN_RECORD:
                    System.out.println("<T_" + tid + " BEGIN>");
                    break;
                case LogType.COMMIT_RECORD:
                    System.out.println("<T_" + tid + " COMMIT>");
                    break;
                case LogType.ABORT_RECORD:
                    System.out.println("<T_" + tid + " ABORT>");
                    break;
                case LogType.UPDATE_RECORD:
                    Page beforeImg = LogFile.readPageData(readOnlyLog);
                    Page afterImg = LogFile.readPageData(readOnlyLog);  // after image
                    System.out.println("<T_" + tid + " UPDATE pid=" + beforeImg.getId() +">");
                    break;
                case LogType.CLR_RECORD:
                    afterImg = LogFile.readPageData(readOnlyLog);  // after image
                    System.out.println("<T_" + tid + " CLR pid=" + afterImg.getId() +">");
                    break;
                case LogType.CHECKPOINT_RECORD:
                    int count = readOnlyLog.readInt();
                    Set<Long> tids = new HashSet<Long>();
                    for (int i = 0; i < count; i++) {
                        long nextTid = readOnlyLog.readLong();
                        tids.add(nextTid);
                    }
                    System.out.println("<T_" + tid + " CHECKPOINT " + tids + ">");
                    break;
                default:
                    throw new RuntimeException("Unexpected type!  Type = " + type);
            }
            long startOfRecord = readOnlyLog.readLong();   // ignored, only useful when going backwards thru log
        }
        System.out.println("END LOG FILE");

        // return the file pointer to its original position
        readOnlyLog.seek(currentOffset);

    }

    /**
     * Rollback the specified transaction, setting the state of any
     * of pages it updated to their pre-updated state.  To preserve
     * transaction semantics, this should not be called on
     * transactions that have already committed (though this may not
     * be enforced by this method.)
     *
     * This is called from LogFile.recover after both the LogFile and
     * the BufferPool are locked.
     *
     * @param tidToRollback The transaction to rollback
     * @throws java.io.IOException if tidToRollback has already committed
     */
    public void rollback(TransactionId tidToRollback) throws IOException {
    	Long initialOffset = readOnlyLog.getFilePointer();					// store initial offset so we can reset it later

    	long trollback = tidToRollback.getId();
        readOnlyLog.seek(readOnlyLog.length() - LogFile.LONG_SIZE);			// find final offset record
        
        while (readOnlyLog.getFilePointer() >= LogFile.LONG_SIZE) {
        	Long ptr = readOnlyLog.readLong();								// read the log entry's offset record
        	readOnlyLog.seek(ptr);											// seek to beginning of log entry
        	
        	int type = readOnlyLog.readInt();								// read the type of this log entry
        	long tid = readOnlyLog.readLong();								// read the tid of this log entry
        	
        	if (type == LogType.COMMIT_RECORD && trollback == tid) {
        		throw new IOException("transaction has already committed!");
        	}
        	else if (type == LogType.UPDATE_RECORD && trollback == tid) {
        		Page beforeImg = LogFile.readPageData(readOnlyLog);			// read beforeImg from log
        		int tableid = beforeImg.getId().getTableId();
        		
        		Database.getLogFile().logCLR(tid, beforeImg);				// write a CLR
        		
        		HeapFile hf = (HeapFile) Database.getCatalog().getDatabaseFile(tableid);
        		hf.writePage(beforeImg);									// write the beforeImg to the heapfile
        		
        		Database.getBufferPool().discardPage(beforeImg.getId());	// discard the page from the buffer pool
        	}
        	
        	if ((ptr - LogFile.LONG_SIZE) >= 0) {
        		readOnlyLog.seek(ptr - LogFile.LONG_SIZE); 					// seek to the previous log entry's offset record
        	} else {
        		break;														// we've reached the beginning of the log
        	}
        }
        
        Database.getLogFile().logAbort(trollback);
        readOnlyLog.seek(initialOffset);									// reset initial offset
    }

    /**
     * Recover the database system by ensuring that the updates of
     * committed transactions are installed and that the
     * updates of uncommitted transactions are not installed.
     *
     * This is called from LogFile.recover after both the LogFile and
     * the BufferPool are locked.
     */
    public void recover() throws IOException {
    	Long initialOffset = readOnlyLog.getFilePointer();
    	HashSet<Long> losers = new HashSet<Long>();
    	HashSet<Long> undone = new HashSet<Long>();
    	print();
    	
    	// locate the last checkpoint if one exists
    	boolean checkpointfound = false;
    	Long ptr = (long) -1;
    	readOnlyLog.seek(readOnlyLog.length() - LogFile.LONG_SIZE);
    	
    	while (readOnlyLog.getFilePointer() >= LogFile.LONG_SIZE) {
    		ptr = readOnlyLog.readLong();
    		readOnlyLog.seek(ptr);
    		int type = readOnlyLog.readInt();
    		
    		if (type == LogType.CHECKPOINT_RECORD) {
    			checkpointfound = true;
    			break;
    		}
    		
    		try { 
    			readOnlyLog.seek(ptr - LogFile.LONG_SIZE); 
    		} catch (IOException e) {
    			break;
    		}
    	}
    	
    	// redo phase
    	System.out.println("STARTING REDO");
    	if (checkpointfound) {
    		readOnlyLog.readLong();
    		int count = readOnlyLog.readInt();
            for (int i = 0; i < count; i++) {
                long nextTid = readOnlyLog.readLong();
                losers.add(nextTid);
            }
            readOnlyLog.readLong();		// skip start of record ptr
            
            //System.out.println("Checkpoint located with losers: " + losers);
            
    	} else {
    		if (ptr == (long) -1) {
    			throw new RuntimeException("log is missing");
    		}
    		readOnlyLog.seek(ptr);
    	}
    	
    	while (readOnlyLog.getFilePointer() < readOnlyLog.length()) {
    		int type = readOnlyLog.readInt();
    		long tid = readOnlyLog.readLong();
    		int tableid;
    		HeapFile hf;
    		
    		switch (type) {
	            case LogType.BEGIN_RECORD:
	            	System.out.println("begin found " + tid);
	                losers.add(tid);
	                break;
	            case LogType.COMMIT_RECORD:
	            	System.out.println("commit found " + tid);
	                losers.remove(tid);
	                break;
	            case LogType.ABORT_RECORD:
	            	System.out.println("abort found " + tid);
	            	losers.remove(tid);
	                break;
	            case LogType.UPDATE_RECORD:
	            	System.out.println("update found " + tid);
	                LogFile.readPageData(readOnlyLog);
	                Page afterImg = LogFile.readPageData(readOnlyLog);
	                
	                tableid = afterImg.getId().getTableId();
	                hf = (HeapFile) Database.getCatalog().getDatabaseFile(tableid);
	                hf.writePage(afterImg);
	                
	                break;
	            case LogType.CLR_RECORD:
	            	System.out.println("CLR found");
	                afterImg = LogFile.readPageData(readOnlyLog);
	                
	                tableid = afterImg.getId().getTableId();
	                hf = (HeapFile) Database.getCatalog().getDatabaseFile(tableid);
	                hf.writePage(afterImg);
	                
	                break;
	            case LogType.CHECKPOINT_RECORD:
	                throw new RuntimeException("Checkpoint record found - this should never happen!");
	            default:
	                throw new RuntimeException("Unexpected type!  Type = " + type);
    		}
    		readOnlyLog.readLong();   // skip start of record ptr
    	}
    	
    	// undo phase
    	System.out.println("STARTING UNDO with losers: " + losers);
    	readOnlyLog.seek(readOnlyLog.length() - LogFile.LONG_SIZE);
    	ptr = readOnlyLog.readLong();
    	readOnlyLog.seek(ptr);
    	
    	while (!losers.isEmpty()) {
    		int type = readOnlyLog.readInt();
    		long tid = readOnlyLog.readLong();
    		int tableid;
    		HeapFile hf;
    		
    		switch (type) {
	            case LogType.BEGIN_RECORD:
	            	System.out.println("begin found");
	                if (losers.contains(tid)) {
	                	losers.remove(tid);
	                	undone.add(tid);
	                }
	                break;
	            case LogType.UPDATE_RECORD:
	            	System.out.println("update found");
	            	if (!losers.contains(tid)) {
	            		LogFile.readPageData(readOnlyLog);
	            		LogFile.readPageData(readOnlyLog);
	            		break;
	            	}
	            	
	                Page beforeImg = LogFile.readPageData(readOnlyLog);
	                LogFile.readPageData(readOnlyLog);
	                	                
	                tableid = beforeImg.getId().getTableId();
	                hf = (HeapFile) Database.getCatalog().getDatabaseFile(tableid);
	                hf.writePage(beforeImg);
	                
	                Database.getLogFile().logCLR(tid, beforeImg);
	                
	                break;
	            case LogType.CLR_RECORD:
	                LogFile.readPageData(readOnlyLog);
	                break;
	            case LogType.CHECKPOINT_RECORD:
	            	int count = readOnlyLog.readInt();
	                for (int i = 0; i < count; i++) {
	                    readOnlyLog.readLong();
	                }
	                break;
	            default:
	            	System.out.println("something else found: " + type);
	            	break;
    		}
    		ptr = readOnlyLog.readLong();
    		
    		try { 
    			readOnlyLog.seek(ptr - LogFile.LONG_SIZE); 
    			ptr = readOnlyLog.readLong();
    	    	readOnlyLog.seek(ptr);
    		} catch (IOException e) {
    			break;
    		}
    	}
    	
    	// create a CLR for every transaction in undone
    	Iterator<Long> it = undone.iterator();
    	
    	while (it.hasNext()) {
    		long tid = it.next();
    		Database.getLogFile().logAbort(tid);
    	}
    	
    	// reset our seek position
    	readOnlyLog.seek(initialOffset);
    	
    	print();
    }
}
