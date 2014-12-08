package simpledb;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashSet;
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

        // some code goes here

    }
}
