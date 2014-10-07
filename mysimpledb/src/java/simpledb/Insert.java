package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId tid;
    private DbIterator iter;
    private int tableid;
    private boolean done = false;
    
    /**
     * Constructor.
     *
     * @param t       The transaction running the insert.
     * @param child   The child operator from which to read tuples to be inserted.
     * @param tableid The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to
     *                     insert.
     */
    public Insert(TransactionId t, DbIterator child, int tableid)
            throws DbException {
        this.tid = t;
        this.iter = child;
        this.tableid = tableid;
    }

    public TupleDesc getTupleDesc() {
        return new TupleDesc(new Type[] {Type.INT_TYPE});
    }

    public void open() throws DbException, TransactionAbortedException {
        iter.open();
        super.open();
    }

    public void close() {
        super.close();
        iter.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        iter.rewind();
        done = false;
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     * null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	if (done) {
    		// insert has been called more than once
        	return null;
        }
    	
    	int count = 0;
        while (iter.hasNext()) {
        	Tuple t = iter.next();
        	try {
				Database.getBufferPool().insertTuple(tid, tableid, t);
			} catch (IOException e) {
				throw new DbException("IOException: "+e);
			}
        	count++;
        }
        
        Tuple r = new Tuple(getTupleDesc());
        r.setField(0, new IntField(count));
        
        done = true;		// can only run once!
        return r;
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[] {iter};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        iter = children[0];
    }
}
