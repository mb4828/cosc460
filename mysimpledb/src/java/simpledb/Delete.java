package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId tid;
    private DbIterator iter;
    private boolean done = false;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     *
     * @param t     The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        this.tid = t;
        this.iter = child;
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     *
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	if (done) {
    		// delete has been called more than once
        	return null;
        }
    	
    	int count = 0;
        while (iter.hasNext()) {
        	Tuple t = iter.next();
        	try {
				Database.getBufferPool().deleteTuple(tid, t);
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
