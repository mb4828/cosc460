package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;
    private Predicate pred;
    private DbIterator iter;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     *
     * @param p     The predicate to filter tuples with
     * @param child The child operator
     */
    public Filter(Predicate p, DbIterator child) {
        pred = p;
        iter = child;
    }

    public Predicate getPredicate() {
        return pred;
    }

    public TupleDesc getTupleDesc() {
        return iter.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
    	iter.open();
    	super.open();    
    }

    public void close() {
        super.close();
        iter.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        iter.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     *
     * @return The next tuple that passes the filter, or null if there are no
     * more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        
    	while (iter.hasNext()) {
        	Tuple t = iter.next();
        	if (pred.filter(t)) {
        		//System.out.println("returning tuple: " + t.toString());
        		return t;
        	}
        }
    	//System.out.println("returning null");
        return null;
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
