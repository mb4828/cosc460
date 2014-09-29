package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;
    DbIterator iter1;
    DbIterator iter2;
    JoinPredicate pred;
    
    Tuple t1 = null;
    Tuple t2 = null;

    /**
     * Constructor. Accepts to children to join and the predicate to join them
     * on
     *
     * @param p      The predicate to use to join the children
     * @param child1 Iterator for the left(outer) relation to join
     * @param child2 Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, DbIterator child1, DbIterator child2) {
        this.pred = p;
        this.iter1 = child1;
        this.iter2 = child2;
    }

    public JoinPredicate getJoinPredicate() {
        return pred;
    }

    /**
     * @return the field name of join field1. Should be quantified by
     * alias or table name.
     */
    public String getJoinField1Name() {
        return iter1.getTupleDesc().getFieldName(pred.getField1());
    }

    /**
     * @return the field name of join field2. Should be quantified by
     * alias or table name.
     */
    public String getJoinField2Name() {
        return iter2.getTupleDesc().getFieldName(pred.getField2());
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     * implementation logic.
     */
    public TupleDesc getTupleDesc() {
        return TupleDesc.merge(iter1.getTupleDesc(), iter2.getTupleDesc());
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        iter1.open();
        iter2.open();
    	super.open();
    }

    public void close() {
        super.close();
        iter1.close();
        iter2.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        iter1.rewind();
        iter2.rewind();
        t1 = null;
        t2 = null;
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p/>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p/>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     *
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (t1 == null && iter1.hasNext()) {
        	// should execute first time only (t1 is null)
        	t1 = iter1.next();
        }
        
        while (t1 != null) {
        	while (iter2.hasNext()) {
        		t2 = iter2.next();
        		if (pred.filter(t1, t2)) {
        			return mergetup(t1, t2);
        		}
        	}
        	
        	if (!iter1.hasNext()) {
        		// we've reached the end of the table
        		break;
        	}
        	
        	t1 = iter1.next();
        	iter2.rewind();
        }
        
        return null;
    }

    private Tuple mergetup(Tuple t1, Tuple t2) {
    	Tuple newtup = new Tuple(getTupleDesc());
    	int size1 = t1.getTupleDesc().numFields();
    	
    	for (int i=0; i < t1.getTupleDesc().numFields(); i++)
    		newtup.setField(i, t1.getField(i));
    	for (int i=0; i < t2.getTupleDesc().numFields(); i++)
    		newtup.setField(i+size1, t2.getField(i));
    	
    	return newtup;
    }
    
    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[] {iter1, iter2};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        iter1 = children[0];
        iter2 = children[1];
    }

}
