package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {

    private static final long serialVersionUID = 1L;
    private DbFileIterator dbi;
    private int tableid;
    private String tableAlias;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid        The transaction this scan is running as a part of.
     * @param tableid    the table to scan.
     * @param tableAlias the alias of this table (needed by the parser); the returned
     *                   tupleDesc should have fields with name tableAlias.fieldName
     *                   (note: this class is not responsible for handling a case where
     *                   tableAlias or fieldName are null. It shouldn't crash if they
     *                   are, but the resulting name can be null.fieldName,
     *                   tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        dbi = Database.getCatalog().getDatabaseFile(tableid).iterator(tid);
        this.tableid = tableid;
        this.tableAlias = tableAlias;
    }

    /**
     * @return return the table name of the table the operator scans. This should
     * be the actual name of the table in the catalog of the database
     */
    public String getTableName() {
        return Database.getCatalog().getTableName(this.tableid);
    }

    /**
     * @return Return the alias of the table this operator scans.
     */
    public String getAlias() {
        return this.tableAlias;
    }

    public SeqScan(TransactionId tid, int tableid) {
        this(tid, tableid, Database.getCatalog().getTableName(tableid));
    }

    public void open() throws DbException, TransactionAbortedException {
        dbi.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        TupleDesc td= Database.getCatalog().getTupleDesc(this.tableid);
        
        Iterator<TupleDesc.TDItem> tdi = td.iterator();
        int numfields = td.numFields();
        Type[] typeAr = new Type[numfields];
        String[] fieldAr = new String[numfields];
        
        int i=0;
        while (tdi.hasNext()) {
        	TupleDesc.TDItem item = tdi.next();
        	typeAr[i] = item.fieldType;
        	fieldAr[i] = this.tableAlias + "." + item.fieldName;
        	i++;
        }
        
        return new TupleDesc(typeAr,fieldAr);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        return dbi.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        return dbi.next();
    }

    public void close() {
        dbi.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        dbi.rewind();
    }
}
