package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         */
        public final Type fieldType;

        /**
         * The name of the field
         */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    private static final long serialVersionUID = 1L;
    private TDItem[] tdlist;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr  array specifying the number of and types of fields in this
     *                TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may
     *                be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        if (typeAr.length < 1 )
        	throw new RuntimeException("typeAr must contain at least one entry");
        
        int l = typeAr.length;
        this.tdlist = new TDItem[l];
        
        for (int i=0; i<l; i++) {
        	if (fieldAr == null)
        		this.tdlist[i] = new TDItem(typeAr[i], null);
        	else
        		this.tdlist[i] = new TDItem(typeAr[i],fieldAr[i]);
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in this
     *               TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        this(typeAr, null);
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return this.tdlist.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if (i < 0 || i >= this.tdlist.length)
        	throw new NoSuchElementException(i+" is not a valid field reference");
        
        return this.tdlist[i].fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid
     *          index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
    	if (i < 0 || i >= this.tdlist.length)
        	throw new NoSuchElementException(i+" is not a valid field reference");
    	
    	return this.tdlist[i].fieldType;
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
    	int i = 0;
    	int l = this.tdlist.length;
    	
        for (; i < l; i++) {
        	if (this.tdlist[i].fieldName == null)
        		continue;
        	if (this.tdlist[i].fieldName.equals(name))
        		break;
        }
        
        if (i>=l)
        	throw new NoSuchElementException("field " + name + " not found");
        
        return i;
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int final_size = 0;
        int l = this.tdlist.length;
        
        for (int i=0; i<l; i++)
        	final_size += this.tdlist[i].fieldType.getLen();
        
        return final_size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        int l1 = td1.tdlist.length;
        int l2 = td2.tdlist.length;
        
        Type[] typeAr = new Type[l1+l2];
        String[] fieldAr = new String[l1+l2];
        
        for (int i=0; i<l1; i++)
        	typeAr[i] = td1.tdlist[i].fieldType;
        for (int i=0; i<l2; i++)
        	typeAr[l1+i] = td2.tdlist[i].fieldType;
        
        for (int i=0; i<l1; i++)
        	fieldAr[i] = td1.tdlist[i].fieldName;
        for (int i=0; i<l2; i++)
        	fieldAr[l1+i] = td2.tdlist[i].fieldName;
        
        return new TupleDesc(typeAr, fieldAr);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
    	if (!(o instanceof TupleDesc))
    		return false;
    	
        int l1 = this.tdlist.length;
        int l2 =  ((TupleDesc) o).tdlist.length;
        
        if (l1 != l2)
        	return false;
        
        for (int i=0; i<l1; i++) {
        	if (this.tdlist[i].fieldType != ((TupleDesc) o).tdlist[i].fieldType)
        		return false;
        }
        
        return true;
    }

    public int hashCode() {
    	String hashstr = "";
    	int l = this.numFields();
    	
    	for (int i=0; i<l; i++)
    		hashstr += this.tdlist[i].toString();
    	
    	return hashstr.hashCode();
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldName[0](fieldType[0]), ..., fieldName[M](fieldType[M])"
     *
     * @return String describing this descriptor.
     */
    public String toString() {
        String output = "";
        int l = this.tdlist.length;
        
        for (int i=0; i < l; i++) {
        	output += tdlist[i].toString();
        	if (i < l-1)
        		output += ", ";
        }
        
        return output;
    }

    /**
     * @return An iterator which iterates over all the field TDItems
     * that are included in this TupleDesc
     */
    public Iterator<TDItem> iterator() {
        return Arrays.asList(this.tdlist).iterator();
    }

}
