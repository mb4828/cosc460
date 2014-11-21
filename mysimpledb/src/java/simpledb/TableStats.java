package simpledb;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.HashSet;

import simpledb.Predicate.Op;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query.
 * <p/>
 * This class is not needed in implementing lab1|lab2|lab3.                                                   // cosc460
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }

    public static void setStatsMap(HashMap<String, TableStats> s) {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    private Object[] hists;
    private Field[] maxvals;
    private Field[] minvals;
    private int[] distinctvals;
    private double scanCost;
    private int numtups = 0;
    
    private void populateMaxMinNumtups(DbFileIterator tupit, int numfields) {
    	//System.out.println("finding the min and max for each field...");
    	Tuple t;
    	Field f;
    	
    	maxvals = new Field[numfields];
    	minvals = new Field[numfields];
    	
    	try {
    		tupit.open();
    		
    		if (tupit.hasNext()) {
    			// set initial values for each field
    			t = tupit.next();
    			
    			for (int i=0; i<numfields; i++) {
    				f = t.getField(i);
    				
    				// is this a string field or an int field?
    				if (f.getType().equals(Type.INT_TYPE)) {
    					maxvals[i] = t.getField(i);
        				minvals[i] = t.getField(i);
    				} else {
    					maxvals[i] = new StringField("",1);
    					minvals[i] = new StringField("",1);
    				}
    			}
    			
    			numtups++;
    		}
    		
    		while (tupit.hasNext()) {
    			// get actual max and min for each field
    			t = tupit.next();
    			
    			for (int i=0; i<numfields; i++) {
    				f = t.getField(i);
    				
    				// is this a string field or an int field?
    				if (f.getType().equals(Type.INT_TYPE)) {
	    				if (f.compare(Op.GREATER_THAN, maxvals[i])) {
	    					maxvals[i] = f;
	    				} else if (f.compare(Op.LESS_THAN, minvals[i])) {
	    					minvals[i] = f;
	    				}
	    				
    				} else {
    					continue;
    				}
    			}
    			
    			numtups++;
    		}
    		
    		tupit.rewind();
    		tupit.close();
    		
    	} catch (Exception e) {
    		System.out.println(e);
    	}
    	
    	//System.out.println("done");
    }
    
    private void populateHists(DbFileIterator tupit) {
    	if (maxvals == null || minvals == null) {
    		throw new RuntimeException("you haven't called populateMinMaxNumtups yet dummy!");
    	}
    	
    	//System.out.println("creating histograms...");
    	
    	// declare the hists array
    	hists = new Object[maxvals.length];
    	
    	for (int i=0; i<hists.length; i++) {
    		// initialize new int or string histograms
    		if (maxvals[i].getType().equals(Type.INT_TYPE)) {
    			hists[i] = new IntHistogram(NUM_HIST_BINS, Integer.parseInt(minvals[i].toString()), Integer.parseInt(maxvals[i].toString()));
    		} else {
    			hists[i] = new StringHistogram(NUM_HIST_BINS);
    		}
    	}
    	
    	// create histograms
    	try {
    		tupit.open();
    		
    		while (tupit.hasNext()) {
    			Tuple t = tupit.next();
    			
    			// add int or string values to histogram
	    		for (int i=0; i<hists.length; i++) {
	    			if (t.getField(i).getType().equals(Type.INT_TYPE)) {
	    				((IntHistogram) hists[i]).addValue(Integer.parseInt(t.getField(i).toString()));
	    			} else {
	    				((StringHistogram) hists[i]).addValue(t.getField(i).toString());
	    			}
	    		}	
    		}
    		
    		tupit.rewind();
    		tupit.close();
    		
    	} catch (Exception e) {
    		System.out.println(e);
    	}
    	
    	//System.out.println("done");
    }
    
    private void populateDistinctvals(DbFileIterator tupit) {
    	if (maxvals == null) {
    		throw new RuntimeException("you haven't called populateMinMaxNumtups yet dummy!");
    	}
    	
    	// initialize the distinctvals array
    	distinctvals = new int[maxvals.length];
    	
    	// create a temporary array of hashsets
    	Object[] tempmap = new Object[distinctvals.length];
    	
    	for (int i=0; i < tempmap.length; i++) {
    		if (hists[i] instanceof IntHistogram) {
    			tempmap[i] = new HashSet<Integer>();
    		} else {
    			tempmap[i] = new HashSet<String>();
    		}
    	}
    	
    	// hash each tuple while checking for distinct values
    	try {
    		tupit.open();
    		
	    	while (tupit.hasNext()) {
	    		Tuple t = tupit.next();
	    		
	    		for (int i=0; i<tempmap.length; i++) {
	    			Field f = t.getField(i);
	    			
	    			// is the field and int field or string field?
	    			if (f.getType().equals(Type.INT_TYPE)) {
	    				if (!((HashSet<Integer>) tempmap[i]).contains(Integer.parseInt(f.toString()))) {
	    					((HashSet<Integer>) tempmap[i]).add(Integer.parseInt(f.toString()));
	    					distinctvals[i]++;	
	    				}
	    				
	    			} else {
	    				if (!((HashSet<String>) tempmap[i]).contains(f.toString())) {
	    					((HashSet<String>) tempmap[i]).add(f.toString());
	    					distinctvals[i]++;
	    				}
	    			}
	    		}
	    	}
	    	
	    	tupit.rewind();
	    	tupit.close();
	    	
    	} catch (Exception e) {
    		System.out.println(e);
    	}
    	
    }
    
    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     *
     * @param tableid       The table over which to compute statistics
     * @param ioCostPerPage The cost per page of IO. This doesn't differentiate between
     *                      sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        
    	// calculate scan cost
    	int numpages = ((HeapFile) Database.getCatalog().getDatabaseFile(tableid)).numPages();
    	scanCost = numpages * ioCostPerPage; 
    	
    	// acquire an iterator over the DbFile for the table
    	TransactionId tid = new TransactionId();
    	DbFileIterator tupit;
		try {
			tupit = Database.getCatalog().getDatabaseFile(tableid).iterator(tid);
		} catch (NoSuchElementException e) {
			throw new RuntimeException("something went wrong");
		} catch (TransactionAbortedException e) {
			throw new RuntimeException("something went wrong");
		} catch (DbException e) {
			throw new RuntimeException("something went wrong");
		}
    	
    	// get the max, min, numtups
    	populateMaxMinNumtups(tupit, Database.getCatalog().getDatabaseFile(tableid).getTupleDesc().numFields());
    	
    	// create histograms
    	populateHists(tupit);
    	
    	// calculate distinct values
    	populateDistinctvals(tupit);
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * <p/>
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     *
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        return scanCost;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     *
     * @param selectivityFactor The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     * selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
    	if (selectivityFactor > 0 && selectivityFactor < (1.0/numtups)) {
    		return 1;
    	}
        return (int) (numtups * selectivityFactor);
    }

    /**
     * This method returns the number of distinct values for a given field.
     * If the field is a primary key of the table, then the number of distinct
     * values is equal to the number of tuples.  If the field is not a primary key
     * then this must be explicitly calculated.  Note: these calculations should
     * be done once in the constructor and not each time this method is called. In
     * addition, it should only require space linear in the number of distinct values
     * which may be much less than the number of values.
     *
     * @param field the index of the field
     * @return The number of distinct values of the field.
     */
    public int numDistinctValues(int field) {
        return distinctvals[field];
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     *
     * @param field    The field over which the predicate ranges
     * @param op       The logical operation in the predicate
     * @param constant The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     * predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
    	if (hists[field] instanceof IntHistogram) {
    		return ((IntHistogram) hists[field]).estimateSelectivity(op, Integer.parseInt(constant.toString()));
    	} else {
    		return ((StringHistogram) hists[field]).estimateSelectivity(op, constant.toString());
    	}
    }

}
