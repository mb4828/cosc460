package simpledb;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
	private int[] hist;			// contains the histogram buckets
	private double[] thresh;	// contains the bucket thresholds (lower bounds)
	private int hmax;			// maximum value in the histogram
	private int hmin;			// minimum value in the histogram
	private int hbuckets;		// number of buckets
	private int htotal;			// total number of elements in the histogram
	
    /**
     * Create a new IntHistogram.
     * <p/>
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * <p/>
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * <p/>
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min     The minimum integer value that will ever be passed to this class for histogramming
     * @param max     The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	hmax = max;
        hmin = min;
        htotal = 0;
        
        // gracefully handle too extra buckets
        if (buckets > (max-min+1))
        	hbuckets = (max-min+1);
        else
        	hbuckets = buckets;
        
        hist = new int[hbuckets];
        thresh = new double[hbuckets];
        
        // initialize hist array
        for (int i=0; i < hist.length; i++) {
        	hist[i] = 0;
        }
        
        // initialize thresh array
        for (int i=0; i < thresh.length; i++) {
        	thresh[i] = getLowerBound(i);
        }
    }

    private int whichBucket(int v) {
    	if (thresh.length == 1)
    		return 0;
    	
    	if (v >= thresh[thresh.length-1] && v <= hmax)
    		return hbuckets-1;							// value belongs in the final bucket
    	
    	for (int i=0; i < thresh.length-1; i++) {
    		if (v >= thresh[i] && v < thresh[i+1])
    			return i;
    	}
    	
    	return -1;
    }
    
    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        if (v < hmin || v > hmax)
        	throw new RuntimeException(v + " is not in the range " + hmin + " to " + hmax);
        
        hist[whichBucket(v)] += 1;  
        htotal++;
        
        //System.out.println("inserted value " + v + " into bucket " + whichBucket(v));
    }
    
    /**
     * Gets the range of the bucket
     */
    private double getWidth(int bucketnum) {
    	if (bucketnum != (hbuckets-1))
    		return (hmax-hmin+1) / hbuckets;
    		
    	else
    		return (hmax+1-thresh[thresh.length-1]);
    }
    
    /**
     * Estimates the frequency of one value in the histogram
     */
    private double getBucketSlice(int v) {
    	int bucketnum = whichBucket(v);
    	System.out.println("w="+getWidth(bucketnum));
    	return hist[bucketnum]/getWidth(bucketnum);
    }
    
    /**
     * Returns the lower bound of the bucket which contains v
     */
    private double getLowerBound(int bucketnum) {
    	double interval = (hmax-hmin+1) / hbuckets;
    	
    	if (interval == 0)
    		interval = 1;
    	
    	return (hmin + bucketnum*interval);
    }
    
    /**
     * Returns a count of the values in the histogram strictly less than v
     */
    private double getLowerCount(int v) {
    	double count = 0;
    	int bucketnum = whichBucket(v);
    	
    	// add full buckets to the left of v
    	for (int i=0; i<bucketnum; i++)
    		count += hist[i];
    	
    	// add partial buckets to the left of v
    	count += (hist[bucketnum] / getWidth(bucketnum)) * (v - ((int) getLowerBound(bucketnum)));
    	
    	return count;
    }
    
    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * <p/>
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v  Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	double fcount = 0;
    	boolean flag = false;
    	double slice = 0;
    	double lowercount = 0;
    	
       	// check for out-of-range v
    	if (v < hmin || v > hmax) {
    		flag = true;
    	}
    	
    	if (!flag) {
	    	slice = getBucketSlice(v);
	    	lowercount = getLowerCount(v);
	    	
	    	System.out.println("lowercount = " + lowercount);
	    	System.out.println("slice = " + slice);
	    	System.out.println("total = " + htotal);
    	}
    	
    	if (op.equals(Predicate.Op.EQUALS)) {
    		// prediate is equals
    		if (!flag)
    			fcount = slice;
    		else
    			fcount = 0;
    		
    	} else if (op.equals(Predicate.Op.NOT_EQUALS)) {
    		// predicate is not equals
    		if (!flag)
    			fcount = (htotal - slice);
    		else
    			fcount = htotal;
    		
    	} else if (op.equals(Predicate.Op.LESS_THAN) || op.equals(Predicate.Op.LESS_THAN_OR_EQ)) {
    		// add lower count, save slice for later
    		if (!flag)
    			fcount += lowercount;
    		else if (v < hmin)
    			fcount = 0;
    		else if (v > hmin)
    			fcount = htotal;
    		
    	} else if (op.equals(Predicate.Op.GREATER_THAN) || op.equals(Predicate.Op.GREATER_THAN_OR_EQ)) {
    		// add inverse of lower count, save slice for later
    		if (!flag)
    			fcount += htotal - lowercount;
    		else if (v < hmin)
    			fcount = htotal;
    		else
    			fcount = 0;
    		
    	}
    	
    	if (op.equals(Predicate.Op.LESS_THAN_OR_EQ) && (!flag)) {
    		// add slice in if necessary
    		fcount += slice;
	    } else if (op.equals(Predicate.Op.GREATER_THAN) && (!flag)) {
	    	// subtract slice if necessary
	    	fcount -= slice;
	    }
    	
    	System.out.println("selectivity = " + fcount/htotal);
    	return fcount / htotal;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
    	String output = new String();
    	output += "{" + ((double) hmin) + ", " + hist[0] + "} ";
        
        for (int i=1; i < hist.length; i++) {
        	output += "{" + thresh[i] + ", " + hist[i] + "} ";
        }
    	
    	return output;
    }
    
}
