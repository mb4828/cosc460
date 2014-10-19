package simpledb;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
	private int[] hist;			// contains the histogram buckets
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
        
        // gracefully handle too many buckets
        if (buckets > (max-min+1))
        	hbuckets = (max-min+1);
        else
        	hbuckets = buckets;
        
        hist = new int[hbuckets];
        
        for (int i=0; i < hist.length; i++) {
        	hist[i] = 0;
        } 
        
        System.out.println("NEW HISTOGRAM: buckets=" + hbuckets + ", min=" + hmin + ", max=" + hmax);
    }

    private int whichBucket(int v) {
    	if (v == hmax)
    		return hbuckets-1;			// value belongs in the final bucket
    	return (v-hmin) % hbuckets;
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
    		return (hmax-hmin+1) % hbuckets;
    }
    
    /**
     * Estimates the frequency of one value in the histogram
     */
    private double getBucketSlice(int v) {
    	int bucketnum = whichBucket(v);
    	return hist[bucketnum]/getWidth(bucketnum);
    }
    
    /**
     * Returns the lower bound of the bucket which contains v
     */
    private int getLowerBound(int v, int bucketnum) {
    	int interval = (hmax-hmin+1) % hbuckets;
    	return hmin + bucketnum*interval;
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
    	count += (hist[bucketnum] / getWidth(bucketnum)) * (v - getLowerBound(v, bucketnum));
    	
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
    	double slice = getBucketSlice(v);
    	
    	if (op.equals(Predicate.Op.EQUALS)) {
    		// prediate is equals
    		return slice/htotal;
    	}
    	if (op.equals(Predicate.Op.NOT_EQUALS)) {
    		// predicate is not equals
    		return (htotal - slice) / htotal;
    	}
    	
    	double fcount = 0;
    	double lowercount = getLowerCount(v);
    	
    	if (op.equals(Predicate.Op.LESS_THAN) || op.equals(Predicate.Op.LESS_THAN_OR_EQ)) {
    		// add lower count, save slice for later
    		fcount += lowercount;
    	} else if (op.equals(Predicate.Op.GREATER_THAN) || op.equals(Predicate.Op.GREATER_THAN_OR_EQ)) {
    		// add inverse of lower count, save slice for later
    		fcount += htotal - lowercount;
    	}
    	
    	if (op.equals(Predicate.Op.LESS_THAN_OR_EQ) || op.equals(Predicate.Op.GREATER_THAN_OR_EQ)) {
    		// add slice in if necessary
    		fcount += slice;
    	}
    	
    	return fcount / htotal;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
    	String output = new String();
    	output += "{" + hmin + ", " + hist[0] + "} ";
    	
    	int interval = (hmax-hmin+1) % hbuckets;
        
        for (int i=1; i < hist.length; i++) {
        	output += "{" + (hmin + i*interval) + ", " + hist[i] + "} ";
        }
    	
    	return output;
    }
    
}
