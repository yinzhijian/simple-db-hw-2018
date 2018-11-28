package simpledb;

import static simpledb.Predicate.Op.*;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int ntups;
    private int min;
    private int max;
    private double b_width;
    private int buckets;
    private int values[];
    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.min = min;
        this.max = max;
        if (buckets > max - min + 1) {
            buckets = max - min + 1;
        }
        values = new int[buckets];
        this.b_width = (double)(max - min + 1)/buckets;
        this.buckets = buckets;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        ntups++;
        values[getIndex(v)] += 1;
    }
    private int getIndex(int v) {
        return (int)((v-min) / b_width);
    }
    private boolean inRange(int v) {
        if (v >= min && v <= max) {
            return true;
        }
        return false;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        // some code goes here
        if (op.equals(EQUALS)) {
            if (!inRange(v)) {
                return 0;
            }
            return getOnePointSelectivity(v);
        }
        if (op.equals(GREATER_THAN)) {
            if (v >= max) {
                return 0;
            }
            if (v < min) {
                return 1;
            }
            return getGreaterThanPointSelectivity(v);
        }
        if (op.equals(LESS_THAN)) {
            if (v > max) {
                return 1;
            }
            if (v <= min) {
                return 0;
            }
            return getLessThanPointSelectivity(v) - getOnePointSelectivity(v);
        }
        if (op.equals(LESS_THAN_OR_EQ)) {
            if (v >= max) {
                return 1;
            }
            if (v < min) {
                return 0;
            }
            return getLessThanPointSelectivity(v);
        }
        if (op.equals(GREATER_THAN_OR_EQ)) {
            if (v > max) {
                return 0;
            }
            if (v <= min) {
                return 1;
            }
            return getGreaterThanPointSelectivity(v) + getOnePointSelectivity(v);
        }
        if (op.equals(NOT_EQUALS)) {
            if (!inRange(v)) {
                return 1;
            }
            return 1 - getOnePointSelectivity(v);
        }
        return -1.0;
    }
    private double getGreaterThanPointSelectivity(int v) {
        int b = getIndex(v);
        double b_right = (b+1)*b_width;
        double b_f = ((double)values[b]) / ntups;
        double b_part = ((double)(b_right - v)) / b_width;
        double selectivity = b_f*b_part;
        for (int i = b + 1; i < buckets; i++) {
            selectivity += ((double)values[i]) / ntups;
        }
        return selectivity;
    }
    private double getLessThanPointSelectivity(int v) {
        int b = getIndex(v);
        double b_left = (b)*b_width;
        double b_f = ((double)values[b]) / ntups;
        double b_part = ((double)(v - b_left)) / b_width;
        double selectivity = b_f*b_part;
        //System.out.println("part buckets:"+buckets);
        //System.out.println("part values[b]:"+values[b]);
        //System.out.println("part ntups:"+ntups);
        //System.out.println("part b_f:"+b_f);
        //System.out.println("part b_left:"+b_left);
        //System.out.println("part v:"+v);
        //System.out.println("part b_width:"+b_width);
        //System.out.println("part b_part:"+b_part);
        for (int i = 0; i < b; i++) {
            selectivity += ((double)values[i]) / ntups;
            //System.out.println("part s:"+selectivity);
        }
        return selectivity;
    }
    private double getOnePointSelectivity(int v) {
        int b = getIndex(v);

        return ((double)values[b])/b_width/ntups;
    }
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        StringBuilder sb = new StringBuilder();
        for(int i = 0;i < values.length;i++) {
            sb.append("values["+i+"]="+values[i]);
        }
        return sb.toString();
    }
}
