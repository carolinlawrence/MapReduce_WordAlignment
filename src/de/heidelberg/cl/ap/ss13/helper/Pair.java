package de.heidelberg.cl.ap.ss13.helper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

/**
 * A Pair class that has a String as its first element and a double as its second & that can be used in a MapReduce job
 * 
 * @author Carolin Haas (carhaas1@gmail.com)
 */
public class Pair implements WritableComparable<Pair>{
    private String first; //first member of pair
    private double second; //second member of pair

	/**
	 * Initializes a Pair with default values
	 *
	 */
    public Pair() {
        this.first = "";
        this.second = 0.0;
    }

	/**
	 * sets the first element of the Pair to the string specified
	 *
	 * @param first the string that should be the value of the first element in the pair
	*/
    public void setFirst(String first) {
        this.first = first;
    }

	/**
	 * sets the second element of the Pair to the double specified
	 *
	 * @param second the double that should be the value of the second element in the pair
	 */
    public void setSecond(double second) {
        this.second = second;
    }

	/**
	 * returns the first element of the Pair
	 *
	 * @return String the string which is the first element of the Pair
	 */
    public String getFirst() {
        return first;
    }
	
	/**
	 * returns the second element of the Pair
	 *
	 * @return double the double which is the second element of the Pair
	 */
    public double getSecond() {
        return second;
    }

	/**
	 * specifies how the Pair should be written by simply concatenating first and second element
	 *
	 * @return String the string representation of the Pair
	 */
	public String toString() {
		return first+" "+second;
	}

	/**
	 * Checks if this Pair object is equal to another Pair object
	 *
	 * @return boolean 1 if the objects are identical, 0 otherwise
	 */
	public boolean equals(Object obj) {
		Pair pair = (Pair) obj;
		return first.equals(pair.getFirst()) && second==pair.getSecond();
	}

	/**
	 * Compares this Pair object to another Pair object. First it compares the first elements and if those are identical it compares second elements
	 *
	 * @return int the result of the comparison
	 */
	public int compareTo(Pair comparePair) {//for WritableComparable
		String comparePairFirst = comparePair.getFirst();
		double comparePairSecond = comparePair.getSecond();
		if (first.equals(comparePairFirst)) {
			if (second < comparePairSecond)
				return -1;
			if (second > comparePairSecond)
				return 1;
			return 0;
		}
		return first.compareTo(comparePairFirst);
	}

	/**
	 * Tells hadoop how to read this Pair as a WritableComparable
	 *
	 * @return in the DataInput object
	 */	
	public void readFields(DataInput in) throws IOException {//for WritableComparable
		first = Text.readString(in);
		second = in.readDouble();
	}

	/**
	 * Tells hadoop how to write this Pair as a WritableComparable
	 *
	 * @return out the DataOutput object
	 */		
	public void write(DataOutput out) throws IOException {//for WritableComparable
		Text.writeString(out, first);
		out.writeDouble(second);
	}

	/**
	 * Gives a hash code for this Pair by adding up the hashCode() of the String and the hashCode of the double
	 *
	 * @return int the hash code
	 */
	public int hashCode() {//for WritableComparable
		return first.hashCode() + Double.valueOf(second).hashCode();
	}


}
