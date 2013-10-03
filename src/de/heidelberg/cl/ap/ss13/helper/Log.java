package de.heidelberg.cl.ap.ss13.helper;

/**
 * A class that allows for adding doubles which are in the log probability space
 *
 * @author Carolin Haas (carhaas1@gmail.com)
 */
public class Log{

	/**
	 * Adds up two double which are in the log probability space
	 *
	 * @param a the first number
	 * @param b the second number
	 * @return double which is the sum of a and b with a and b in the log porbability space
 	 */
	public static double logPlus(double a, double b){
		double plus = 1.0;
		
		//ensure that a is the larger number
		if(a<b){
			double save = a;
			a = b;
			b = save;
		}
		plus = a + Math.log(1.0+Math.exp(b-a));
		
		return plus;
	}
}
