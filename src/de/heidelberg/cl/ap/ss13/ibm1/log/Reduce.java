package de.heidelberg.cl.ap.ss13.ibm1.log;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import de.heidelberg.cl.ap.ss13.helper.Pair;
import de.heidelberg.cl.ap.ss13.helper.Log;

/**
 * The Reducer class for the IBM1 MapReduce job
 * Assumes that probabilities are in the log probability space
 * 
 * @author Carolin Haas (carhaas1@gmail.com)
 */
public class Reduce extends Reducer<Text, Pair, Text, DoubleWritable> {
    private static DoubleWritable double_value_writable = new DoubleWritable(1.0);
    private static double double_value = 0.0;
    private final static Text output = new Text();
	private static double total_f = 0.0;
    private final static HashMap<String, Double> count_e_f = new HashMap<String, Double>();
	private static String search_in_map = "";

	/**
	 * reduce function in the IBM1 MapReduce job
	 * It takes source language words as a key and a Pair containing the target language word and a double
	 * It finishes the Expectation step by combining values dependent on the source language word and then computes the Maximization step of the EM algorithm
	 * Then proceeds to execute the Maximization Step of the EM algorithm
	 * It outputs the source word and target word as a key ("e|f") and double which is the probability of "f" being translated into "e"
	 * for more details see my project report
	 *
	 * @param key the source language word
	 * @param values all pairs containing the target language word and a double output from the mapper
	 * @param context the Context variable that allows communication between the different MapReduce stages
	 */
    public void reduce(Text key, Iterable<Pair> values, Context context) throws IOException, InterruptedException {
		double_value_writable.set(0.0);
		total_f = Double.MAX_VALUE;
		count_e_f.clear();
		double_value = Math.log(0.0000001);//to ensure that its not equal to 0 (very basic smoothing)
		
		for (Pair val : values) {
			if(total_f == Double.MAX_VALUE){
				total_f = val.getSecond();
			} else {
				total_f = Log.logPlus(total_f , val.getSecond());
			}
			//count_e_f += t_e_f / s_total_e
			search_in_map = val.getFirst()+"|"+key;
			if(count_e_f.containsKey(search_in_map)){
				double_value = count_e_f.get(search_in_map);
				double_value = Log.logPlus(double_value, val.getSecond());
				count_e_f.remove(search_in_map);
				count_e_f.put(search_in_map, double_value);
			} else {
				count_e_f.put(search_in_map, val.getSecond());
			}
		}

		for (String e_f : count_e_f.keySet()) {
    	//t_e_f = count_e_f/total_f;
			output.set(e_f);
			if(total_f!=Double.MAX_VALUE){
				//double_value = Log.logPlus(count_e_f.get(e_f), Math.log(1)) - Log.logPlus(total_f, (Math.log(1)+Math.log(10000)));
				double_value = count_e_f.get(e_f) - total_f;
				if(double_value < Math.log(0.0000001)){
					double_value = Math.log(0.0000001);
				}
			}
			double_value_writable.set(double_value);
			context.write(output, double_value_writable);
		}	
	}
}
