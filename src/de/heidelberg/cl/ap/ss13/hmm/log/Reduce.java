package de.heidelberg.cl.ap.ss13.hmm.log;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import de.heidelberg.cl.ap.ss13.helper.Pair;
import de.heidelberg.cl.ap.ss13.helper.Log;
import de.heidelberg.cl.ap.ss13.io.Writer;

/**
 * The Reducer class for the HMM MapReduce job
 * Assumes that probabilities are in the log probability space
 * 
 * @author Carolin Haas (carhaas1@gmail.com)
 */
public class Reduce extends Reducer<Text, MapWritable, Text, Text> {
	private final static SortedMapWritable stripe = new SortedMapWritable();
	private final static Text word = new Text();
	private final static Text write_out = new Text();
	private static DoubleWritable double_writable;
	private static DoubleWritable double_writable2;
	private static Double double_value = Math.log(0.0000001);
	private static double z = 0.0;
	private static double smoothing_factor = 0.2;
	private static MultipleOutputs mos;

	/**
	 * Creates MultipleOutputs so that the parameters can be written to different files
	 *
	 * @param context the Context variable that allows communication between the different MapReduce stages
	 */
	public void setup(Context context) throws IOException{		
		mos = new MultipleOutputs<Text, Text>(context);
	}

	/**
	 * reduce function in the HMM MapReduce job
	 * It finishes the Expectation step by adding up the stripes emitted by the map function. This is saved in the map stripe
	 * It then proceeds to execute the Maximization Step of the EM algorithm by adding up the values in the map stripe (saved in z) and then iterating once again over all keys in stripe and dividing the values by z
	 * It then emits the new probabilities for the emissions, transition and initial state probabilities
	 * for more details see my project report
	 *
	 * @param key either "emit from "+source word, "initial" or "transit from "+source word position
	 * @param maps the stripes emitted by the map function
	 * @param context the Context variable that allows communication between the different MapReduce stages
	 */
    public void reduce(Text key, Iterable<MapWritable> maps, Context context) throws IOException, InterruptedException {
		stripe.clear();
		z = Double.MAX_VALUE; double_value = Math.log(0.0000000001);//to ensure that its not equal to 0 (very basic smoothing)
		write_out.set("");

		for (MapWritable map : maps) {
			for(Writable currentKey : map.keySet() ){
				double_writable = (DoubleWritable) map.get(currentKey);	
				WritableComparable comparable_key = (WritableComparable) currentKey;
				if(stripe.containsKey(comparable_key)){
					double_writable2 = (DoubleWritable) stripe.get(comparable_key);
					double_writable.set(Log.logPlus(double_writable.get(), double_writable2.get()));
					Writable value = double_writable;
					stripe.remove(comparable_key);
					stripe.put(comparable_key, value);
				} else {
					Writable value = double_writable;
					stripe.put(comparable_key, value);
				}

			}
		}

		for(WritableComparable currentKey : stripe.keySet() ){
			double_writable = (DoubleWritable) stripe.get(currentKey);
			double_value = double_writable.get();
			if(z == Double.MAX_VALUE){
				z = double_writable.get();
			} else {
				z = Log.logPlus(z, double_writable.get());
			}
		}

		for(WritableComparable currentKey : stripe.keySet() ){
			double_writable = (DoubleWritable) stripe.get(currentKey);
			if(z!=Double.MAX_VALUE){
				//double_value = double_writable.get() - Log.logPlus(z, Math.log(1000));
				/*if(key.toString().contains("transit from ") || key.toString().contains("emit from ")){
					double_value = (1.0-smoothing_factor)*(double_writable.get() - z)+smoothing_factor/stripe.size();	
				//} else if (key.toString().contains("emit from ")){
				//	double_value = Log.logPlus(double_writable.get(), Math.log(1)) - Log.logPlus(z, (Math.log(1)+Math.log(10000)));				
				} else {
					double_value = double_writable.get() - z;
				}*/
				double_value = double_writable.get() - z;
				if(double_value < Math.log(0.0000001)){
					double_value = Math.log(0.0000001);
				}
			}
			if(key.toString().contains("emit from ")){
				word.set(currentKey+"|"+key.toString().replace("emit from ", ""));
				write_out.set(""+double_value);
				context.write(word, write_out);
			} else if (key.toString().contains("initial")){
				word.set(""+currentKey);
				write_out.set(""+double_value);
				mos.write(word, write_out, "ini");
			} else if (key.toString().contains("transit from ")){
				write_out.set(write_out.toString()+"\t"+double_value);
			}
		}
		
		if(key.toString().contains("transit from ")){
			word.set(key.toString().replace("transit from ", ""));
			write_out.set(write_out.toString().replaceFirst("\t", ""));
			mos.write(word, write_out, "transition");
		}
    }

	/**
	 * Closes the MultipleOutputs object. Required for the file to be written to the HDFS
	 *
	 * @param context the Context variable that allows communication between the different MapReduce stages
	 */	
	public void cleanup(Context context) throws IOException, InterruptedException {
		mos.close();
	}
}
