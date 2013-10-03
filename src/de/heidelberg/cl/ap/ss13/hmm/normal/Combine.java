package de.heidelberg.cl.ap.ss13.hmm.normal;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import de.heidelberg.cl.ap.ss13.helper.Pair;

/**
 * The Combiner class for the HMM MapReduce job
 * 
 * @author Carolin Haas (carhaas1@gmail.com)
 */
public class Combine extends Reducer<Text, MapWritable, Text, MapWritable>{
	private final static MapWritable stripe = new MapWritable();
   private final static Text word = new Text();
   private static DoubleWritable double_writable;
   private static DoubleWritable double_writable2;

	/**
	 * combine function in the HMM MapReduce job
	 * Adds up the stripes emitted by the map function
	 *
	 * @param key either "emit from "+source word, "initial" or "transit from "+source word position
	 * @param maps the stripes emitted by the map function
	 * @param context the Context variable that allows communication between the different MapReduce stages
	 */
    public void combine(Text key, Iterable<MapWritable> maps, Context context) throws IOException, InterruptedException {
		stripe.clear();
		
		for (MapWritable map : maps) {
			for(Writable currentKey : map.keySet() ){
				double_writable = (DoubleWritable) map.get(currentKey);	
				System.out.println("current key: " +currentKey);	
				if(stripe.containsKey(currentKey)){
					double_writable2 = (DoubleWritable) stripe.get(currentKey);
					double_writable.set(double_writable.get() + double_writable2.get());
					stripe.remove(currentKey);
					stripe.put(currentKey, double_writable);
				} else {
					stripe.put(currentKey, double_writable);
				}

			}
        }

		
		context.write(key, stripe);
    }
}
