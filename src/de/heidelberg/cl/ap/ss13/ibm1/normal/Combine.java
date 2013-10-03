package de.heidelberg.cl.ap.ss13.ibm1.normal;

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
 * The Combiner class for the IBM1 MapReduce job
 * 
 * @author Carolin Haas (carhaas1@gmail.com)
 */
public class Combine extends Reducer<Text, Pair, Text, Pair>{
    private final static Text word = new Text();
	private final static Pair e_countef = new Pair();
    private final static HashMap<String, Double> count_e_f = new HashMap<String, Double>();
	private static String search_in_map = "";
    private static double double_value = 0.0;

	/**
	 * combine function in the IBM1 MapReduce job
	 * Given the same source and target sides word pairs ("e|f" ), it adds up the value that the mapped output gave the pair ("e|f")
	 * It outputs the source word as a key and a pair of target word and a double as value
	 *
	 * @param key the source language word
	 * @param values all pairs containing the target language word and a double output from the mapper
	 * @param context the Context variable that allows communication between the different MapReduce stages
	 */
    public void combine(Text key, Iterable<Pair> values, Context context) throws IOException, InterruptedException {
		count_e_f.clear();
		
		for (Pair val : values) {
			//count_e_f += t_e_f / s_total_e
			search_in_map = val.getFirst()+"|"+key;
			if(count_e_f.containsKey(search_in_map)){
				double_value = count_e_f.get(search_in_map);
				double_value += val.getSecond();
				count_e_f.remove(search_in_map);
				count_e_f.put(search_in_map, double_value);
			} else {
				count_e_f.put(search_in_map, val.getSecond());
			}
        }
		
		for (String e_f : count_e_f.keySet()) {
			//t_e_f = count_e_f/total_f;
			String[] split_key = e_f.split("\\|");
			word.set(split_key[1]);
			e_countef.setFirst(split_key[0]);
			e_countef.setSecond(count_e_f.get(search_in_map));
			context.write(word, e_countef);
		}
    }
}
