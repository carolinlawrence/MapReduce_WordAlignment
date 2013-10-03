package de.heidelberg.cl.ap.ss13.ibm1.log;

import java.io.IOException;
import java.util.*;


import de.heidelberg.cl.ap.ss13.helper.Pair;
import de.heidelberg.cl.ap.ss13.helper.Log;
import de.heidelberg.cl.ap.ss13.io.ReaderHDFS;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.fs.FileSystem;

import java.util.HashMap;

/**
 * The Mapper class for the IBM1 MapReduce job
 * Assumes that probabilities are in the log probability space
 * 
 * @author Carolin Haas (carhaas1@gmail.com)
 */
public class Map extends Mapper<LongWritable, Text, Text, Pair> {
    private final static Text word = new Text();
    private static double double_value = 0.0;
    private static String search_in_map = "";
    private static HashMap<String, Double> trans_table;
    private final static HashMap<String, Double> s_total_e = new HashMap<String, Double>();
    private final static HashMap<String, Double> count_e_f = new HashMap<String, Double>();
	private final static Pair e_countef = new Pair();

	/**
	 * loads the translation table for this Mapper before any map function calls
	 *
	 * @param context the Context variable that allows communication between the different MapReduce stages
	 */
	public void setup(Context context) throws IOException{	
		Configuration conf = context.getConfiguration();
		FileSystem fs = FileSystem.get(conf);
		trans_table = ReaderHDFS.readInTableHDFS(conf.get("current_dir"), Integer.parseInt(conf.get("number_reducers")), fs);
	}
	
	/**
	 * map function in the IBM1 MapReduce job
	 * It takes a parallel sentence and computes the Expectation step of the EM algorithm
	 * It outputs the source word as a key and a pair of target word and a double as value
	 * for more details see my project report
	 *
	 * @param key the MapReduce key
	 * @param value a text line in which everything before "|||" is the source language sentence and everything after the parallel target language sentence
	 * @param context the Context variable that allows communication between the different MapReduce stages
	 */
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] sentences = line.split("XXXXX");
		for(String current_sentence : sentences){	
			s_total_e.clear(); count_e_f.clear();
			String[] split_languages = current_sentence.split(" \\|\\|\\| ");
			String source = split_languages[0];
			String target = split_languages[1];
			String[] split_source = source.split("\\s");
			String[] split_target = target.split("\\s");
			for (int j=0; j<split_target.length; j++){
				double_value = Double.MAX_VALUE;
				for (int i=0; i<split_source.length; i++){
					search_in_map = split_target[j]+"|"+split_source[i];
					if(trans_table.containsKey(search_in_map)){
						if(double_value == Double.MAX_VALUE){
							double_value = trans_table.get(search_in_map);
						} else {
							double_value = Log.logPlus(double_value, trans_table.get(search_in_map));
						}
					}
				}
				if(s_total_e.containsKey(split_target[j]) && double_value != Double.MAX_VALUE){
					double_value = Log.logPlus(double_value, s_total_e.get(split_target[j]));
					s_total_e.remove(split_target[j]);
					s_total_e.put(split_target[j], double_value);
				} else {
					s_total_e.put(split_target[j], double_value);
				}
			}
			
			for (int j=0; j<split_target.length; j++){
				for (int i=0; i<split_source.length; i++){
					//count_e_f += t_e_f / s_total_e
					search_in_map = split_target[j]+"|"+split_source[i];
					if(count_e_f.containsKey(search_in_map)){
						if(s_total_e.containsKey(split_target[j]) && trans_table.containsKey(search_in_map)){
							double_value = count_e_f.get(search_in_map);
							double_value = Log.logPlus(double_value, (trans_table.get(search_in_map)-s_total_e.get(split_target[j])));
							count_e_f.remove(search_in_map);
							count_e_f.put(search_in_map, double_value);
						}
					} else {
						double_value = 0.0;
						if(s_total_e.containsKey(split_target[j]) && trans_table.containsKey(search_in_map)){
							double_value = trans_table.get(search_in_map)-s_total_e.get(split_target[j]);
						}
						count_e_f.put(search_in_map, double_value);
					}
				}
			}
			
			for (int j=0; j<split_target.length; j++){
				for (int i=0; i<split_source.length; i++){
					search_in_map = split_target[j]+"|"+split_source[i];
					if(count_e_f.containsKey(search_in_map)){
						word.set(split_source[i]);
						e_countef.setFirst(split_target[j]);
						e_countef.setSecond(count_e_f.get(search_in_map));
						context.write(word, e_countef);
					}
				}
			}			
		}
	}
}
