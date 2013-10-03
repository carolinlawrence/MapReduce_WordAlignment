package de.heidelberg.cl.ap.ss13.hmm.log;

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
 * The Mapper class for the HMM MapReduce job
 * Assumes that probabilities are in the log probability space
 * 
 * @author Carolin Haas (carhaas1@gmail.com)
 */
public class Map extends Mapper<LongWritable, Text, Text, MapWritable> {
    private final static MapWritable pi_new = new MapWritable();//key is the state (int) and value is the probability of starting in this state (double)
    private static MapWritable[] transition_new;
    private static MapWritable[] emission_new;
    private final static Text word = new Text();
	private static Text text_writable;
    private static IntWritable int_writable;
    private static DoubleWritable double_writable;
    private static String search_in_map = "";
    private static HashMap<String, Double> emission;//e|f
	private static double double_value;
	private static double[][] alpha; //size: observation x state
	private static double[][] beta; //size: observation x state
	private static double[] pi; //size: state
	private static double[][] transition; //size: state x state
	
	/**
	 * Calculates the forward probability given a sentence. This function is called to start the calculation for the forward probability
	 *
	 * @param states the source sentence split up into its words, each word representing a state in the HMM
	 * @param observations the target sentence split up into its words, each word representing a possible emission in the HMM
 	 */
	private void forward(String[] states, String[] observations){
		alpha = new double[observations.length][states.length];
		
		//initialisation
		for(int q = 0; q < states.length; q++){
			forward(states, observations, q);
		}
		
		for(int x = 1; x < observations.length; x++){//x=0 was already done in the init step
			for(int q = 0; q < states.length; q++){
				forward(states, observations, q, x);	
			}
		}
	}

	/**
	 * Calculates the forward probability given a sentence. This function is called to handle the initialization step.
	 *
	 * @param states the source sentence split up into its words, each word representing a state in the HMM
	 * @param observations the target sentence split up into its words, each word representing a possible emission in the HMM
	 * @param q the state for which alpha is to be initialized
 	 */	
	private void forward(String[] states, String[] observations, int q){
		search_in_map = observations[0] + "|" + states[q]; //0 because this is the initalisation
		if(emission.containsKey(search_in_map)){
			alpha[0][q] = pi[q] + emission.get(search_in_map);
		}
	}
	
	/**
	 * Calculates the forward probability given a sentence. This function is called to handle the steps after the initialization.
	 *
	 * @param states the source sentence split up into its words, each word representing a state in the HMM
	 * @param observations the target sentence split up into its words, each word representing a possible emission in the HMM
	 * @param q the current state
	 * @param x the current observation
 	 */		
	private void forward(String[] states, String[] observations, int q, int x){
		double sum = Double.MAX_VALUE;
		search_in_map = observations[x] + "|" + states[q];
		
		for(int s = 0; s < states.length; s++){
			if(sum == Double.MAX_VALUE){
				sum = alpha[x-1][s] + transition[s][q];
			} else {
				sum = Log.logPlus(sum, alpha[x-1][s] + transition[s][q]);
			}
		}
		
		if(emission.containsKey(search_in_map)){
			alpha[x][q] = sum + emission.get(search_in_map);
		}
	}


	/**
	 * Calculates the backward probability given a sentence. This function is called to start the calculation for the backward probability
	 *
	 * @param states the source sentence split up into its words, each word representing a state in the HMM
	 * @param observations the target sentence split up into its words, each word representing a possible emission in the HMM
 	 */
	private void backward(String[] states, String[] observations){
		beta = new double[observations.length][states.length];
		
		//initalisation
		for(int q = 0; q < states.length; q++){
			beta[observations.length-1][q] = 0.0;//observations.length-1 because counting starts at 0
		}
		
		for(int x = observations.length-2; x >=0; x--){//observations.length-1 was already done in initialisation
			for(int q = 0; q < states.length; q++){
				backward(states, observations, q, x);
			}
		}
	}

	/**
	 * Calculates the backward probability given a sentence. This function is called for the individual steps after the initialization
	 *
	 * @param states the source sentence split up into its words, each word representing a state in the HMM
	 * @param observations the target sentence split up into its words, each word representing a possible emission in the HMM
	 * @param q the current state
	 * @param x the current observation
 	 */	
	private void backward(String[] states, String[] observations, int q, int x){
		double sum = Double.MAX_VALUE;
		
		for(int s = 0; s < states.length; s++){
			search_in_map = observations[x+1] + "|" + states[s]; 
			if(emission.containsKey(search_in_map)){
				if(sum == Double.MAX_VALUE){
					sum = beta[x+1][s] + transition[q][s] + emission.get(search_in_map);
				} else {
					sum = Log.logPlus(sum, beta[x+1][s] + transition[q][s] + emission.get(search_in_map));
				}
			}
		}
		beta[x][q] = sum;
	}

	/**
	 * loads the hmm (translation table[=emissions probability], transition probability and the probability for the initial state) for this Mapper for the current iteration before any map function calls
	 *
	 * @param context the Context variable that allows communication between the different MapReduce stages
	 */
	public void setup(Context context) throws IOException{		
		Configuration conf = context.getConfiguration();
		FileSystem fs = FileSystem.get(conf);
		emission = ReaderHDFS.readInTableHDFS(conf.get("current_dir"), Integer.parseInt(conf.get("number_reducers")), fs);	
		pi = ReaderHDFS.readIniStateProbHDFS(conf.get("current_dir"), Integer.parseInt(conf.get("number_reducers")), fs);
		transition = ReaderHDFS.readTransitionProbHDFS(conf.get("current_dir"), Integer.parseInt(conf.get("number_reducers")), fs);
	}
	
	/**
	 * map function in the HMM MapReduce job
	 * It takes a parallel sentence and computes the Expectation step of the EM algorithm within a HMM Framework
	 * states in the HMM are the source words/their position in the sentence and the observations are the target words
	 * It outputs the following stripes:
	 * 1 - emissions: "emit from " + the source word as a key (state) and a map in which the keys are target words (observation) and a double as value
	 * 2 - initial state probabilities: "initial" as key and a map in which the keys are the source word positions (states) and a double as value
	 * 3 - transition probabilities: "transit from" + the source word position (state) as key and a map in which the keys are again the source word positions (states) and a double as value
	 * for more details see my project report
	 *
	 * @param key the MapReduce key
	 * @param context the Context variable that allows communication between the different MapReduce stages
	 */
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();	
		String[] sentences = line.split("XXXXX");	

		for(String current_sentence : sentences){		
			pi_new.clear();
			String[] split_languages = current_sentence.split(" \\|\\|\\| ");
			String source = split_languages[0];
			String target = split_languages[1];
			String[] states = source.split("\\s");
			String[] observations = target.split("\\s");
			
			forward(states, observations);
			backward(states, observations);
			
			for(int q = 0; q < states.length; q++){
				int_writable = new IntWritable(q);
				double_value = alpha[0][q] + beta[0][q];
				double_writable  = new DoubleWritable(double_value);
				pi_new.put(int_writable, double_writable);
			}

			word.set("initial");
			context.write(word, pi_new);

			emission_new = new MapWritable[states.length];
			transition_new = new MapWritable[states.length];
			for(int q = 0; q < states.length; q++){
				emission_new[q] = new MapWritable();
				transition_new[q] = new MapWritable();
			}

			for(int x = 0; x < observations.length; x++){
				for(int q = 0; q < states.length; q++){
					//inner key=observation, outer key = state			
					double_value = alpha[x][q] + beta[x][q];
					double_writable  = new DoubleWritable(double_value);
					text_writable = new Text(observations[x]);
					if(emission_new[q].containsKey(text_writable)){//not tested
						double_writable = (DoubleWritable) emission_new[q].get(text_writable);
						double_writable.set(Log.logPlus(double_writable.get(), double_value));
						emission_new[q].remove(text_writable);
						emission_new[q].put(text_writable, double_writable);
					} else {
						emission_new[q].put(text_writable, double_writable);
					}
				}
			}

			for(int x = 0; x < observations.length-1; x++){
				int x_plus_1 = x+1;
				for(int q = 0; q < states.length; q++){
					for(int s = 0; s < states.length; s++){	
						search_in_map = observations[x_plus_1] + "|" + states[s]; 
						if(emission.containsKey(search_in_map)){
							double_value = alpha[x][q] + transition[q][s] + emission.get(search_in_map) + beta[x_plus_1][s];
							int_writable = new IntWritable(s);	
							double_writable  = new DoubleWritable(double_value);
							if(transition_new[q].containsKey(int_writable)){//not tested
								double_writable = (DoubleWritable) transition_new[q].get(int_writable);
								double_writable.set(Log.logPlus(double_writable.get(), double_value));
								transition_new[q].remove(int_writable);
								transition_new[q].put(int_writable, double_writable);					
							} else {
								transition_new[q].put(int_writable, double_writable);
							}
						}
					}
				}
			}

			for(int q = 0; q < states.length; q++){
				word.set("emit from "+states[q]);
				context.write(word, emission_new[q]);
				word.set("transit from "+q);
				context.write(word, transition_new[q]);
			}
		}
    }
}
