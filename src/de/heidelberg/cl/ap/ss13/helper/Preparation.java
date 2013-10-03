package de.heidelberg.cl.ap.ss13.helper;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HashMap;
import java.lang.Math;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import de.heidelberg.cl.ap.ss13.io.Writer;
import de.heidelberg.cl.ap.ss13.io.Reader;

/**
 * A class that handles various preparation steps that are necessary before the MapReduce job can be performed
 * 
 * @author Carolin Haas (carhaas1@gmail.com)
 */
public class Preparation {	
	
	public static HashMap<String, Integer> StringToInt(String input_file, String output_file){
		ArrayList<String> text = Reader.readInList(input_file, "\n", 0);
		HashMap<String, Integer> hash_table = new HashMap<String, Integer>();
		Integer counter = 1; //0 is reserved for the null token

		for(String line : text){
			String[] words = line.split(" ");
			for(String word : words){
				if(!hash_table.containsKey(word)){
					hash_table.put(word, counter);
					counter++;
				}
			}
		}
		Writer.writeVocabulary(hash_table, output_file, false);
		return hash_table;
	}
		
	/**
	 * Combines the two parallel text files sentence-wise. First the source sentences, then the distinctive separator " ||| " and then the target sentence. This is necessary so that the Map function receives both languages through its usual input
	 * Then it initializes the translation table, the alignment transition probability and the initial state probability uniformly 
	 *               
	 * @param source_file the input file location of the source language text
	 * @param target_file the input file location of the target language text
	 * @param output_file the output file location of the combined text
	 * @param insert_null_token whether or not to insert a null token
	 * @param trans_table_file the output file location for the translation table
	 * @param alignment_prob_file the output file location for the alignment transition probability
	 * @param ini_state_prob_file the output file location for the initial state probability
	 * @param log_prob whether or not log probability should be used for initialization
	 */
	public static void Initalisation(String source_file, String target_file, String output_file, boolean insert_null_token, String trans_table_file, String alignment_prob_file, String ini_state_prob_file, boolean log_prob, HashMap<String, Integer> hash_table_source, HashMap<String, Integer> hash_table_target) throws Exception{		

		HashSet<String> trans_table = new HashSet<String>();
		HashSet<String> target_words = new HashSet<String>();	
		int longest_source_sentence = 0;	
		StringBuilder output = new StringBuilder();

		int count = 0;
		int file_number = 0;
		BufferedReader bReader_source = new BufferedReader(new FileReader(source_file));
		BufferedReader bReader_target = new BufferedReader(new FileReader(target_file));
		while (bReader_source.ready()&&bReader_target.ready()) {
			count++;
			if(insert_null_token){
				output.append("0 ");
			}
			String source = bReader_source.readLine().trim();
			String target = bReader_target.readLine().trim();
			String[] split_source = source.split("\\s");
			String[] split_target = target.split("\\s");

			for(String word : split_source){
				output.append(hash_table_source.get(word)+" ");
			}

			output.append("|||");

			for(String word : split_target){
				output.append(" "+hash_table_target.get(word));
			}

			output.append("XXXXX");

			if(split_source.length > longest_source_sentence){
				longest_source_sentence = split_source.length;
			}

			for (int i=0; i<split_source.length; i++){
				for (int j=0; j<split_target.length; j++){
					String e_f = hash_table_target.get(split_target[j])+"|"+hash_table_source.get(split_source[i]);
					if(!trans_table.contains(e_f)){
						trans_table.add(e_f);
					}
					if(!target_words.contains(split_target[j])){
						target_words.add(split_target[j]);
					}					
				}
			}

			if(count == 500){
				Writer.writeString(output.toString(), output_file+"_"+file_number, true);
				output.delete(0, output.length());
				count = 0;
				file_number++;
			}
		}
		if(output.length() > 0){
			Writer.writeString(output.toString(), output_file+"_"+file_number, true);//write the remaining
			output.delete(0, output.length());
		}
		
		bReader_source.close();
		bReader_target.close();
		
		if(insert_null_token){
			longest_source_sentence++;//then we have 1 more state -> the null state
		}

		System.out.println("\tStarting To Write Translation Probability Initialisation");
		double divide = 1.0/target_words.size();
		if(log_prob){divide = Math.log(divide);}
		int c = 0;
		for(String e_f : trans_table){
     	output.append(e_f+"\t"+divide+"\n");
			c++;
			if(c%1000 == 0){
				Writer.writeString(output.toString(), trans_table_file, true);
				output.delete(0, output.length());
			}
		}	

		if(output.length() > 0){
			Writer.writeString(output.toString(), trans_table_file, true);//write the remaining	
			output.delete(0, output.length());
		}
		System.out.println("\tFinished Writing Translation Probability Initialisation");

		System.out.println("\tStarting To Write Transition Probability Initialisation");
		//initital alignment transition prob: every line equals 1 given state (a_i-1), for which is then contains the probability that this state goes into any other p(a_i|a_i-1)
		double ini_state_prob = 1.0 / longest_source_sentence;
		if(log_prob){ini_state_prob = Math.log(ini_state_prob);}
		for(int i = 0; i < longest_source_sentence; i++){
			String line = "";
			for(int j = 0; j < longest_source_sentence; j++){
				if(j==(longest_source_sentence-1)){
					line += ini_state_prob+"\n";
				} else {
					line += ini_state_prob+"\t";				
				}
			}
			Writer.writeString(i+"\t"+line, alignment_prob_file, true);
		}
		System.out.println("\tFinished Writing Transition Probability Initialisation");

		System.out.println("\tStarting To Write Initial State Probability Initialisation");
		//initital state prob: every line contains 1 probabiliy, 1st line = probability for 1st state
		for(int i = 0; i < longest_source_sentence; i++){
			Writer.writeString(i+"\t"+ini_state_prob+"\n", ini_state_prob_file, true);
		}
		System.out.println("\tFinished Writing Initial State Probability Initialisation");
	}
}
