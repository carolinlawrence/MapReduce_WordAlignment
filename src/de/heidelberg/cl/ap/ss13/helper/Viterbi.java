package de.heidelberg.cl.ap.ss13.helper;

import org.apache.hadoop.fs.FileSystem;

import java.util.HashMap;
import java.util.ArrayList;
import java.io.File;
import java.io.IOException;

import de.heidelberg.cl.ap.ss13.io.Writer;
import de.heidelberg.cl.ap.ss13.io.Reader;
import de.heidelberg.cl.ap.ss13.io.ReaderHDFS;

/**
 * A class that finds the most probable (=Viterbi) alignment given some model parameters
 *
 * @author Carolin Haas (carhaas1@gmail.com)
 */
public class Viterbi{

	/**
	 * Finds the most probable alignment for the given sentences and given model parameters
	 * Assumes that probabilities are in the log probability space
	 *
	 * @param input_path the file location (not in the HDFS) containing the parallel sentences for which the Viterbi alignment is to be found. First the source sentences, then the distinctive separator " ||| " and then the target sentence.
	 * @param parameter_directory the hadoop output directory in the HDFS from the last iteration
	 * @param fs the hadoop filesystem
	 * @param output_path the location of where the result should be written (not in the HDFS)
	 * @throws IOException handed down from Writer
 	 */
	public static void CalculateViterbiHMMLogProb(String input_path, String parameter_directory, FileSystem fs, String output_path, HashMap<String, String> hash_table_reverse_source, HashMap<String, String> hash_table_reverse_target, int number_reducers) throws IOException {
		ArrayList<String> paralell_sentences = Reader.readInListFromSeveralFiles(input_path, "XXXXX", 0);			
		double[] pi; //size: state
		double[][] transition; //size: state x state
		HashMap<String, Double> trans_table;
		double prob;
		String search_in_map = "";
		trans_table = ReaderHDFS.readInTableHDFS(parameter_directory, number_reducers, fs);
		transition = ReaderHDFS.readTransitionProbHDFS(parameter_directory, number_reducers, fs);
		pi = ReaderHDFS.readIniStateProbHDFS(parameter_directory, number_reducers, fs);
		StringBuilder output = new StringBuilder();
		File f = new File(output_path);//if output file already exists, delete it
		if(f.exists()){ f.delete() ; }
		int id = 1;
		for(String paralell_sentence : paralell_sentences){
			String[] split_languages = paralell_sentence.split(" \\|\\|\\| ");
			String[] states = split_languages[0].split("\\s");
			String[] observations = split_languages[1].split("\\s");
			double[][] gamma = new double[observations.length][states.length];
			int[][] bp = new int[observations.length][states.length];
			int[] best_sequence = new int[observations.length];
			//base case
			for(int q = 0; q < states.length; q++){
				search_in_map = observations[0] + "|" + states[q]; 
				if(trans_table.containsKey(search_in_map)){
					gamma[0][q] = pi[q] + trans_table.get(search_in_map);
					bp[0][q] = -1;
				}
			}
			
			for(int x = 1; x < observations.length; x++){
				for(int q = 0; q < states.length; q++){
					gamma[x][q] = -Double.MAX_VALUE;
					bp[x][q] = -1;
					for(int s = 0; s < states.length; s++){
						search_in_map = observations[x] + "|" + states[q]; 
						if(trans_table.containsKey(search_in_map)){
							double current_gamma = gamma[x-1][s] + transition[s][q] + trans_table.get(search_in_map);
							if(gamma[x][q] < current_gamma){
								gamma[x][q] = current_gamma;
								bp[x][q] = s;
							}
						}
					}		
				}
			}
				
			//find y^*_|x|
			prob = -Double.MAX_VALUE;
			for(int q = 0; q < states.length; q++){
				double current_prob = gamma[observations.length-1][q];	
				if(current_prob > prob){
					prob = current_prob;
					best_sequence[observations.length-1] = q;
					int x = observations.length-1;
				}
			}

			//recursively get y^*_t-1 = bp_t(y_t)
			for(int x = observations.length-2; x >=0; x--){
				best_sequence[x] = bp[x+1][best_sequence[x+1]];
			}

			output.append("# Sentence pair ("+id+") source length "+states.length+" target length "+observations.length+" alignment score : "+prob+"\n");		
			for(int i = 0; i < observations.length; i++){
				if(i == observations.length - 1){
					output.append(hash_table_reverse_target.get(observations[i])+"\n");
				} else {
					output.append(hash_table_reverse_target.get(observations[i])+" ");
				}
			}

			for(int q = 0; q < states.length; q++){
				output.append(hash_table_reverse_source.get(states[q])+" ({");
				for(int x = 0; x < best_sequence.length; x++){
					if(best_sequence[x] == q){
						int giza_count = x + 1; //giza starts counting from 1 instead of (here) 0, so we need to add 1						
						output.append(" "+giza_count);
					}
				}
				if(q == states.length-1){
					output.append(" })\n");
				} else {
					output.append(" }) ");
				}
			}

			if(id%500 == 0){
				Writer.writeString(output.toString(), output_path, true);
				output.delete(0, output.length());
			}

			id++;
		}	
		if(output.length() > 0){
			Writer.writeString(output.toString(), output_path, true);
			output.delete(0, output.length());
		}	
	}

	/**
	 * Finds the most probable alignment for the given sentences and given model parameters
	 *
	 * @param input_path the file location (not in the HDFS) containing the parallel sentences for which the Viterbi alignment is to be found. First the source sentences, then the distinctive separator " ||| " and then the target sentence.
	 * @param parameter_directory the hadoop output directory in the HDFS from the last iteration
	 * @param fs the hadoop filesystem
	 * @param output_path the location of where the result should be written (not in the HDFS)
	 * @throws IOException handed down from Writer
 	 */
	public static void CalculateViterbiHMM(String input_path, String parameter_directory, FileSystem fs, String output_path, HashMap<String, String> hash_table_reverse_source, HashMap<String, String> hash_table_reverse_target, int number_reducers) throws IOException {
		ArrayList<String> paralell_sentences = Reader.readInListFromSeveralFiles(input_path, "XXXXX", 0);			
		double[] pi; //size: state
		double[][] transition; //size: state x state
		HashMap<String, Double> trans_table;
		double prob = 0.0;
		String search_in_map = "";
		
		trans_table = ReaderHDFS.readInTableHDFS(parameter_directory, number_reducers, fs);//investigate if this works/why there don't seem to be any entries
		transition = ReaderHDFS.readTransitionProbHDFS(parameter_directory, number_reducers, fs);
		pi = ReaderHDFS.readIniStateProbHDFS(parameter_directory, number_reducers, fs);
	
		File f = new File(output_path);//if output file already exists, delete it
		if(f.exists()){ f.delete() ; }
		int id = 1;
		for(String paralell_sentence : paralell_sentences){
			String[] split_languages = paralell_sentence.split(" \\|\\|\\| ");
			String[] states = split_languages[0].split("\\s");
			String[] observations = split_languages[1].split("\\s");
			double[][] gamma = new double[observations.length][states.length];
			int[][] bp = new int[observations.length][states.length];
			int[] best_sequence = new int[observations.length];
			//base case
			for(int q = 0; q < states.length; q++){
				search_in_map = observations[0] + "|" + states[q]; 
				if(trans_table.containsKey(search_in_map)){
					gamma[0][q] = pi[q] * trans_table.get(search_in_map);
					bp[0][q] = -1;
				}
			}
			
			for(int x = 1; x < observations.length; x++){
				for(int q = 0; q < states.length; q++){
					gamma[x][q]  = 0;
					bp[x][q] = -1;
					for(int s = 0; s < states.length; s++){
							//reaches
						search_in_map = observations[x] + "|" + states[q];  
						if(trans_table.containsKey(search_in_map)){
							//reaches
							double current_gamma = gamma[x-1][s] * transition[s][q] * trans_table.get(search_in_map);		
							if(gamma[x][q] < current_gamma){
								gamma[x][q] = current_gamma;
								bp[x][q] = s;
							}
						}
					}		
				}
			}
							
			//find y^*_|x|
			prob = 0.0;
			for(int q = 0; q < states.length; q++){
				double current_prob = gamma[observations.length-1][q];	
				if(current_prob > prob){
					prob = current_prob;
					best_sequence[observations.length-1] = q;
					int x = observations.length-1;
				}
			}
			
			//recursively get y^*_t-1 = bp_t(y_t)
			for(int x = observations.length-2; x >=0; x--){
				best_sequence[x] = bp[x+1][best_sequence[x+1]];
			}

			String output = "# Sentence pair ("+id+") source length "+states.length+" target length "+observations.length+" alignment score : "+prob+"\n";			
			for(int i = 0; i < observations.length; i++){
				if(i == observations.length - 1){
					output += hash_table_reverse_target.get(observations[i])+"\n";
				} else {
					output += hash_table_reverse_target.get(observations[i])+" ";
				}
			}	

			for(int q = 0; q < states.length; q++){
				output+=hash_table_reverse_source.get(states[q])+" ({";
				for(int x = 0; x < best_sequence.length; x++){
					if(best_sequence[x] == q){
						int giza_count = x + 1; //giza starts counting from 1 instead of (here) 0, so we need to add 1						
						output+=" "+giza_count;
					}
				}
				if(q == states.length-1){
					output+=" })\n";
				} else {
					output+=" }) ";
				}
			}

			Writer.writeString(output, output_path, true);

			id++;
		}					
	}
}
