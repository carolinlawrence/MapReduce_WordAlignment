package de.heidelberg.cl.ap.ss13.io;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.io.InputStreamReader;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;

/**
 * A class with different functions to read in files located on the HDFS
 * 
 * @author Carolin Haas (carhaas1@gmail.com)
 */
public class ReaderHDFS {

	/**
	 * Reads line-wise from a file in the HDFS containing a translation table 
	 * 
	 * @param directory the directory where the translation table files are
	 * @param number_reducers the number of reducers, which is equal to the number of files we need to check if they exist
	 * @param fs the hadoop filesystem
	 *
	 * @return a HashMap<String, Double> that holds "e|f" as they key and "double" as the value
	 */
	public static HashMap<String, Double> readInTableHDFS(String directory, int number_reducers, FileSystem fs) {
		HashMap<String, Double> trans_table = new HashMap<String, Double>();
		try { 
			for(int i = 0; i<number_reducers; i++){
				String file = directory+"part-r-"+String.format("%05d", i);	
				if(fs.exists(new Path(file))){
					BufferedReader bReader = new BufferedReader(new InputStreamReader(fs.open(new Path(file))));
					while (bReader.ready()) {
						String line = bReader.readLine();
						line = line.trim();
						if (!line.isEmpty()) {
							String[] split_line = line.split("\\t");
							if(split_line.length == 2){
								trans_table.put(split_line[0], Double.parseDouble(split_line[1]));
							}
						}
					}
					bReader.close();
				}
			}
		} catch (IOException ioe) { 
			ioe.printStackTrace();
		}
		return trans_table;
	}
	
	/**
	 * Reads the initial state probability files from the HDFS
	 * 
	 * @param directory the input directory of where the initial state probability files are
	 * @param number_reducers the number of reducers, which is equal to the number of files we need to check if they exist
	 * @param fs the hadoop filesystem
	 *
	 * @return double[] that holds the initial probability for each state
	 */
	public static double[] readIniStateProbHDFS(String directory, int number_reducers, FileSystem fs) {
		int number_of_lines = findNumberOfLinesHDFS(directory, "ini-r-", number_reducers, fs);
		double[] ini_prob = new double[number_of_lines];
		try { 
			for(int i = 0; i<number_reducers; i++){
				String file = directory+"ini-r-"+String.format("%05d", i);	
				if(fs.exists(new Path(file))){
					BufferedReader bReader = new BufferedReader(new InputStreamReader(fs.open(new Path(file))));
					while (bReader.ready()) {
						String line = bReader.readLine();
						line = line.trim();
						if (!line.isEmpty()) {
							String[] split_line = line.split("\t");
							if(split_line.length == 2){
								ini_prob[Integer.parseInt(split_line[0])] = Double.parseDouble(split_line[1]);
							}
						}
					}
					bReader.close();
				}
			}
		} catch (IOException ioe) { 
			ioe.printStackTrace();
		}

		return ini_prob;
	}


	/**
	 * Reads the transition probability files from the HDFS
	 * 
	 * @param directory the input directory of where the transition probability files are
	 * @param number_reducers the number of reducers, which is equal to the number of files we need to check if they exist
	 * @param fs the hadoop filesystem
	 *
	 * @return double[][] matrix that holds the transition probability
	 */
	public static double[][] readTransitionProbHDFS(String directory, int number_reducers, FileSystem fs) {
		int number_of_lines = findNumberOfLinesHDFS(directory, "transition-r-", number_reducers, fs);
		double[][] transition_prob = new double[number_of_lines][number_of_lines];
		try { 
			for(int i = 0; i<number_reducers; i++){
				String file = directory+"transition-r-"+String.format("%05d", i);	
				if(fs.exists(new Path(file))){
					BufferedReader bReader = new BufferedReader(new InputStreamReader(fs.open(new Path(file))));
					while (bReader.ready()) {
						String line = bReader.readLine();
						line = line.trim();
						if (!line.isEmpty()) {
							String[] split_line = line.split("\\t");
							if(split_line.length == number_of_lines+1){
								for(int j = 1; j < split_line.length; j++){
									transition_prob[Integer.parseInt(split_line[0])][j-1] = Double.parseDouble(split_line[j]);
								}
							} else {
								System.out.println("row and column number are not identical, a square matrix is required though!");
							}
						}
					}
					bReader.close();
				}
			}
		} catch (IOException ioe) { 
			ioe.printStackTrace();
		}
		return transition_prob;
	}
	
	/**
	 * Finds out the number of lines in a list of files sharing a common prefix from the HDFS by iterating through it
	 * 
	 * @param directory the input directory of where the input file are
	 * @param prefix the prefix that the files have in common
	 * @param number_reducers the number of reducers, which is equal to the number of files we need to check if they exist
	 * @param fs the hadoop filesystem
	 *
	 * @return a int that contains the number of lines the files with the common prefix contain
	 */
	public static int findNumberOfLinesHDFS(String directory, String prefix, int number_reducers, FileSystem fs){
		int number_of_lines=0;
		try{
			for(int i = 0; i<number_reducers; i++){
				String file = directory+prefix+String.format("%05d", i);
				if(fs.exists(new Path(file))){
					BufferedReader bReader = new BufferedReader(new InputStreamReader(fs.open(new Path(file))));
					while (bReader.readLine() != null) number_of_lines++;
					bReader.close();
				}
			}
		} catch (IOException ioe) { 
			ioe.printStackTrace();
		}
		return number_of_lines;
	}
}
