package de.heidelberg.cl.ap.ss13.helper;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import de.heidelberg.cl.ap.ss13.helper.Pair;
import de.heidelberg.cl.ap.ss13.helper.Preparation;
import de.heidelberg.cl.ap.ss13.helper.Viterbi;
import de.heidelberg.cl.ap.ss13.io.Writer;
import de.heidelberg.cl.ap.ss13.io.Reader;
import de.heidelberg.cl.ap.ss13.io.Deleter;

import java.util.*;
import java.io.File;

/**
 * Entry point for my word alignment EM-algorithm (implementing IBM 1)
 * 
 * @author Carolin Haas (carhaas1@gmail.com)
 */
public class Main {

	/**
	 * The main program
	 */
	public static void main(String[] args) {
		String path_config_file = "";

		if (args.length == 1) {
			path_config_file = args[0];
			if(path_config_file.equals("--help")){
				System.out.println("To write your own configuartion file, use the following format per line:");
				System.out.println("key:value");
				System.out.println("e.g.: source:my_source_file.txt");
				System.out.println("The required keys are:");
				System.out.println("source - the source language file of your parallel corpus in which each line contains 1 tokenized sentence");
				System.out.println("target - the target language file of your parallel corpus in which each line contains 1 tokenized sentence");
				System.out.println("output - the file location and the file name of where the viterbi alignment should be written");
				System.out.println("root-dir - a existing directory where intermediate steps are saved (NOTE: unless you specify \"clean-up:false\", the created intermediate files will be deleted at the end of the program)");
				System.out.println("The optional keys are:");
				System.out.println("ibm1 (DEFAULT value: 5) - specify how many iterations of the IBM 1 model should there be at most");
				System.out.println("hmm (DEFAULT value: 5) - specify how many iterations of the HMM model should there be at most");
				System.out.println("null-token (DEFAULT value: false) - specify true if you want a null token to be added");
				System.out.println("clean-up (DEFAULT value: true) - specify false if you want the intermediate files to not be deleted");
				System.out.println("reverse-order (DEFAULT value: true) - specify false if you don't want to learn the viterbi alignment in both directions");
				System.out.println("log-prob (DEFAULT value: true) - specify false if you don't want to have the probabilities in log space (NOTE: underflow can happen even with small input files!");
				System.out.println("number-reducers (DEFAULT value: 1) - specify how many reducers you want to use");
				System.exit(1);
			}
		} else {
			System.out.println("Please specify the location of the configuration file or use --help for more information");
			System.exit(1);
		}
		
		Configure config = new Configure(path_config_file);
		RunWordAlignment(config, false, config.getLogProb());
		System.gc();
		if(config.getReverseOrder()){
			RunWordAlignment(config, true, config.getLogProb());
		}
	}

	/**
	 * Function that starts the word alignment computations with the configuration specified
	 *
	 * @param config the configuration file
	 * @param true if source and target language file should be reversed, false else
	 */
	public static void RunWordAlignment(Configure config, boolean reverse_order, boolean log_prob){

		if(reverse_order){
			String temp_new_target = config.getSource();
			String temp_new_source = config.getTarget();
			config.setSource(temp_new_source);
			config.setTarget(temp_new_target);
			System.out.println("Starting word alignment run in reversed order with the following configuration:");
		} else {	
			System.out.println("Starting word alignment run with the following configuration:");
		}
		
		System.out.println("Source corpus: "+config.getSource());
		System.out.println("Target corpus: "+config.getTarget());
		System.out.println("Number of IBM 1 iterations: "+config.getIBM1());
		System.out.println("Number of HMM iterations: "+config.getHMM());
		System.out.println("Using Null Token: "+config.getNullToken());
		System.out.println("Using Log Probability: "+config.getLogProb());
		System.out.println("Number of reducers: "+config.getNumberRedcuers());
		
		String combined_output = config.getRootDir()+"/tmp/combined";
		String translation_table = config.getRootDir()+"/tmp/part-r-00000";
		String alignment_prob = config.getRootDir()+"/tmp/transition-r-00000";
		String initial_state_prob = config.getRootDir()+"/tmp/ini-r-00000";

		try{
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			System.out.println("Starting Initialisation");
 			boolean success = (new File(config.getRootDir()+"tmp")).mkdir();
			if(!success){
				System.out.println("Couldn't create directory in specified root directory: "+config.getRootDir());
				System.exit(1);
			}
			long startTime_prep = System.currentTimeMillis();
			HashMap<String, Integer> hash_table_source = Preparation.StringToInt(config.getSource(), config.getRootDir()+"tmp/source_vocab");
			HashMap<String, Integer> hash_table_target = Preparation.StringToInt(config.getTarget(), config.getRootDir()+"tmp/target_vocab");
			Preparation.Initalisation(config.getSource(), config.getTarget(), combined_output, config.getNullToken(), translation_table, alignment_prob, initial_state_prob, log_prob, hash_table_source, hash_table_target);		
			System.out.println("Finished Initialisation, it took "+(System.currentTimeMillis()-startTime_prep)+" milli-seconds");
			fs.mkdirs(new Path("input"));
			fs.mkdirs(new Path("iterations/0"));
			int file_counter = 0;
			while(true){
				String file = combined_output+"_"+file_counter;
				file_counter++;
				File f = new File(file);
				if(f.exists()){
					fs.copyFromLocalFile(new Path(file), new Path("input"));
				} else { break; }
			}
			fs.copyFromLocalFile(new Path(translation_table), new Path("iterations/0"));
			long startTime_all = System.currentTimeMillis();

			//iterations of the EM
			int counter = 0;
			int new_path = 0;
			while(counter < config.getIBM1()){
				long startTime_it = System.currentTimeMillis();
				new_path = counter+1;				
				conf.set("current_dir","iterations/"+counter+"/");
				conf.set("number_reducers",""+config.getNumberRedcuers());
				conf.set("mapred.child.java.opts", "-Xms256m -Xmx2g -XX:+UseSerialGC");
				conf.set("mapred.job.map.memory.mb", "5120");
				conf.set("mapred.job.reduce.memory.mb", "1024");
				Job job = new Job(conf, "wordalignment");
				job.setJarByClass(de.heidelberg.cl.ap.ss13.helper.Main.class); 
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(Pair.class);
				if(log_prob){
					job.setMapperClass(de.heidelberg.cl.ap.ss13.ibm1.log.Map.class);
					job.setCombinerClass(de.heidelberg.cl.ap.ss13.ibm1.log.Combine.class);
					job.setReducerClass(de.heidelberg.cl.ap.ss13.ibm1.log.Reduce.class);
				} else {
					job.setMapperClass(de.heidelberg.cl.ap.ss13.ibm1.normal.Map.class);
					job.setCombinerClass(de.heidelberg.cl.ap.ss13.ibm1.normal.Combine.class);
					job.setReducerClass(de.heidelberg.cl.ap.ss13.ibm1.normal.Reduce.class);
				} 
				job.setOutputFormatClass(TextOutputFormat.class);
				job.setNumReduceTasks(config.getNumberRedcuers());
				FileInputFormat.addInputPath(job, new Path("input"));
				FileOutputFormat.setOutputPath(job, new Path("iterations/"+new_path));
				System.out.println("Starting Map Reduce: IBM 1, iteration " + new_path+"/"+config.getIBM1());			
				job.waitForCompletion(true);
				counter++;
				System.out.println("Finished Map Reduce: IBM 1, iteration "+new_path+"/"+config.getIBM1()+", it took "+(System.currentTimeMillis()-startTime_it)+" milli-seconds");
			}
			fs.copyFromLocalFile(new Path(alignment_prob), new Path("iterations/"+new_path+"/transition-r-00000"));
			fs.copyFromLocalFile(new Path(initial_state_prob), new Path("iterations/"+new_path+"/ini-r-00000"));
			int hmm_it_done = 0;
			while(hmm_it_done < config.getHMM()){
				long startTime_it = System.currentTimeMillis();		
				new_path = counter+1;
				int current_hmm_it = hmm_it_done + 1;
				conf.set("current_dir","iterations/"+counter+"/");
				conf.set("number_reducers",""+config.getNumberRedcuers());
				conf.set("mapred.child.java.opts", "-Xms256m -Xmx2g -XX:+UseSerialGC");
				conf.set("mapred.job.map.memory.mb", "5120");
				conf.set("mapred.job.reduce.memory.mb", "1024");
				Job job = new Job(conf, "wordalignment");
				job.setJarByClass(de.heidelberg.cl.ap.ss13.helper.Main.class); 
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(MapWritable.class);
				if(log_prob){
					job.setMapperClass(de.heidelberg.cl.ap.ss13.hmm.log.Map.class);
					job.setCombinerClass(de.heidelberg.cl.ap.ss13.hmm.log.Combine.class);
					job.setReducerClass(de.heidelberg.cl.ap.ss13.hmm.log.Reduce.class); 
				} else {
					job.setMapperClass(de.heidelberg.cl.ap.ss13.hmm.normal.Map.class);
					job.setCombinerClass(de.heidelberg.cl.ap.ss13.hmm.normal.Combine.class);
					job.setReducerClass(de.heidelberg.cl.ap.ss13.hmm.normal.Reduce.class); 
				}
				job.setOutputFormatClass(TextOutputFormat.class);
				job.setNumReduceTasks(config.getNumberRedcuers());
				FileInputFormat.addInputPath(job, new Path("input"));
				FileOutputFormat.setOutputPath(job, new Path("iterations/"+new_path));
				MultipleOutputs.addNamedOutput(job, "transition", TextOutputFormat.class, Text.class, Text.class);
				System.out.println("Starting Map Reduce: HMM, iteration " + current_hmm_it+"/"+config.getHMM());
				job.waitForCompletion(true);
				counter++;
				System.out.println("Finished Map Reduce: HMM, iteration "+ current_hmm_it+"/"+config.getHMM()+", it took "+(System.currentTimeMillis()-startTime_it)+" milli-seconds");
				hmm_it_done++;
			}

			System.out.println("Finding the Viterbi alignment now...");
			HashMap<String, String> hash_table_reverse_source = Reader.readVocabulary( config.getRootDir()+"tmp/source_vocab");
			HashMap<String, String> hash_table_reverse_target = Reader.readVocabulary( config.getRootDir()+"tmp/target_vocab");
			
			long startTime_vit = System.currentTimeMillis();		
			if(log_prob){
				if(reverse_order){
					Viterbi.CalculateViterbiHMMLogProb(combined_output, "iterations/"+new_path+"/", fs, config.getOutput()+".reversed_order", hash_table_reverse_source, hash_table_reverse_target, config.getNumberRedcuers());
				} else {
					Viterbi.CalculateViterbiHMMLogProb(combined_output, "iterations/"+new_path+"/", fs, config.getOutput(), hash_table_reverse_source, hash_table_reverse_target, config.getNumberRedcuers());
				}
			} else {
				if(reverse_order){
					Viterbi.CalculateViterbiHMM(combined_output, "iterations/"+new_path+"/", fs, config.getOutput()+".reversed_order", hash_table_reverse_source, hash_table_reverse_target, config.getNumberRedcuers());
				} else {
					Viterbi.CalculateViterbiHMM(combined_output, "iterations/"+new_path+"/", fs, config.getOutput(), hash_table_reverse_source, hash_table_reverse_target, config.getNumberRedcuers());
				}
			}
			System.out.println("Finding the Viterbi alignment done! It took "+(System.currentTimeMillis()-startTime_vit)+" milli-seconds");

			System.out.println("Map Reduce training & finding the Viterbi alignment took "+(System.currentTimeMillis()-startTime_all)+" milli-seconds");

			if(config.getCleanUp()){
				cleanUp(fs, config);
			}

			if(reverse_order){
				System.out.println("Finished word alignment run in reversed order");
			} else {	
				System.out.println("Finished word alignment run");
			}

		} catch (Exception e){
			System.out.println(e);
		}
	}	

	/**
	 * Deletes all temporary files that were created, leaving only the output file containing the Viterbi alignment
	 * @param fs the HDFS file system to delete file on there
	 * @param config the configuration to be able to locate the root directory
	 */
	public static void cleanUp(FileSystem fs, Configure config){
		try{
			File dir = new File(config.getRootDir()+"tmp");
			Deleter.delete(dir);
			fs.delete(new Path("iterations"), true);
			fs.delete(new Path("input"), true);
		} catch (Exception e){
			System.out.println(e);
		}
	}	
}


