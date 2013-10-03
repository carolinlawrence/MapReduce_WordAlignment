package de.heidelberg.cl.ap.ss13.helper;

import de.heidelberg.cl.ap.ss13.io.Reader;

import java.util.HashMap;

/**
 * A class that handles the configuration for the word alignment
 *
 * @author Carolin Haas (carhaas1@gmail.com)
 */
public class Configure{

	//required parameters
	private String source;
	private String target;
	private String output;
	private String root_dir;

	//optional parameters
	private int ibm1;
	private int hmm;
	private boolean null_token;
	private boolean clean_up;
	private boolean reverse_order;
	private boolean log_prob;
	private int number_reducers;

	/**
	 * Initializes the Configure object with the configuration options taken from the specified configuration file
	 * The configuration file should have the following format: every line contains one key value pair. keys and values are split with ":"
	 * 
	 * Required keys: source, target, output, root_dir
	 * Optional keys: ibm1, hmm, null_token, clean_up, reverse_order, log_prob, number_reducers
     * For more details see the help message of the main program or the documentation
	 * @param path_config_file the file location of the configuration file
	 */
	public Configure(String path_config_file){
		//initialisation for safety reasons
		source = ""; target = ""; output = ""; root_dir = ""; 

		//default values
		ibm1 = 5; hmm = 5; null_token = false; clean_up = true; reverse_order = true; log_prob = true; number_reducers = 1;

		//read parameters from file and initialise with the right values
		HashMap<String, String> parameters = Reader.readConfigFile(path_config_file);

		if(parameters.containsKey("source")){
			source = parameters.get("source");
		} else {
			System.out.println("Please specify a source language corpus file or use --help for more information");
			System.exit(1);
		}

		if(parameters.containsKey("target")){
			target = parameters.get("target");
		} else {
			System.out.println("Please specify a target language corpus file or use --help for more information");
			System.exit(1);
		}

		if(parameters.containsKey("output")){
			output = parameters.get("output");
		} else {
			System.out.println("Please specify a output file location or use --help for more information");
			System.exit(1);
		}

		if(parameters.containsKey("root-dir")){
			root_dir = parameters.get("root-dir");
		} else {
			System.out.println("Please specify a root directory or use --help for more information");
			System.exit(1);
		}

		if(parameters.containsKey("ibm1")){
			try{
				ibm1 = Integer.parseInt(parameters.get("ibm1"));
			} catch (Exception e){
				System.out.println("ibm1 needs a positive integer as value!");
				System.exit(1);
			}
		}

		if(parameters.containsKey("hmm")){
			try{
				hmm = Integer.parseInt(parameters.get("hmm"));
				if(hmm < 1){
					System.out.println("hmm needs to be at least 1!");
					System.exit(1);				
				}
			} catch (Exception e){
				System.out.println("hmm needs a positive integer as value!");
				System.exit(1);
			}
		}

		if(parameters.containsKey("null-token")){
			try{
				null_token = Boolean.parseBoolean(parameters.get("null-token"));
			} catch (Exception e){
				System.out.println("null-token needs to be either true or false!");
				System.exit(1);
			}
		}

		if(parameters.containsKey("clean-up")){
			try{
				clean_up = Boolean.parseBoolean(parameters.get("clean-up"));
			} catch (Exception e){
				System.out.println("clean-up needs to be either true or false!");
				System.exit(1);
			}
		}

		if(parameters.containsKey("reverse-order")){
			try{
				reverse_order = Boolean.parseBoolean(parameters.get("reverse-order"));
			} catch (Exception e){
				System.out.println("reverse-order needs to be either true or false!");
				System.exit(1);
			}
		}

		if(parameters.containsKey("log-prob")){
			try{
				log_prob = Boolean.parseBoolean(parameters.get("log-prob"));
			} catch (Exception e){
				System.out.println("log-prob needs to be either true or false!");
				System.exit(1);
			}
		}

		if(parameters.containsKey("number-reducers")){
			try{
				number_reducers = Integer.parseInt(parameters.get("number-reducers"));
			} catch (Exception e){
				System.out.println("number-reducers needs a positive integer as value!");
				System.exit(1);
			}
		}
	}

	/**
	 * sets the file location of the source language corpus
	 * should only be used for reversing the order
	 *
	 * @param new_source the new file location of the source language corpus
	 */
	public void setSource(String new_source){
		source = new_source;
	}

	/**
	 * sets the file location of the target language corpus
	 * should only be used for reversing the order
	 *
	 * @param new_target the new file location of the target language corpus
	 */
	public void setTarget(String new_target){
		target = new_target;
	}

	/**
	 * returns the file location of the source language corpus
	 *
	 * @return String the string which contains the file location of the source language corpus
	 */
	public String getSource(){
		return source;
	}

	/**
	 * returns the file location of the target language corpus
	 *
	 * @return String the string which contains the file location of the target language corpus
	 */
	public String getTarget(){
		return target;
	}

	/**
	 * returns the file location of where the output file containing the viterbi alignment for the given parallel corpus should be written
	 *
	 * @return String the string of where the output file should be written
	 */
	public String getOutput(){
		return output;
	}

	/**
	 * returns the file location of the specified root dir. The root dir will be used to as the location to save some temporary files
	 *
	 * @return String the string of where the root dir is located
	 */
	public String getRootDir(){
		return root_dir;
	}

	/**
	 * returns the value of how many IBM 1 iterations there should be at maximum. The default value is 5.
	 *
	 * @return int the maximum number of IBM 1 iterations
	 */
	public int getIBM1(){
		return ibm1;
	}

	/**
	 * returns the value of how many HMM iterations there should be at maximum. The default value is 5.
	 *
	 * @return int the maximum number of HMM iterations
	 */
	public int getHMM(){
		return hmm;
	}

	/**
	 * returns whether or not a null token should be inserted. True if the null token should be inserted, false else. Default is false.
	 *
	 * @return boolean whether or not a null token should be inserted
	 */
	public boolean getNullToken(){
		return null_token;
	}

	/**
	 * returns whether or not the temporary files (on the normal file system as well as the HDFS) should be deleted. True if they should be deleted, false if the temporary files should remain. Default is true.
	 *
	 * @return boolean whether or not the temporary files should be deleted
	 */
	public boolean getCleanUp(){
		return clean_up;
	}

	/**
	 * returns whether or not source and target languages should be reversed. True if the order should be inversed, false else. Default is true.
	 *
	 * @return boolean whether or not source and target languages should be reversed
	 */
	public boolean getReverseOrder(){
		return reverse_order;
	}

	/**
	 * returns whether or not the probabilities should be in log space. True if log space should be used, false else. Default is true.
	 *
	 * @return boolean whether or not the log space should be used
	 */
	public boolean getLogProb(){
		return log_prob;
	}

	/**
	 * returns the number of reducers that should be used. Default value is 1.
	 *
	 * @return int return the number of reducers to be used
	 */
	public int getNumberRedcuers(){
		return number_reducers;
	}
}
