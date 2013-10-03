package de.heidelberg.cl.ap.ss13.io;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.io.File;

/**
 * A class with different functions to read in files
 * 
 * @author Carolin Haas (carhaas1@gmail.com)
 */
public class Reader {
	
	/**
	 * Reads text from files in a directory and stores the data in a ArrayList<String>. The start and end of one entry in the ArrayList is determined by a variable
	 * 
	 * @param directory the input directory
	 * @param splitter the variable after which the text is split. set to "\n" if the entries in the ArrayList should correspond to lines in the input file
	 * @param numberOfLines the number of lines which should be read. set to 0 to read the whole file
	 * @return a ArrayList<String> that holds the data of the file
	 */
	public static ArrayList<String> readInListFromSeveralFiles(String directory, String splitter, int numberOfLines) {
		ArrayList<String> list = new ArrayList<String>();
		int file_counter = 0;
		try { 
			while(true){
				String file = directory+"_"+file_counter;
				file_counter++;
				File f = new File(file);
				if(f.exists()){
					FileReader fReader = new FileReader(file);
					BufferedReader bReader = new BufferedReader(fReader);
					int counter = 1;
					while (bReader.ready()&&(counter<=numberOfLines||numberOfLines==0)) {
						String line = bReader.readLine();
						line = line.trim();
						if (!line.isEmpty()) {
							String[] split_line = line.split(splitter);
							for(String word : split_line){
								list.add(word);
							}
						}
						counter++;
					}
					fReader.close();
					bReader.close();
				} else { break; }
			}
		} catch (IOException ioe) { 
			ioe.printStackTrace();
		}
		return list;
	}
	
	/**
	 * Reads text from a file and stores the data in a ArrayList<String>. The start and end of one entry in the ArrayList is determined by a variable
	 * 
	 * @param file the input file
	 * @param splitter the variable after which the text is split. set to "\n" if the entries in the ArrayList should correspond to lines in the input file
	 * @param numberOfLines the number of lines which should be read. set to 0 to read the whole file
	 * @return a ArrayList<String> that holds the data of the file
	 */
	public static ArrayList<String> readInList(String file, String splitter, int numberOfLines) {
		ArrayList<String> list = new ArrayList<String>();
		int file_counter = 0;
		try { 
			FileReader fReader = new FileReader(file);
			BufferedReader bReader = new BufferedReader(fReader);
			int counter = 1;
			while (bReader.ready()&&(counter<=numberOfLines||numberOfLines==0)) {
				String line = bReader.readLine();
				line = line.trim();
				if (!line.isEmpty()) {
					String[] split_line = line.split(splitter);
					for(String word : split_line){
						list.add(word);
					}
				}
				counter++;
			}
			fReader.close();
			bReader.close();
		} catch (IOException ioe) { 
			ioe.printStackTrace();
		}
		return list;
	}
	
	/**
	 * Reads line-wise from a file containing a configuration for the word alignment algorithm 
	 * 
	 * @param file the configuration file
	 *
	 * @return a HashMap<String, String> that holds the configuration keys as keys and the values as values
	 */
	public static HashMap<String, String> readConfigFile(String file) {
		HashMap<String, String> parameters = new HashMap<String, String>();
		try { 
			FileReader fReader = new FileReader(file);
			BufferedReader bReader = new BufferedReader(fReader);
			while (bReader.ready()) {
				String line = bReader.readLine();
				line = line.trim();
				if (!line.isEmpty()) {
					String[] split_line = line.split("\\:");
					if(split_line.length == 2){
						parameters.put(split_line[0], split_line[1]);
					}
				}
			}
			bReader.close();
		} catch (IOException ioe) { 
			ioe.printStackTrace();
		}

		return parameters;
	}

	/**

	 * Reads line-wise from a file containing a configuration for the word alignment algorithm 
	 * 
	 * @param file the configuration file
	 *
	 * @return a HashMap<String, String> that holds the configuration keys as keys and the values as values
	 */
	public static HashMap<String, String> readVocabulary(String file) {
		HashMap<String, String> hash_table_reverse = new HashMap<String, String>();
		try { 
			FileReader fReader = new FileReader(file);
			BufferedReader bReader = new BufferedReader(fReader);
			while (bReader.ready()) {
				String line = bReader.readLine();
				line = line.trim();
				if (!line.isEmpty()) {
					String[] split_line = line.split("\\t");
					if(split_line.length == 2){
						hash_table_reverse.put(split_line[1], split_line[0]);
					}
				}
			}
			bReader.close();
		} catch (IOException ioe) { 
			ioe.printStackTrace();
		}

		return hash_table_reverse;
	}
}
