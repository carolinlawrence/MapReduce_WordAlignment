package de.heidelberg.cl.ap.ss13.io;

import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;

/**
 * A class to write text to a file
 * 
 * @author Carolin Haas (carhaas1@gmail.com)
 */
public class Writer {

	/**
	 * Writes a String to a specified file location, either appending or overwriting
	 * 
	 * @param text the string to be written
	 * @param file the file name and location of were the new file is to be created
	 * @param append true if output should be appended to existing file, false else
	 * @throws IOException
	 */
	public static void writeString(String text, String file, boolean append) throws IOException {
		FileWriter fWriter = new FileWriter(file, append);
		fWriter.write(text);
		fWriter.flush();
		fWriter.close();
	}

	public static void writeVocabulary(HashMap<String, Integer> map, String file, boolean append){
		try {
			FileWriter fWriter = new FileWriter(file,append);		
			for(String word : map.keySet()){
				fWriter.write(word+"\t"+map.get(word)+"\n");
			}
			fWriter.flush();
			fWriter.close();
		} catch (IOException ioe) { 
			ioe.printStackTrace();
		}
	}
}
