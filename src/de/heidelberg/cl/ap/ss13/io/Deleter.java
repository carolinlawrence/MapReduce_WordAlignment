package de.heidelberg.cl.ap.ss13.io;

import java.io.File;

/**
 * A class that handles deletion of files and directories in either the normal file system or the HDFS
 * 
 * @author Carolin Haas (carhaas1@gmail.com)
 */
public class Deleter {

	/**
	 * Given either a file or directory, it deletes either the file or every file or directory within the directory
	 *
	 * @param file the file or directory to be deleted
	 */
	public static void delete(File file){
		try{
	   	if(file.isDirectory()){
	   		if(file.list().length==0){
					file.delete(); 
    		} else {
       	  String files[] = file.list();
       	  for (String del : files) {
        		File fileDelete = new File(file, del);
        	  delete(fileDelete);
        	}
					if(file.list().length==0){
          	file.delete();
					}
				}
    	}else{
    		file.delete();
    	}
		} catch (Exception e){
			System.out.println(e);
		}
	}
}
