MapReduce_WordAlignment
=======================

-------------------------------------------------
Final Project for Advanced Programming SS2013 handed in by Carolin Haas
-------------------------------------------------

Contents
-------------------------------------------------
1. Source code & Compilation
2. Documentation
3. Corpora
4. Configuration files
5. Scripts

1. Source code & Compilation
-------------------------------------------------
NOTE: This program requires hadoop to run
The source code can be found in the "src" and following sub folders.
The compiled code can be found in WordAlignment.jar
Should further compilation be required, this can be done with the following commands:

javac -source 1.6 -target 1.6 -classpath `hadoop classpath`:. -d classes src/de/heidelberg/cl/ap/ss13/*/*.java src/de/heidelberg/cl/ap/ss13/*/*/*.java
jar -cvf WordAlignment.jar -C classes/ .

The program can be run either with the provided scripts (see 5. Scripts) or with the following command (assuming you are in the scripts folder):

hadoop jar ../WordAlignment.jar de.heidelberg.cl.ap.ss13.helper.Main <configuration_file>

For more information on the configuration file see 4. Configuration files.



2. Documentation
-------------------------------------------------
The javadoc documentation lies in the folder "doc".
Open index.html for the start page.


3. Corpora
-------------------------------------------------
The only text files included already is a toy example consisting of two "sentences" in French and English.
Here "maison blue" is aligned with "blue house" and "maison" with "house".
They can be used to demonstrate that the algorithms work correctly.
These files can be found in the folder "corpora"
A reference file "Toy_Calculation" containing the hand made calculations can be found in the top level directory.
To find out how to run the program with this toy example, refer to (5. Scripts).
To acquire further corpora used for testing in this work also refer to (5. Scripts).


4. Configuration files
-------------------------------------------------
NOTE: file paths need to be relative to where the jar file is (not relative to the script location)
The configuration file sums up important needed information to run the program in one file.
They can be found in the folder "configs".
The following format is used, per line: "key:value" (E.g. source:my_source_file.txt)

Required keys are:
source - the source language file of your parallel corpus in which each line contains 1 tokenized sentence
target - the target language file of your parallel corpus in which each line contains 1 tokenized sentence
output - the file location and the file name of where the viterbi alignment should be written
root-dir - a existing directory where intermediate steps are saved (NOTE: unless you specify \"clean-up:false\", 
			the created intermediate files will be deleted at the end of the program)

The optional keys are:
ibm1 (DEFAULT value: 5) - specify how many iterations of the IBM 1 model there should be at most
hmm (DEFAULT value: 5) - specify how many iterations of the HMM model there should be at most
null-token (DEFAULT value: false) - specify true if you want a null token to be added
clean-up (DEFAULT value: true) - specify false if you want the intermediate files to not be deleted
reverse-order (DEFAULT value: true) - specify false if you don't want to learn the viterbi alignment in both directions
log-prob (DEFAULT value: true) - specify false if you don't want to have the probabilities in log space (NOTE: underflow can happen even with small input files!
number-reducers (DEFAULT value: 1) - specify how many reducers you want to use

The following configuration files are already included:
toy - configures the program to do a test run with the toy example. It does not delete the files after so that they can be looked at for correctness.
		It only runs 1 iteration of IBM 1 and 1 iteration of the HMM and the probabilities are not in the log space.
		This way the results can be compared with the file "Toy_Calculation" which can be found in the top level directory.
nc5000 - runs 5 iterations of IBM 1 and 5 iterations of the HMM in the log space on the first 5,000 sentences of the news commentary corpus, in both directions (http://www.statmt.org/wmt13/translation-task.html#download, 24/09/2013)
nc20000 - runs 5 iterations of IBM 1 and 5 iterations of the HMM in the log space on the first 20,000 sentences of the news commentary corpus(http://www.statmt.org/wmt13/translation-task.html#download, 24/09/2013)
nc20000_reverse - runs 5 iterations of IBM 1 and 5 iterations of the HMM in the log space on the first 20,000 sentences of the news commentary corpus, in reversed direction. It was necessary to split the runs for this corpus because otherwise Java would run out of memory during the Viterbi algorithm.(http://www.statmt.org/wmt13/translation-task.html#download, 24/09/2013)
utopia - runs 5 iterations of IBM 1 and 5 iterations of the HMM in the log space on the utopia corpus, in both directions (https://github.com/wlin12/SMMTT, 24/09/2013)


5. Scripts
-------------------------------------------------
NOTE: All scripts assume that the folder structure of this project is not altered
NOTE2: Some scripts requires Moses (http://www.statmt.org/moses/index.php?n=Main.HomePage, 24/09/2013) and possibly GIZA++ (https://code.google.com/p/giza-pp/, 24/09/2013)

Script to clean up:
cleanUp.sh: If the program Word Alignment is run with the configuration "clean-up" set to false, then the intermediate files are not deleted.
			To run the program again though, these files have to be deleted. This can be done with this script.

			
Scripts to run the program:
runToy.sh: This script just runs the Word Alignment program using the toy example and its configuration "toy". Note that running cleanUp.sh is required before the program Word Alignment can be called again.
runNC5000.sh: This script just runs the Word Alignment program using the NC 5,0000 sub set and its configuration "nc5000".


Scripts to obtain further corpora:
getNCMoses.sh: This scrip obtains the first 5,000 as well as the first 20,000 sentences from the news commentary corpus for the languages German and English.
			   The files are then pre-processed in the following way: lowercased, all sentences longer than 80 words removed and tokenized.
			   It also downloads a test set and applies the same pre-processing steps to it.
			   This file requires you to specify the location of your Moses directory. If you don't have Moses you might consider using getNC.sh. This script is needed though if you want to replicate the SMT experiment.
getNC.sh: This file doesn't require Moses but note that the pre-processing isn't as good - the text is only lowercased. It also doesn't download the test set.
getUtopiaMoses.sh: This script obtains the utopia corpus, the languages are English and Mandarin.
				   The files are then pre-processed in the following way: for English: lowercased and tokenized. for Mandarin:  basic tokenization - after every non-alphanumeric letter a space is inserted. 
				   It also downloads a test set and applies the same pre-processing steps to it.
				   This file requires you to specify the location of your Moses directory. If you don't have Moses you might consider using getNC.sh. This script is needed though if you want to replicate the SMT experiment.
getUtopia.sh: This file doesn't require Moses but note that the preprocessing isn't as good - the English text is only lowercased. It also doesn't download the test set.


Scripts to build a SMT system:
(NOTE: the systems don't have their weights tuned to allow the scripts to finish faster)

For all scripts: 
Once a script is done, all files created can be found in the folder "systems/<[GIZA|MapReduce]_[nc20000|utopia]>" (sub folder depending on script name) when navigating from this project's top level directory.
Of special notice might be the file "translated.txt" which contains the translation created by the system and "bleu.txt" which contains the BLEU score.

buildGIZA_nc20000.sh: This script builds a basic SMT system with the NC 20,000 sub set, using Moses standard options. The only changed options are that IBM 1 runs for 5 iterations, IBM 4 for 1 and HMM for 5 again.
buildMapReduce_nc20000.sh: This script builds a basic SMT system with the NC 20,000 sub set, creating the Word Alignment with this program (configuration file "nc20000" & "nc20000_reverse") and then continuing using Moses standard options.
buildGIZA_utopia.sh: This script builds a basic SMT system with the utopia corpus, using Moses standard options. The only changed options are that IBM 1 runs for 5 iterations, IBM 4 for 1 and HMM for 5 again.
buildMapReduce_utopia.sh: This script builds a basic SMT system with the utopia corpus,  creating the Word Alignment with this program (configuration file "utopia") and then continuing using Moses standard options. The only changed options are that IBM 1 runs for 5 iterations, IBM 4 for 1 and HMM for 5 again.

