package org.myorg;

import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

import org.apache.log4j.Logger;

public class TFIDF extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(TFIDF.class);
	private static final String COUNT = "filecount";

	public static void main(String[] args) throws Exception {
		// Tool Runner is used to parse the hadoop command line arguments and
		// run our class.
		int res = ToolRunner.run(new TFIDF(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		Path path_ = new Path(args[0]);
		FileSystem fileSystem = FileSystem.get(getConf()); // Gets the FileSystem from the Tool Configuration
		ContentSummary contentSummary = fileSystem.getContentSummary(path_); // Gets the ContentSummary at the given input path
		long fileCount = contentSummary.getFileCount(); // Gets the file count from the content summary.
		getConf().set(COUNT, "" + fileCount); // Sets the file count to the configuration.

		JobControl jobControl = new JobControl("JobChain"); // Creates a JobControl instance to monitor the status of the jobs.
		Job job = Job.getInstance(getConf(), "TF"); // Creates and name the job to run the map and reduce tasks.
													// This Job is meant to calculate the term frequency.
		job.setJarByClass(this.getClass());
		FileInputFormat.addInputPath(job, new Path(args[0])); // Sets the args[0] as the input path for the 1st job.
		FileOutputFormat.setOutputPath(job, new Path(args[1])); // Sets the args[1] as the output path for the 1st job.
		job.setMapperClass(Map1.class); // Sets the Map1.class to the 1st job.
		job.setReducerClass(Reduce1.class); // Sets the Reduce1.class to the 1st job.
		job.setOutputKeyClass(Text.class); // Sets the type of output key for the 1st job.
		job.setOutputValueClass(DoubleWritable.class); // Sets the type of output value for the 1st job.
		ControlledJob controlledJob = new ControlledJob(getConf()); // Creates a controlled job to add as depending job for job 2.
		controlledJob.setJob(job);

		jobControl.addJob(controlledJob);

		Job job2 = Job.getInstance(getConf(), "TFIDF"); // Creates and name the job to run the map and redcuce tasks.
														// This job is meant to calculate the tfidf based on the output of previous job.
		job2.setJarByClass(this.getClass());

		FileInputFormat.addInputPath(job2, new Path(args[1])); // Sets the args[1] where the output of the previous job is stored as input.
		FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/final")); // Sets the args[1]/final as the output for this job.
		job2.setMapperClass(Map2.class); // Sets the Map2.class for the 2nd job.
		job2.setReducerClass(Reduce2.class); // Sets the Recude2.class for the 2nd job.
		job2.setOutputKeyClass(Text.class); // Sets the type of the output key for the 2nd job.
		job2.setOutputValueClass(Text.class); // Sets the type of the output value for the 2nd job.
		ControlledJob controlledJob2 = new ControlledJob(getConf());
		controlledJob2.setJob(job2);

		controlledJob2.addDependingJob(controlledJob); // Adds Job 1 as a dependency job for job 2.

		jobControl.addJob(controlledJob2);

		Thread jobControlThread = new Thread(jobControl);
		jobControlThread.start();
		while (!jobControl.allFinished()) { // Waits till both the jobs are finished.
			try {
				Thread.sleep(5000);
			} catch (Exception e) {

			}

		}
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class Map1 extends
			Mapper<LongWritable, Text, Text, DoubleWritable> {
		private final static DoubleWritable one = new DoubleWritable(1);
		private Text word = new Text();
		private static final Pattern WORD_BOUNDARY = Pattern
				.compile("\\s*\\b\\s*");

		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {
			String line = lineText.toString(); // Converts each line from the input to string.
			Text currentWord = new Text(); // Text object to hold the current word.
			FileSplit fileSplit = (FileSplit) context.getInputSplit(); // Gets the FileSplit to obtain the fileName.
			String fileName = fileSplit.getPath().getName();
			for (String word : WORD_BOUNDARY.split(line)) { // Splits each line into words based on the pattern in WORD_BOUNDARY.
				if (word.isEmpty()) {
					continue;
				}
				currentWord = new Text(word.toLowerCase());	// Converts each word to lowerCase and adds it to the currentWord Text Object.
				Text fileString = new Text(currentWord + "####" + fileName);	// Appends each word with the file name with a delimiter "####".
				context.write(fileString, one); // Writes the intermediate data of Map1.
			}
		}
	}

	public static class Reduce1 extends
			Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		@Override
		public void reduce(Text word, Iterable<DoubleWritable> counts,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (DoubleWritable count : counts) {	// Calculates the number of occurances of each word
				sum += count.get();
			}
			double termFrequency = 1.00 + Math.log10(sum);	//Calculates the TermFrequency using the equation#1
			context.write(word, new DoubleWritable(termFrequency));	// Writes the output of Reduce 1 to HDFS.
		}
	}

	public static class Map2 extends Mapper<LongWritable, Text, Text, Text> {
		
		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
			String line = lineText.toString();	//Converts each line from the input into a string.

			if (line.isEmpty()) {	// Ends the task if the line is empty.
				return;
			}
			line = line.toLowerCase(); // Converts the line to lowercase.
			String[] splitLine = line.split("####");	// Splits the line based on the delimiter "####".
			if (splitLine.length < 2) {	// Ends the task if the line has less than 2 terms.
				return;
			}

			Text words = new Text(splitLine[0]);	//Stores the "word" part of each line in a Text Object words.
			Text wordSummary = new Text(splitLine[1]);	// Stores the remaining part of the line in wordSummary TextObject.
			context.write(words, wordSummary);	// Writes the output of Map2.
		}
	}

	public static class Reduce2 extends Reducer<Text, Text, Text, DoubleWritable> {
		@Override
		public void reduce(Text word, Iterable<Text> counts, Context context) throws IOException, InterruptedException {
			double sum = 0;
			double tfidf = 0;
			long frequencyCount = context.getConfiguration().getLong(COUNT,1); // Gets the fileCount from the configuration.
			ArrayList<Text> wordInFiles = new ArrayList<>();

			for(Text count : counts) {	// Runs for each string in the value list.
				sum++; // count will be incremented for each string.
				wordInFiles.add(new Text(count.toString()));
			}
			for (Text having : wordInFiles) {
				String[] parts = having.toString().split("\t");	// Splits the file name and term frequency using the delimiter "\t". 
				double idf = Math.log10(1.0 + (frequencyCount/sum)); //calculate the IDF using equation #3
				tfidf = Double.parseDouble(parts[1])*idf; // Caluclate the TF-IDF scores using euqation #4
				context.write(new Text(word+"####"+parts[0]), new DoubleWritable(tfidf)); // Writes the output of Reduce 2 to HDFS.
			}
		}
	}
}
