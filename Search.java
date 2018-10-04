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

public class Search extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(Search.class);
	private static final String ARGUMENTS = "args";

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Search(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {

		Job job = Job.getInstance(getConf(), "Search");	// Creates and names a job to run the Map and Reduce tasks.
		job.setJarByClass(this.getClass());

		String[] nargs = new String[args.length - 2];	// Obtain the Search Query Terms and stores them in another array.
		for (int i = 2; i < args.length; i++) {
			nargs[i - 2] = args[i];
		}

		job.getConfiguration().setStrings(ARGUMENTS, nargs); // Sets the query term arguments to the configuration object.
		FileInputFormat.addInputPath(job, new Path(args[0]));	// Sets the input path for the Search Job.
		FileOutputFormat.setOutputPath(job, new Path(args[1]));	// Sets the output path for the Search Job.
		job.setMapperClass(Map.class);	// Sets the Map Task for the job.
		job.setReducerClass(Reduce.class);	// Sets the Reduce Task for the job.
		job.setOutputKeyClass(Text.class);	// Sets the type of the output key.
		job.setOutputValueClass(DoubleWritable.class);	// Sets the type of the output value.

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {
		private final static DoubleWritable one = new DoubleWritable(1);
		private Text word = new Text();
		private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

		public void map(LongWritable offset, Text lineText, Context context)
	        throws IOException, InterruptedException {
	      String line = lineText.toString();	// Converts each of input lines into a string.
	      Text currentWord = new Text();
	      ArrayList<Text> arrayArguments = new ArrayList<>();
	      double tfidflist = 0;
	      
	      if(line.isEmpty()){	// Ends the task of input line is empty.
	    	  return;
	      }
	      
	      line = line.toLowerCase();	// Converts the input line to lowercase.
	      String[] splitLine = line.split("####");	// Splits the input line based on the delimiter "####".
	      
	      if(splitLine.length < 2){	// Ends the task if the input line has less than two terms.
	    	  return;
	      }
	      
	      Text word = new Text(splitLine[0]);
	      Text wordDetails = new Text(splitLine[1]);
	      
	      String[] noargs = context.getConfiguration().getStrings(ARGUMENTS); // Gets the list of query terms from the configuration.
	      boolean wordExists = false;
	      for(int i=0;i<noargs.length;i++){	// Checks for the word in the list of query terms.
	    	  String currentArgument = noargs[i].toLowerCase();
	    	  if(currentArgument.equalsIgnoreCase(splitLine[0])){
	    		  wordExists = true;
	    		  break;
	    	  }
	      }
	      if(!wordExists){ // Ends the task if the input word is not in the list of query terms.
	    	  return;
	      }
	      
	      String[] parts = wordDetails.toString().split("\t");	// Splits the filename and tfidf scores based on the delimiter "\t".
	      
	      word = new Text(parts[0]);
	      tfidflist = Double.parseDouble(parts[1]);
	      context.write(word, new DoubleWritable(tfidflist));	//Writes the output of Map task.
	      
	    }
	}

	public static class Reduce extends
			Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		@Override
		public void reduce(Text word, Iterable<DoubleWritable> counts,
				Context context) throws IOException, InterruptedException {
			double sum = 0.00;
			
			for (DoubleWritable count : counts) { //Adds the list of all values from the same text file.
				sum += count.get();
			}
			context.write(word, new DoubleWritable(sum));	// Writes the output of Reduce task to HDFS.
		}
	}
}
