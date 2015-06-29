/*
 * This class implements a simple map reduce application to count how many movies each actor have played in.
 * The output of movie counts and actor is sorted in descending order. 
 */

package ucsc.hadoop.homework2;

import java.io.File;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

import ucsc.hadoop.util.ConfigurationUtil;

public class Homework2Part2 extends Configured implements Tool{
	
	private static Log LOG = LogFactory.getLog(Homework2Part2.class);
	
	private final static String OUTPUT_PATH = "/TEMP_OUTPUT";
	
	public int run(String args[]) throws Exception{
		
		Configuration conf = getConf();
		
		if(args.length != 2){
			System.err.println("Usage actor moviecount list: <in> <out>");
			System.exit(2);
		}
		
		ConfigurationUtil.dumpConfigurations(conf, System.out);
		
		LOG.info("input: " + args[0] + "output: " + args[1]);
		
		/*
		 * Map reduce job 1 job configuration which outputs actor name and movies count of movies the actor played in.
		 */
		Job job = new Job(conf, "actor moviecount list");		
		job.setJarByClass(Homework2Part2.class); //Create jar file by class
		
		//Set MapReduce job 1
		job.setMapperClass(ActorMovieCountTokenizerMapper.class);   //Set mapper class
		job.setReducerClass(ActorMovieCountReducer.class);  //Set reducer class
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//job.setOutputFormatClass(TextOutputFormat.class);   //set output record format, field separated by tab and row separated by newline
		
		File input = new File(args[0]);
		String outputjob1 = input.getParent() + OUTPUT_PATH;
		
		//Provides file input path for map task and output file path for reduce task
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(outputjob1));
		
		boolean result = job.waitForCompletion(true);
		
		//If job 1 is not successful print error and exit
		if(!result)
		{
			System.err.println("Actor moviecount list Job 1 not successful!");
			System.exit(2);
		}
		
		/*
		 * Map reduce job 2 job configuration which outputs movie count of movies an 
		 * actor played in and actor name sorted in descending order of movie count
		 */
		conf = getConf();
		Job job2 = new Job(conf, "actor moviecount list - job2");
		job2.setJarByClass(Homework2Part2.class); //Create jar file by class
		job2.setNumReduceTasks(1);
		job2.setMapperClass(DescendingSortActorMovieCountMapper.class);   //Set mapper class
		job2.setReducerClass(DescendingSortActorMovieCountReducer.class);  //Set reducer class
		
		//Set key & value data type for intermediate output from mapper
		job2.setMapOutputKeyClass(IntWritable.class);
		job2.setMapOutputValueClass(Text.class);
		
		//Set key & value data type for final output from reducer
		job2.setOutputKeyClass(IntWritable.class);
		job2.setOutputValueClass(Text.class);
		
		//Provide a custom descending sort
		job2.setSortComparatorClass(IntComparatorDescending.class);
		//job2.setOutputFormatClass(TextOutputFormat.class);   //set output record format, field separated by tab and row separated by newline		
		//job2.setInputFormatClass(TextInputFormat.class);
		
		// Provide input file  path for map task and output file path for reduce task 
		FileInputFormat.addInputPath(job2, new Path(outputjob1 + "/part-r-00000"));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		
		boolean result2 = job2.waitForCompletion(true);
		
		return result2? 0:1;
		
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		int exitcode = ToolRunner.run(new Homework2Part2(), args);
		//System.out.println(Homework2Part2.class.getName());
		System.exit(exitcode);
	}

}
