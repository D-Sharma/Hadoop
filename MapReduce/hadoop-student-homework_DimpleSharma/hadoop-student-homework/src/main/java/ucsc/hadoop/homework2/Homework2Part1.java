/*
 * This class implements a simple map reduce application to show which actors played a movie
 */
package ucsc.hadoop.homework2;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import ucsc.hadoop.homework2.MovieTokenMapper;
import ucsc.hadoop.homework2.MovieActorReducer;
import ucsc.hadoop.util.ConfigurationUtil;

public class Homework2Part1 extends Configured implements Tool{
	
	private static final Log LOG = LogFactory.getLog(Homework2Part1.class);
	
	public int run(String args[]) throws Exception {
		Configuration conf = getConf();
		
		if(args.length !=2 ){
			System.err.println("Usage: movieactorlist <in> <out>");
			System.exit(2);
		}
		
		ConfigurationUtil.dumpConfigurations(conf, System.out);
		
		LOG.info("input: " + args[0] + " output: " + args[1]);
		
		//Configure job to run a map reduce job
		Job job = new Job(conf, "movie actor list");		
		job.setJarByClass(Homework2Part1.class);		
		
		//Set class that contain implementation of Map and Reduce
		job.setMapperClass(MovieTokenMapper.class);
		job.setReducerClass(MovieActorReducer.class);

		//Classes for output of Map task
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		//Classes for output of reduce task
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//Format output file, values separated by tab and record separated by new line.
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//Provide input file path for input to map task and output file path for output of reduce task 
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean result = job.waitForCompletion(true);
		//return (result) ? 0 : 1;
		return result ? 0:1;
	
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		
		int exitCode = ToolRunner.run(new Homework2Part1(), args);
		System.exit(exitCode);
		
	}

}
