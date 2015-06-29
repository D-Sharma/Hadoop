package ucsc.hadoop.homework2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DescendingSortActorMovieCountMapper extends Mapper<Object, Text, IntWritable, Text> {
	
	private final static IntWritable COUNT = new IntWritable(0);
	private final static Text ACTOR = new Text("");
	
	//A mapper reads key of a text file as LongWritable as it contains the byte offset of the current line
	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException{
		String[] tokens = value.toString().split("\\t");
		
		if(tokens.length == 2){
			
			int moviecount = Integer.parseInt(tokens[1]);
			String actor = tokens[0];
			
			COUNT.set(moviecount);
			ACTOR.set(actor);
			context.write(COUNT, ACTOR);
		}
		else
		{
			System.exit(2);
		}
		
	}

}
