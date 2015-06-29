package ucsc.hadoop.homework2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Reducer;

public class DescendingSortActorMovieCountReducer extends Reducer<IntWritable, Text, IntWritable, Text>{
	
	private final static IntWritable COUNT = new IntWritable(0);
	private final static Text ACTOR = new Text("");
	
	
	public void reduce(IntWritable count, Text actor, Context context) 
			throws IOException, InterruptedException{
		
		COUNT.set(count.get());
		ACTOR.set(actor);
		context.write(COUNT, ACTOR);
		
	}
	
}
