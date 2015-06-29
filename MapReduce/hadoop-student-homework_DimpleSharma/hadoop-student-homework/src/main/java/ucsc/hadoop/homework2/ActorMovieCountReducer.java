package ucsc.hadoop.homework2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ActorMovieCountReducer extends Reducer<Text,IntWritable, Text, IntWritable>{
	
	private final static IntWritable result = new IntWritable(0);
	
	@Override
	public void reduce(Text actor, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException{
		
		int moviecount = 0;		
		for(IntWritable count: values){
			moviecount += count.get();
		}
		
		result.set(moviecount);
		context.write(actor, result);
	}

}
