package ucsc.hadoop.homework2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ActorMovieCountTokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
	
	private final static IntWritable ONE = new IntWritable(1);
	private final static Text ACTOR = new Text("");
	
	@Override
	public void map(Object key, Text value, Context context)
					throws IOException, InterruptedException{
		String[] tokens = value.toString().split("\\t");
		
		if(tokens.length >= 2 ){
			String actor = tokens[0];
			
			ACTOR.set(actor);
			context.write(ACTOR, ONE);
		}
		
	}
	
	

}
