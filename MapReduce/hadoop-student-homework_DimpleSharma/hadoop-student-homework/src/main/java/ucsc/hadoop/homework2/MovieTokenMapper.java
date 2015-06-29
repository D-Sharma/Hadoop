/*
 * This class implements a mapper class.
 * The MovieTokenMapper class contains implementation of a map function which outputs
 * a kay value pair of movie name and actor name.
 */

package ucsc.hadoop.homework2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class MovieTokenMapper extends Mapper<Object, Text, Text, Text> {
	//private final static IntWritable ONE = new IntWritable(1);
	//private final static IntWritable YEAR = new IntWritable();
	
	private static Text MOVIE = new Text("");
	private static Text ACTOR = new Text("");
	
	@Override
	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {
		String[] tokens = value.toString().split("\\t");
		
		if (tokens.length >= 2) {
			String movie = tokens[1];
			String actor = tokens[0];
			
			MOVIE.set(movie);
			ACTOR.set(actor);
			context.write(MOVIE, ACTOR);
		}
	}
}
