/*
 * This class implements a reducer.
 * The MovieActorReducer contains implementation of reduce function which
 * outputs a key value pair of movie and actors who acted in that movie separated by semicolon ";"
 */
package ucsc.hadoop.homework2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class MovieActorReducer extends Reducer<Text, Text, Text, Text> {
	private Text result = new Text();
	
	@Override
	public void reduce(Text movie, Iterable<Text> values, Context context) 
			 throws IOException, InterruptedException {
			
		String actors = "";
		for (Text actor : values) {
			actors += actor + ";";
		}
		result.set(actors);
		context.write(movie, result);
	}
}

