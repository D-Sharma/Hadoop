package ucsc.hadoop.homework2.tests;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

import ucsc.hadoop.homework2.ActorMovieCountReducer;
import ucsc.hadoop.homework2.ActorMovieCountTokenizerMapper;


public class Homework2Part2MRTest {

	
	//MapReduceDriver<Object, Text, Text, IntWritable, Text, IntWritable> mapreducedriver;
	
	@Before
	public void setup()	{
		
		
	}
	
	@Test
	public void testMapper() throws Exception{
		MapDriver<Object, Text, Text, IntWritable> mapdriver = new MapDriver<Object, Text, Text, IntWritable>();
		
		mapdriver.withMapper(new ActorMovieCountTokenizerMapper())
				 .withInput(new IntWritable(1), new Text("McClure, Mark (I)\tFreaky Friday"))
				 .withOutput(new Text("McClure, Mark (I)"), new IntWritable(1))
				 .runTest();
		
		System.out.println("Expected output " + mapdriver.getExpectedOutputs());	
	}
	
	@Test
	public void testReducer() throws Exception{
		ReduceDriver<Text, IntWritable, Text, IntWritable> reducedriver = new ReduceDriver<Text, IntWritable, Text, IntWritable>();
		
		List<IntWritable> valuelist = new ArrayList<IntWritable>();
		valuelist.addAll(Arrays.asList(new IntWritable(1), new IntWritable(1), 
							new IntWritable(1), new IntWritable(1), new IntWritable(1), 
							new IntWritable(1)));
		Text actor = new Text("McClure, Mark (I)");
		
		reducedriver.withReducer(new ActorMovieCountReducer())
					.withInput(actor, valuelist)
					.withOutput(new Text("McClure, Mark (I)"), new IntWritable(6))
					.runTest();
		
		System.out.println("Expected output " + reducedriver.getExpectedOutputs());	
	}
	
}
