package ucsc.hadoop.homework2;

import java.nio.ByteBuffer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparator;

public class IntComparatorDescending extends WritableComparator{
	
	protected IntComparatorDescending() {
		super(IntWritable.class);
	}

	@Override
	public int compare(byte[] byte1, int integer1, int length1, byte[] byte2, int integer2, int length2){
		
		//Integer
		Integer val1 = ByteBuffer.wrap(byte1, integer1, length1).getInt();
		Integer val2 = ByteBuffer.wrap(byte2, integer2, length2).getInt();
		
		//return Integer.compare(val1, val2);
		int result = val1 > val2? -1: val1 == val2? 0 :1; 
		return result;
		
	}

}
