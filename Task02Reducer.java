package Session05Assignment02;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Task02Reducer {
	
	
	public static class Reducer2 extends Reducer<Text, ProductDataType, Text, IntWritable> {
		
		public void reduce(Text key,Iterable<ProductDataType> values,Context context) throws IOException, InterruptedException {
			
			int i = 0;
			for(ProductDataType p : values) {
				i = i+1;
			}
			context.write(key, new IntWritable(i));
		}
	}

}
