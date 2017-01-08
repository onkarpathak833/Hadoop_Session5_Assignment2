package Session05Assignment01;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Task02Combiner {
	
	public static class Combiner2 extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int i =0;
			for(IntWritable intVal : values) {
				
				if(intVal.get() > 1) {
					i = i+1;
				}
				
			 
			}
			context.write(key, new IntWritable(i));
			
		}
	}

}
