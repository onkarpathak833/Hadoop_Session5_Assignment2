package Session05Assignment01;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Task01Reducer {

	public static class Reducer1 extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		public void reduce(Text key,Iterable<IntWritable> values, Context context) {
			int i =0;
			for(IntWritable val : values) {
				 i = i+1;
				
			}
			try {
				context.write(key, new IntWritable(i));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	}
}
