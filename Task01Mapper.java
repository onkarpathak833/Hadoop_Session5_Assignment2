package Session05Assignment01;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Task01Mapper {

	public static class Mapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		public void map(LongWritable key,Text line,Context context) {
			String value = line.toString();
			String []allValues = value.split("\\|");
			String companyName = allValues[0];
			try {
				context.write(new Text(companyName), new IntWritable(1));
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
