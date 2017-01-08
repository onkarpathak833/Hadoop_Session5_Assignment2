package Session05Assignment02;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Task02Mapper {
	
	public static class Mapper2 extends Mapper<LongWritable, Text, Text, ProductDataType> {
		
		public void map(LongWritable key,Text line,Context context) throws IOException, InterruptedException {
			
			String value = line.toString();
			String []inputArray = value.split("\\|");
			String companyName = inputArray[0];
			String product = inputArray[1];
			int size = Integer.parseInt(inputArray[2]);
			context.write(new Text(companyName), new ProductDataType(new Text(product), new IntWritable(size)));
		}
		
	}

}
