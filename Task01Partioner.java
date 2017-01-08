package Session05Assignment01;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


public class Task01Partioner {

	public static class Partitioner1 extends Partitioner<Text, IntWritable> {

		@Override
		public int getPartition(Text key, IntWritable value, int numReduceTasks) {
			// TODO Auto-generated method stub
			String companyName = key.toString();
			companyName = companyName.toUpperCase();
			System.out.println(" *** Inside custom partitioner.");
			if(numReduceTasks==0) {
				return 0;
			}
			if (!"null".equals(companyName) && companyName != null && !companyName.isEmpty()) {

				if (companyName.startsWith("A") || companyName.startsWith("B") || companyName.startsWith("C")
						|| companyName.startsWith("D") || companyName.startsWith("E") || companyName.startsWith("F")) {
					System.out.println(" *** Values in 1st partition.");
					return 1;
				}
				
				if(companyName.startsWith("G") || companyName.startsWith("H") || companyName.startsWith("I")
						|| companyName.startsWith("J") || companyName.startsWith("K") || companyName.startsWith("L")) {
					
					System.out.println(" *** Values in 2nd partition.");
					return 2;
				}
				if(companyName.startsWith("M") || companyName.startsWith("N") || companyName.startsWith("O")
						|| companyName.startsWith("P") || companyName.startsWith("Q") || companyName.startsWith("R")) {}
			
				System.out.println(" *** Values in 3rd partition.");
				return 3;
			}
			System.out.println(" *** All other values in 4th partition.");
			return 4;
		}

	}
}
