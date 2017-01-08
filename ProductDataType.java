package Session05Assignment02;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class ProductDataType implements WritableComparable<ProductDataType> {
	
	private Text product;
	private IntWritable size;
	
	public ProductDataType() {
		// TODO Auto-generated constructor stub
	 set(new Text(), new IntWritable());
	}
	
	public ProductDataType(Text product,IntWritable size) {
		// TODO Auto-generated constructor stub
	 this.product = product;
	 this.size = size;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		product.readFields(in);
		size.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		product.write(out);
		size.write(out);
	}
	
	@Override
	public int hashCode() {
		
		
		return product.hashCode()*4571+size.hashCode();
		
	}

	@Override
	public int compareTo(ProductDataType o) {
		// TODO Auto-generated method stub
		int cmp = product.compareTo(o.getProduct());
		if(cmp!=0) {
			return cmp;
		}
		
		return size.compareTo(o.getSize());
	}
	
	public boolean equals(Object o) {
		if(o instanceof ProductDataType) {
			ProductDataType p =(ProductDataType) o;
			return (product.equals(p.getProduct()) && size.equals(p.getSize()));
		}
		
		return false;
		
	}
	
	public void set(Text product, IntWritable size) {
		this.product = product;
		this.size = size;
		
	}
	
	public Text getProduct() {
		return product;
	}
	
	public IntWritable getSize() {
		return size;
	}

}
