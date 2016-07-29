package hbase.Exercise5;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;



public class ProductAnalyzer_Sink {

	public enum Counters { ROWS, COLS, VALID, ERROR }
	public static final byte[] COLUMN_FAMILY = Bytes.toBytes("cf");
	public static final byte[] COLUMN = Bytes.toBytes("cnt");
	
	static class ProductMapper extends TableMapper<Text, IntWritable>{
		private IntWritable ONE = new IntWritable(1); 
		
		@Override
		public void map(ImmutableBytesWritable row, Result columns, Context context) throws IOException{
			context.getCounter(Counters.ROWS).increment(1);
			String value = null;
			try{
				for(KeyValue kv: columns.list()){
					context.getCounter(Counters.COLS).increment(1);	
					value = Bytes.toStringBinary(kv.getValue()); 
					context.write(new Text(value), ONE); 
					context.getCounter(Counters.VALID).increment(1);
				}
			} catch (Exception e) {
				e.printStackTrace();
				System.err.println("Row: " + Bytes.toStringBinary(row.get()) + ", value = " + value);
				context.getCounter(Counters.ERROR).increment(1);
			}
			
		}
	
	}
	
	static class ProductReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable>{
		@SuppressWarnings("unused")
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int count = 0;
			for(IntWritable one: values) count++;
			
			// create the Put object
			// add the value into the Put object
			// emit the value out of the reducer
	
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		String sourceTable = args[0];
		String targetTable = args[1];
		
		Configuration conf = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(conf);
		HTableDescriptor desc = new HTableDescriptor(targetTable);
		HColumnDescriptor colFamilyDesc = new HColumnDescriptor(COLUMN_FAMILY);
		desc.addFamily(colFamilyDesc);
		admin.createTable(desc);
		
		Scan scan = new Scan();
		scan.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("pdk"));
		
		Job job = new Job(conf, "Analyze product key");
		job.setJarByClass(ProductAnalyzer_Sink.class);
		TableMapReduceUtil.initTableMapperJob(sourceTable, scan, ProductMapper.class, Text.class, 
				IntWritable.class, job);
		
		// use the TableMapReduceUtil.initTableReducerJob to set up the sink
		
		
		job.setNumReduceTasks(1);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
