package hbase.Exercise5;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class ProductAnalyzer {

	public enum Counters { ROWS, COLS, VALID, ERROR }
	
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
	
	static class ProductReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		@SuppressWarnings("unused")
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int count = 0;
			for(IntWritable one: values) count++;
			context.write(key, new IntWritable(count));
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		String table = args[0];
		String output = args[1];
		
		Scan scan = new Scan();
		scan.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("pdk"));
		
		Configuration conf = HBaseConfiguration.create();
		Job job = new Job(conf, "Analyze product key");
		job.setJarByClass(ProductAnalyzer.class);
		TableMapReduceUtil.initTableMapperJob(table, scan, ProductMapper.class, Text.class, IntWritable.class, job);
		job.setReducerClass(ProductReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(1);
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
