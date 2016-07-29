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
			// increment the row counter here
			
			String value = null;
			try{
				for(KeyValue kv: columns.list()){
					context.getCounter(Counters.COLS).increment(1);	
					// get the value
					
					// emit the key/value out
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
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int count = 0;
			// increment count for each of the values in the Iterable passed in. 
			// emit the Key and total count out of the Reducer 

		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		// create String table
	    	// create String output 
		
		// create the scan object with the attribute 
		// to scan to just grab the cf:pdk column
		
		Configuration conf = HBaseConfiguration.create();
		Job job = new Job(conf, "Analyze product key");
		// set the class of the job that contains the mapper and reducer
		
		// Use the TableMapReduceUtil.initTableMapperJob() method to set up your job. 
		
		// assign the reducer using the job object
		
		// assign the reducer's output key class
		
		// assign the reducer's output value class
		
		// set the number of reduce tasks
		
		// set the output format
		
		
		// submit the job
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
}
