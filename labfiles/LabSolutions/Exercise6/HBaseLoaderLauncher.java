import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HBaseLoaderLauncher extends Configured implements Tool{
        public static void main(String[] args) {
                try {
                        ToolRunner.run(HBaseConfiguration.create(), new HBaseLoaderLauncher(), args);
                }
                catch(Exception exception) {
                        exception.printStackTrace();
                }
        }

        @Override
        public int run(String[] args) throws Exception {
                // args[0] is the input path
                // args[1] is the output path

		//Creates a Configuration class and uses the set function to set the data seperator as ','
                Configuration configuration = getConf();
                configuration.set("data.seperator", ",");
		// Set the respective values for the tabe names and column family names using the set function of Configuration
		// Note: hbase.table.name = table name and COLUMN_FAMILY_1, COLUMN_FAMILY_2 are respectively column family 1 and 2
		configuration.set("hbase.table.name", "Info_Table");
		configuration.set("COLUMN_FAMILY_1", "cf1");
		configuration.set("COLUMN_FAMILY_2", "cf2");
		// Create a new Job class using the configuration variable
               	Job job = new Job(configuration);
		// Sets some of the attributes of the job using classes and string(s)
                job.setJarByClass(HBaseLoaderLauncher.class);
                job.setJobName("HBase Bulk Load..");
                job.setInputFormatClass(TextInputFormat.class);
                job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		// Set the mapper class with the setMapperClass function of Job. HBaseBulkLoadMapper.class will be our mapper class
                job.setMapperClass(HBaseBulkLoadMapper.class);
		// Set the input path with the addInputPaths functoin of FileInputFormat using the Job class and the respective args
                FileInputFormat.addInputPaths(job, args[0]);
                FileSystem.getLocal(getConf()).delete(new Path(args[1]), true);
		// Set the  output path with the setOutputPath function of FileInputFormat using the Job class and the respective args
               	FileOutputFormat.setOutputPath(job, new Path(args[1]));
		// Use the setMapOutputValue function of the Job class specificying the Put.class
                job.setMapOutputValueClass(Put.class);
		// Waits for Job completion after configuring the Incremental load to our Hbase Table
                HFileOutputFormat.configureIncrementalLoad(job, new HTable(configuration, "Info_Table"));
                System.out.println("Waiting for job completion...");
                job.waitForCompletion(true);
                if (job.isSuccessful()) {
                        HBaseBulkLoad.BulkLoad(args[1], "Info_Table");
                        System.out.println("Job Complete! :)");
                }
                else {
                        System.out.println("Job Failed.");
                }
		return 0;
        }
}

