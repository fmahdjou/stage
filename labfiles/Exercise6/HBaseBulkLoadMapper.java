import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HBaseBulkLoadMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
	public void map(LongWritable key, Text value, Context context) {
		// inputdata[0] is the rowkey
		// inputdata[1] is the first_name
		// inputdata[2] is the last_name
		// inputdata[3] is the email
		// inputdata[4] is the phone_number
		try {
			// Creates a Configuration class using context and creates a Put class using a splitted string
			Configuration configuration = context.getConfiguration();
			String[] inputdata = value.toString().split(",");
			Put put = new Put(Bytes.toBytes(inputdata[0]));
			// Add first_name and last)name to COLUMN_FAMILY_1 with their respective inputs given above.
			// Use the add function of the Put variable and make sure all the variables are in Bytes!
			

			// Add email and phone_nymber to COLUMN_FAMILY_2 with their respective inputs given above.
			// Use the add function of the Put variable and make sure all the variables are in Bytes!
			

			// Finalize it by using the write function on the Context variable using the hbase table name and the Put variable
			
		}
		catch(Exception exception) {
			exception.printStackTrace();
		}
	}
}
