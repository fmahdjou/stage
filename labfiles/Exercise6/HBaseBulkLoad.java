import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;

public class HBaseBulkLoad {
	public static void BulkLoad(String pathToHFile, String tableName) {
		try {
			//Create a new configuration
			
			configuration.set("mapreduce.child.java.opts", "-Xmx1g");
			//Add HBase configuration files to configuration
			
			//Crease an LoadIncrementalHFile variable using configuration
			
			//Create an HTable variable with the tableName and configuration

			//Load the file with doBulkLoad function to the correct path and HTable variable

			System.out.println("Finished Bulk Load.");
		}
		catch(Exception exception) {
			exception.printStackTrace();
		}
	}
}
