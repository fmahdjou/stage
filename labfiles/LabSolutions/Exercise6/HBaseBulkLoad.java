import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;

public class HBaseBulkLoad {
	public static void BulkLoad(String pathToHFile, String tableName) {
		try {
			//Create a new configuration
			Configuration configuration = new Configuration();
			configuration.set("mapreduce.child.java.opts", "-Xmx1g");
			//Add HBase configuration files to configuration
			HBaseConfiguration.addHbaseResources(configuration);
			//Crease an LoadIncrementalHFile variable using configuration
			LoadIncrementalHFiles loadFiles = new LoadIncrementalHFiles(configuration);
			//Create an HTable variable with the tableName and configuration
			HTable hTable = new HTable(configuration, tableName);
			//Load the file with doBulkLoad function to the correct path and HTable variable
			loadFiles.doBulkLoad(new Path(pathToHFile), hTable);
			System.out.println("Finished Bulk Load.");
		}
		catch(Exception exception) {
			exception.printStackTrace();
		}
	}
}
