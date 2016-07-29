package hbase.Exercise3;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

public class HBase_SchemaTester {

	
	public static void main(String[] args) throws IOException{
		byte[] tableName = Bytes.toBytes("tableFromJava");
		byte[] columnFamily = Bytes.toBytes("columnFamilyFromJava");
		
		Configuration conf = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(conf);
		
		/*
		//Code to add table
		HTableDescriptor desc = new HTableDescriptor(tableName);
		HColumnDescriptor colFamilyDesc = new HColumnDescriptor(columnFamily);
		desc.addFamily(colFamilyDesc);
		admin.createTable(desc);
		boolean available = admin.isTableAvailable("tableFromJava");
		System.out.println("Table available: " + available);
		*/
		

		// Code to modify table
		HTableDescriptor htd1 = admin.getTableDescriptor(tableName);
		long oldMaxFileSize = htd1.getMaxFileSize();
		HColumnDescriptor colFamilyDesc2 = new HColumnDescriptor(Bytes.toBytes("cf2"));
		htd1.addFamily(colFamilyDesc2);
		htd1.setMaxFileSize(1024 * 1024 * 1024L);
		
		admin.disableTable(tableName);
		admin.modifyTable(tableName, htd1);
		admin.enableTable(tableName);
		
		HTableDescriptor htd2 = admin.getTableDescriptor(tableName);
		System.out.println("Old max file size = " + oldMaxFileSize);
		System.out.println("New max file size = " + htd2.getMaxFileSize());
		System.out.println("Equals: " + htd1.equals(htd2));
		System.out.println("New schema: " + htd2);

	}

}
