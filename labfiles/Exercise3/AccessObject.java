package hbase.Exercise3;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

public class AccessObject {

	// declaring the table and column family CONSTANTS
	public static final byte[] TABLE_NAME = 
		Bytes.toBytes("sales_fact");
	public static final byte[] COLUMN_FAMILY =
		Bytes.toBytes("cf");
	
	
	// declaring the columns CONSTANTS
	private static final byte[] UNIT_PRICE =
		Bytes.toBytes("up");
	private static final byte[] QUANTITY = 
		Bytes.toBytes("q");
	
	private Configuration conf = HBaseConfiguration.create();
	private HConnection my_connection;
		
	public AccessObject() throws IOException{
		this.my_connection = HConnectionManager.createConnection(conf);
	}
	
	public void closeConnection() throws IOException{
		my_connection.close();
	}
	
	
	public static Scan mkScan(){
		Scan s = new Scan();
		s.addFamily(COLUMN_FAMILY);
		return s;
	}
	
	
	public List<Result> getInfo(Filter filter) throws IOException{
		ArrayList<Result> list = new ArrayList<Result>();
		
		HTableInterface sales_fact = my_connection.getTable(TABLE_NAME);
		Scan scan = mkScan();
		/* add the column UNIT_PRICE */
		/* add the column QUANTITY */
		
		/*set the  filter*/
		
		ResultScanner scanner = sales_fact.getScanner(scan);
		for(Result r : scanner){
			System.out.println(r);
			list.add(r);
		}
		return list;
	}
	
	public void performIncrement(String rowkey, int viewCountValue, int anotherCountValue) throws IOException{
		HTableInterface sales_fact = my_connection.getTable(TABLE_NAME);
		
		// create the Increment increment1 object with the passed in rowkey
		
		
		// add the column "AnotherCount" with the anotherCountValue to the increment1 object
		// add the column "ViewCount" with the viewCountValue to the increment1 object
		
		
		// invoke the increment() method by passing in increment1 object
		
		
		// uncomment out this section when you have your written your code above
		/*
		for(KeyValue kv : result1.raw()){
			System.out.println("KV: " + kv + "Value: " + Bytes.toLong(kv.getValue()));;
		}*/
	}
}
