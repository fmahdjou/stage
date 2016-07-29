package hbase.Exercise3;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class HBase_FilterTester {
	
		public static void main(String[] args) throws IOException{
				
		AccessObject ao = new AccessObject();
		
		// create a row filter f1 that returns rows less than or equal to 20070920
		// create a row filter f2 that returns rows less than or equal to 20050920
		// create a row filter f3 that returns rows equal to the regular expression .*2006.
		// create a column filter f4 that returns rows less than or equal to the column q
		// create a value filter f5 that is equal to the value 136.90
				
		//ao.getInfo(f1);
		
		
				
		
		
	}
}
