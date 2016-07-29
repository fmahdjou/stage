package hbase.Exercise3;

import java.io.IOException;

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
		
		Filter f1 = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes("20070920")));
		Filter f2 = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes("20050920")));
		Filter f3 = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(".*2006."));
		Filter f4 = new QualifierFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes("q")));
		Filter f5 = new ValueFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("136.90")));
		ao.getInfo(f5);
		
		
				
		
		
	}
}
