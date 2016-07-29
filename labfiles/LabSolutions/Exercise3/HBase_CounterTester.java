package hbase.Exercise3;

import java.io.IOException;

public class HBase_CounterTester {
	public static void main(String[] args) throws IOException{
		
		AccessObject ao = new AccessObject();
		ao.performIncrement("20061222", 0, -5);
	}
}

