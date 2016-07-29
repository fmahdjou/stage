package hbase.Exercise2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;


public class AccessObject {

	// declaring the table and column family CONSTANTS
	public static final byte[] TABLE_NAME = 
		Bytes.toBytes("users");
	public static final byte[] INFO_FAMILY =
		Bytes.toBytes("info");
	
	
	// declaring the columns CONSTANTS
	private static final byte[] USER_COL =
		Bytes.toBytes("user");
	private static final byte[] NAME_COL =
		Bytes.toBytes("name");
	private static final byte[] EMAIL_COL =
		Bytes.toBytes("email");
	
	private Configuration conf = HBaseConfiguration.create();
	private HConnection connection;
		
	public AccessObject() throws IOException{
		this.connection = HConnectionManager.createConnection(conf);
	}
	
	public static Get mkGet(String user){
		Get g = new Get(Bytes.toBytes(user));
		g.addFamily(INFO_FAMILY);
		return g;
	}
	
	public Put mkPut(User u){
		Put p = new Put(Bytes.toBytes(u.user)); 
		p.add(INFO_FAMILY, USER_COL, Bytes.toBytes(u.user));
		p.add(INFO_FAMILY, NAME_COL, Bytes.toBytes(u.name));
		p.add(INFO_FAMILY, EMAIL_COL, Bytes.toBytes(u.email));
		return p;
	}
	
	public static Delete mkDelete(String user){
		Delete d = new Delete (Bytes.toBytes(user));
		return d;
	}
	
	public static Scan mkScan(){
		Scan s = new Scan();
		s.addFamily(INFO_FAMILY);
		return s;
	}
	
	public User getUser(String user) throws IOException{
		HTable users = new HTable(conf, TABLE_NAME);		
		Get g = mkGet(user);
		Result result = users.get(g);
		if(result.isEmpty()){
			return new User(result);
		}
		User u = new User(result);
		users.close();
		return u;
		
	}
	
	public void addUser(String user, String name, String email) throws IOException{
		HTableInterface users = connection.getTable(TABLE_NAME);
		Put p = mkPut(new User(user,name,email));	
		users.put(p);
		users.close();
	}
		
	
	public List<User> getUsers() throws IOException{
		HTable users = new HTable(conf, TABLE_NAME);
		ResultScanner results = users.getScanner(mkScan());
		ArrayList<User> al = new ArrayList<User>();
		for(Result r : results){
			al.add(new User(r));
		}
		return al;
	}
	
	public void deleteUser(String user) throws IOException{
		HTable users = new HTable(conf, TABLE_NAME);
		
		Delete d = mkDelete(user);
		users.delete(d);
		users.close();
	}
	
	
	static class User extends hbase.Exercise2.User{
				
		// taking the returned object Result and creating a user
		private User (Result r){
			this(r.getValue(INFO_FAMILY, USER_COL),
				r.getValue(INFO_FAMILY, NAME_COL),
				r.getValue(INFO_FAMILY, EMAIL_COL));
		}
		
		private User(byte[] user, byte[] name, byte[] email){
			this(Bytes.toString(user),
					Bytes.toString(name),
					Bytes.toString(email));
		}
		
		private User(String user, String name, String email){
			this.user = user;
			this.name = name;
			this.email = email;
		}
	}
}
