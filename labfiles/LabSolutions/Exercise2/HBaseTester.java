package hbase.Exercise2;

import java.io.IOException;

public class HBaseTester {
	public static final String usage = 
		"add user name email - add a new user\n"+
		"get user - retrieve the user\n"+
		"delete user - remove the user\n"+
		"list - list all users\n";
	
		public static void main(String[] args) throws IOException{
		if(args.length == 0){
			System.out.println(usage);
			System.exit(0);
		}
		
		AccessObject ao = new AccessObject();
				
		if("get".equals(args[0])){
			System.out.println("Getting user " + args[1]);
			User u = ao.getUser(args[1]);
			System.out.println(u);
		}
		if("add".equals(args[0])){
			System.out.println("Adding user...");
			ao.addUser(args[1],args[2],args[3]);
			User u = ao.getUser(args[1]);
			System.out.println("Successfully added user " + u);
		}
		
		if("delete".equals(args[0])){
			System.out.println("Deleting user...");
			ao.deleteUser(args[1]);
			System.out.println("Successfully deleted user");
		}
		
		if("list".equals(args[0])){
			for(User u : ao.getUsers()){
				System.out.println(u);
			}
		}
	}
}
