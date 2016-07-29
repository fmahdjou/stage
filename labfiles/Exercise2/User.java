package hbase.Exercise2;

public abstract class User {
	public String user;
	public String name;
	public String email;
	
	public String toString() {
		return String.format("<User: %s, %s, %s>", user, name, email);
	}

}
