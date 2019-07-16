package myPackage;

import java.sql.*;

public class DBC {
	
	public static Connection DBConnection(String username, String password, String database) throws Exception
	  {
	        try
	        {
	            Class.forName("org.postgresql.Driver");
	        }
	        catch (Exception e)
	        {
	        	System.out.println("PostgreSQL JDBC Driver is missing.Please check it again !");
				e.printStackTrace();
				return null;
	        }
	        
	        String host = "localhost";
		    String dbName = "mydb1";
		    int port = 5432;
		    String postgresURL = "jdbc:postgresql://"+host+":"+port+"/"+dbName;
		    Connection conn;
	        try
	        {
	            conn = DriverManager.getConnection(postgresURL,username,password);
	        }
	        catch (SQLException E) {
	            java.lang.System.out.println("SQLException: " + E.getMessage());
	            java.lang.System.out.println("SQLState:     " + E.getSQLState());
	            java.lang.System.out.println("VendorError:  " + E.getErrorCode());
	            throw E;
	        }
	        
	        if (conn != null) 
			{
				System.out.println("Connection has been succesfully established :-)");
			} 
			else 
			{
				System.out.println("Connection could not be established :-(");
			}
			
			//Perform some database info prints
			DatabaseMetaData dbMetadata = conn.getMetaData();
			String productName = dbMetadata.getDatabaseProductName();
			String productVersion = dbMetadata.getDatabaseProductVersion();
			System.out.println("Database: "+ productName);
			System.out.println("Version: "+ productVersion);
			System.out.println();
			return conn;
	  }

}
