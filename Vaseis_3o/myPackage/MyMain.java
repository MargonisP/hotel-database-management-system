package myPackage;

import java.sql.*;
import tuc.ece.cs111.util.StandardInputRead;

public class MyMain {

	public static void main(String[] args) throws SQLException{
	        try{
			StandardInputRead sir = new StandardInputRead();
			String database = sir.readString("Insert database name: ");
			String user = sir.readString("Insert username : ");
			String pwd = sir.readString("Insert password : ");
			Connection db = DBC.DBConnection(user, pwd, database);//connect to the database
			
			ZipfGenerator Zipf = new ZipfGenerator();
	        //DB_Initialization.DB_Initialize(db,Zipf);
	        Workload.Work_load(db,Zipf);
	        
	       
	        }catch(Exception e){
		        e.printStackTrace();
	        }
	        
		
}
}

