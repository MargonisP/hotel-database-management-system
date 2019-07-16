package myPackage;

import java.sql.*;
import java.util.*;

public class DB_Initialization {

	private static int employeeNum=0;
	
	public static void DB_Initialize(Connection db, ZipfGenerator Zipf) throws SQLException{
		
		CallableStatement insert_1 = db.prepareCall("SELECT insert_hotel_info(?,?,?,?,?,?,?,?)");
		DB_Initialization.insert_data_into_hotel(insert_1, Zipf);
		System.out.println("Data have been succesfully inserted into table hotel!");
		
		CallableStatement insert_2 = db.prepareCall("SELECT insert_roomtype_info(?)");
		DB_Initialization.insert_data_into_roomtype(insert_2, Zipf);
		System.out.println("Data have been succesfully inserted into table roomtype!");
		
		CallableStatement insert_3 = db.prepareCall("SELECT insert_roomrate_info(?,?,?,?)");
		DB_Initialization.insert_data_into_roomrate(insert_3, db, Zipf);
		System.out.println("Data have been succesfully inserted into table roomrate!");
		
		CallableStatement insert_4 = db.prepareCall("SELECT insert_room_info(?,?,?,?,?,?)");
		DB_Initialization.insert_data_into_room(insert_4, db, Zipf);
		System.out.println("Data have been succesfully inserted into table room!");
		
		CallableStatement insert_5 = db.prepareCall("SELECT insert_travelagent_info(?,?,?)");
		DB_Initialization.insert_data_into_travelagent(insert_5, Zipf);
		System.out.println("Data have been succesfully inserted into table travelagent!");
		
		CallableStatement insert_6 = db.prepareCall("SELECT insert_employee_info(?,?,?,?)");
		DB_Initialization.insert_data_into_employee(insert_6, db, Zipf);
		System.out.println("Data have been succesfully inserted into table employee!");
		
		CallableStatement insert_7 = db.prepareCall("SELECT insert_client_info(?,?)");
		DB_Initialization.insert_data_into_client(insert_7, db, Zipf);
		System.out.println("Data have been succesfully inserted into table client!");
		
		CallableStatement insert_8 = db.prepareCall("SELECT insert_creditcard_info(?,?,?,?,?)");
		DB_Initialization.insert_data_into_creditcard(insert_8, db, Zipf);
		System.out.println("Data have been succesfully inserted into table creditcard!");
		
		CallableStatement insert_9 = db.prepareCall("SELECT insert_facility_info(?,?,?)");
		DB_Initialization.insert_data_into_facility(insert_9, db, Zipf);
		System.out.println("Data have been succesfully inserted into table facility!");
		
		CallableStatement insert_10 = db.prepareCall("SELECT insert_hotelfacilities_info(?,?,?)");
		DB_Initialization.insert_data_into_hotelfacilities(insert_10, db, Zipf);
		System.out.println("Data have been succesfully inserted into table hotelfacilities!");
		
		CallableStatement insert_11 = db.prepareCall("SELECT insert_roomfacilities_info(?,?,?)");
		DB_Initialization.insert_data_into_roomfacilities(insert_11, db, Zipf);
		System.out.println("Data have been succesfully inserted into table roomfacilities!");
		
	}
	
	
	public static void insert_data_into_hotel(CallableStatement insert, ZipfGenerator Zipf) throws SQLException{
		
		int id=1;
		String[] city={"Thessaloniki","Athens","Chania","Heraklion","London","Manchester","Leeds","Madrid","Barcelona","Seville"};
		String[] country={"Hellas","England","Spain"};
		
		for(int i=0;i<100;i++){
			
			int stars=Zipf.getRandInt(6);
			String starString=null;
			if(stars==0){
				starString="0 stars";
			}
			else if(stars==1){
				starString="1 star";
			}
			else if(stars==2){
				starString="2 stars";
			}
			else if(stars==3){
				starString="3 stars";
			}
			else if(stars==4){
				starString="4 stars";
			}
			else if(stars==5){
				starString="5 stars";
			}
			
			int cityNum=Zipf.zipf(10);
			
			String countryString=null;
			if(cityNum<4){
				countryString=country[0];
			}
			else if(cityNum<7){
				countryString=country[1];
			}
			else{
				countryString=country[2];
			}
			
			insert.setInt(1,id);
			insert.setString(2,"hotelname_"+id);
			insert.setString(3, starString);
			insert.setString(4,"hoteladdress_"+id);
			insert.setString(5,countryString);
			insert.setString(6,"hotelphone_"+id);
			insert.setString(7,"hotelfax_"+id);
			insert.setString(8,city[cityNum]);
			insert.executeQuery();
			id++;
		}
	}

    public static void insert_data_into_roomtype(CallableStatement insert, ZipfGenerator Zipf) throws SQLException{
    	
    	int type=1;
    	for(int i=0;i<10;i++){
    		insert.setString(1,"typename_"+type);
    		insert.executeQuery();
    		type++;
    	}
    }

    public static void insert_data_into_roomrate(CallableStatement insert, Connection db, ZipfGenerator Zipf) throws SQLException{
    	
    	String roomType;
    	int hotelId;
    	
    	String query_1 = "SELECT "+"\"idHotel\""+ " FROM hotel";
		PreparedStatement pstmt_1 = db.prepareStatement(query_1);
		ResultSet resultSet_1 = pstmt_1.executeQuery();
		
		while(resultSet_1.next()){ //all hotels
			
			hotelId=resultSet_1.getInt(1);
			
			String query_2 = "SELECT typename FROM roomtype";
			PreparedStatement pstmt_2 = db.prepareStatement(query_2);
			ResultSet resultSet_2 = pstmt_2.executeQuery();
		    while(resultSet_2.next()){  //all roomtypes
				
		    	roomType=resultSet_2.getString(1);
				double rate=10+Zipf.getRandInt(20)*10;
				
				double disc=Zipf.getRandInt(5)*0.1;
				disc=Math.floor(disc*100)/100;  //epeidi sto 0.3 evgaze vlakeies meta  
				
				insert.setInt(1,hotelId);
				insert.setString(2,roomType);
				insert.setDouble(3,rate);
				insert.setDouble(4,disc);
				insert.executeQuery();
			}
		}
    }
    
    public static void insert_data_into_room(CallableStatement insert,Connection db,ZipfGenerator Zipf) throws SQLException{
    	
    	String roomType;
    	int hotelID;
    	int num_floor;
    	short vacant;
    	double n;
    	int xil=0;
    	String query_1 = "SELECT "+"\"idHotel\""+ " FROM hotel";
		PreparedStatement pstmt_1 = db.prepareStatement(query_1);
		ResultSet resultSet_1 = pstmt_1.executeQuery();
		 
		while(resultSet_1.next()){  //all hotels
			num_floor=4+Zipf.getRandInt(5);  //floor number per hotel [4,8]
			hotelID=resultSet_1.getInt(1);
			xil=xil+1000;
			int curr_floor=1;
			for(int j=0;j<num_floor;j++){  //all floors
				int number=10+Zipf.getRandInt(20);  //room number per floor [10,29]
				int curr_room=1;
				for(int y=0;y<number;y++){ //all rooms
					String query_2 = "SELECT typename FROM roomtype";
					PreparedStatement pstmt_2 = db.prepareStatement(query_2);
					ResultSet resultSet_2 = pstmt_2.executeQuery();
				vacant=0;
				int idRoom=xil+(curr_floor)*100+(curr_room);
				
				int rt=1+Zipf.getRandInt(10);
				if(rt==1){
				    resultSet_2.next();
				    roomType=resultSet_2.getString(1);}
				else if(rt==10){
					for(int h=0;h<2;h++){
					resultSet_2.next();}
					roomType=resultSet_2.getString(1);}
				else {
					for(int h=0;h<=rt;h++){
					resultSet_2.next();}
					roomType=resultSet_2.getString(1);}
				
				double area=10+Zipf.getRandInt(11);
				if(area!=20){area=area+0.1*Zipf.getRandInt(10);}
				area=Math.floor(area*10)/10;
				n=Math.random();
				if (n<=0.4) {
					vacant=1;
				}
				else{			
					vacant=0;
				}
				
				insert.setInt(1,idRoom);
				insert.setInt(2,curr_room);
				insert.setShort(3,vacant);
				insert.setDouble(4,area);
				insert.setInt(5,hotelID);
				insert.setString(6,roomType);
				insert.executeQuery();
				curr_room++;
			}
				curr_floor++;
		}
    	
	}
  }
    
    public static void insert_data_into_travelagent(CallableStatement insert, ZipfGenerator Zipf) throws SQLException{
    	
    	int t_id=1;
    	for(int i=0;i<50;i++){
    		insert.setInt(1,t_id);
    		insert.setString(2, "travel_agent_name_"+t_id);
    		insert.setString(3, "travel_agent_email_"+t_id);
    		insert.executeQuery();
    		t_id++;
    	}
    }
    
    public static void insert_data_into_employee(CallableStatement insert,Connection db, ZipfGenerator Zipf) throws SQLException{
    	
   	    String[] city={"Thessaloniki","Athens","Chania","Heraklion","London","Manchester","Leeds","Madrid","Barcelona","Seville"};
   		String[] country={"Hellas","England","Spain"};
   		int idmanager;
   	    int code_of_employee = 1;
   	    String[] roleName={"receptionist","manager"};
    	int hotelID;
    	int n1,mang_cnt,mang_code=0;
    	
    	String query_1 = "SELECT "+"\"idHotel\""+ " FROM hotel";
   	    PreparedStatement pstmt_1 = db.prepareStatement(query_1);
   	    ResultSet resultSet_1 = pstmt_1.executeQuery();
   	
   		while(resultSet_1.next()){  //all hotels
   			hotelID=resultSet_1.getInt(1);
   			int number_of_employees = 10+Zipf.getRandInt(10);
   			String role=null;
   			mang_cnt=0;
   			n1=2+Zipf.getRandInt(4);   //number of managers
   			for(int j=0;j<number_of_employees;j++){  //all employees
   				int current_employee_id = code_of_employee;
   				
   				if(mang_cnt<n1){
   					if(mang_cnt==0){
   	   					mang_code=current_employee_id;
   	   				}
   				role=roleName[1];
   				mang_cnt++;
   				}
   				else{
   				role=roleName[0];
   				}
   				
   				//Meso tis zipf katanomis epilegetai to onoma, to epitheto kai to filo
   				int num_name = 1+Zipf.getRandInt(50);
   				int num_surname = 1+Zipf.getRandInt(200);
   				int sex =  Zipf.getRandInt(2); //1 for boys
   				

   				//Ipologizoume mia tixaia imerominia gennisis sto dothen euros eton
   				int year_n = 1950+Zipf.getRandInt(40);
   				int month_n =1+Zipf.getRandInt(12);
   				int dayOfMonth_n=0;
   				if (month_n==1 || month_n==3 || month_n==5 || month_n==7 || month_n==8 || month_n==10 || month_n==12){
   					 dayOfMonth_n =1+Zipf.getRandInt(31);
   				}
   				else if (month_n==2){
   					 dayOfMonth_n =1+Zipf.getRandInt(28);
   				}
   				else if  (month_n==4 || month_n==6 || month_n==9 ||month_n==11){
   				     dayOfMonth_n =1+Zipf.getRandInt(30);
   				}
   				
   				Calendar calendar = new GregorianCalendar(year_n,month_n,dayOfMonth_n);
   				java.sql.Date date_of_birth = new java.sql.Date(calendar.getTime().getTime());

   				//Epilegoume me ti zipf mia tixaia poli
   				int cityNum=Zipf.zipf(10);
   				
   				String countryString=null;
   				if(cityNum<4){
   					countryString=country[0];
   				}
   				else if(cityNum<7){
   					countryString=country[1];
   				}
   				else{
   					countryString=country[2];
   				}
   				
   				if (role==roleName[1]){
   					idmanager=0;
   				}
   				else{
   					idmanager = mang_code+Zipf.getRandInt(n1);
   				}
   				
   				CallableStatement insert_person = db.prepareCall("SELECT insert_person_info(?,?,?,?,?,?,?,?)");
   				insert_person.setInt(1,current_employee_id);
   				insert_person.setString(2,"Name_" + num_name);
   				insert_person.setString(3,"Surname_" + num_surname);
   				insert_person.setInt(4,sex);
   				insert_person.setDate(5, date_of_birth);
   				insert_person.setString(6,"Person_Address_" + current_employee_id);
   				insert_person.setString(7,city[cityNum]);
   				insert_person.setString(8,countryString);
   				insert_person.executeQuery();

   				insert.setInt(1,current_employee_id);
   				insert.setString(2,role);
   				if(idmanager==0){
   				insert.setNull(3,idmanager);}
   				else{insert.setInt(3,idmanager);}
   				insert.setInt(4,hotelID);
   				insert.executeQuery();
   				
   				code_of_employee++;
   				employeeNum=code_of_employee;
   			}
   		}
   	}
    
    public static void insert_data_into_client(CallableStatement insert,Connection db, ZipfGenerator Zipf) throws SQLException{
    	
    	String[] city={"Thessaloniki","Athens","Chania","Heraklion","London","Manchester","Leeds","Madrid","Barcelona","Seville"};
   		String[] country={"Hellas","England","Spain"};
   	    int code_of_client = DB_Initialization.getEmployeeNum();
   	    int hotelID;
   	    int number_of_clients = 100;
   	    int document=100000000;
    	
    	String query_1 = "SELECT "+"\"idHotel\""+ " FROM hotel";
   	    PreparedStatement pstmt_1 = db.prepareStatement(query_1);
   	    ResultSet resultSet_1 = pstmt_1.executeQuery();
		
			while(resultSet_1.next()){  //all hotels
				
				hotelID=resultSet_1.getInt(1);
		
				for(int j=0;j<number_of_clients;j++){  //all clients
					int current_client_id = code_of_client;
					//Meso tis zipf katanomis epilegetai to onoma, to epitheto kai to filo
	   				int num_name = 1+Zipf.getRandInt(50);
	   				int num_surname = 1+Zipf.getRandInt(200);
	   				int sex =  Zipf.getRandInt(2); //1 for boys
					

	   			    //Ipologizoume mia tixaia imerominia gennisis sto dothen euros eton
	   				int year_n = 1950+Zipf.getRandInt(40);
	   				int month_n =1+Zipf.getRandInt(12);
	   				int dayOfMonth_n=0;
	   				if (month_n==1 || month_n==3 || month_n==5 || month_n==7 || month_n==8 || month_n==10 || month_n==12){
	   					 dayOfMonth_n =1+Zipf.getRandInt(31);
	   				}
	   				else if (month_n==2){
	   					 dayOfMonth_n =1+Zipf.getRandInt(28);
	   				}
	   				else if  (month_n==4 || month_n==6 || month_n==9 ||month_n==11){
	   				     dayOfMonth_n =1+Zipf.getRandInt(30);
	   				}
	   				
	   				Calendar calendar = new GregorianCalendar(year_n,month_n,dayOfMonth_n);
	   				java.sql.Date date_of_birth = new java.sql.Date(calendar.getTime().getTime());

	   				//Epilegoume me ti zipf mia tixaia poli
	   				int cityNum=Zipf.zipf(10);
	   				
	   				String countryString=null;
	   				if(cityNum<4){
	   					countryString=country[0];
	   				}
	   				else if(cityNum<7){
	   					countryString=country[1];
	   				}
	   				else{
	   					countryString=country[2];
	   				}					

	   				CallableStatement insert_person = db.prepareCall("SELECT insert_person_info(?,?,?,?,?,?,?,?)");
	   				insert_person.setInt(1,current_client_id);
	   				insert_person.setString(2,"Name_" + num_name);
	   				insert_person.setString(3,"Surname_" + num_surname);
	   				insert_person.setInt(4,sex);
	   				insert_person.setDate(5, date_of_birth);
	   				insert_person.setString(6,"Person_Address_" + current_client_id);
	   				insert_person.setString(7,city[cityNum]);
	   				insert_person.setString(8,countryString);
	   				insert_person.executeQuery();
					
					
					insert.setInt(1,current_client_id);
					insert.setString(2,"ID_"+ document);
					insert.executeQuery();
					
					code_of_client++;
					document++;
				}
		}

    }

    public static void insert_data_into_creditcard(CallableStatement insert,Connection  db, ZipfGenerator Zipf) throws SQLException{
    	
    	double n;
    	int clientID,number_of_cards,n1,holder;
    	
     	String query_1 = "SELECT "+"\"idClient\""+ " FROM client";
		PreparedStatement pstmt_1 = db.prepareStatement(query_1);
		ResultSet resultSet_1 = pstmt_1.executeQuery();
		
		while(resultSet_1.next()){
		
			clientID=resultSet_1.getInt(1);
			
		String query_sel_fn = "SELECT fname FROM person WHERE "+"\"idPerson\""+ "="+clientID;
		PreparedStatement pstmt_sel_fn = db.prepareStatement(query_sel_fn);
		ResultSet resultSet_sel_fn = pstmt_sel_fn.executeQuery();
		resultSet_sel_fn.next();
		String fn=resultSet_sel_fn.getString(1);
		String query_sel_ln = "SELECT lname FROM person WHERE "+"\"idPerson\""+ "="+clientID;
		PreparedStatement pstmt_sel_ln = db.prepareStatement(query_sel_ln);
		ResultSet resultSet_sel_ln = pstmt_sel_ln.executeQuery();
		resultSet_sel_ln.next();
		String ln=resultSet_sel_ln.getString(1);
		 
    	n=Math.random();
		if (n<=0.8) {
			holder=1;
		}
		else{			
			holder=0;
		}
		
		if (holder==1){
			number_of_cards=1+Zipf.getRandInt(5);
		}
		else{
			number_of_cards=0;
		}
		
		if(number_of_cards!=0){
		for(int j=0;j<number_of_cards;j++){
			n1=1+Zipf.getRandInt(5);
			String tetr0=Zipf.getRandInt(10) + "" + Zipf.getRandInt(10) + "" + Zipf.getRandInt(10)+ "" + Zipf.getRandInt(10);
			String tetr1=Zipf.getRandInt(10) + "" + Zipf.getRandInt(10) + "" + Zipf.getRandInt(10)+ "" + Zipf.getRandInt(10);
			String tetr2=Zipf.getRandInt(10) + "" + Zipf.getRandInt(10) + "" + Zipf.getRandInt(10)+ "" + Zipf.getRandInt(10);
			String tetr3=Zipf.getRandInt(10) + "" + Zipf.getRandInt(10) + "" + Zipf.getRandInt(10)+ "" + Zipf.getRandInt(10);
			
			int year_n = 2015+Zipf.getRandInt(3);
			int month_n = 1+Zipf.getRandInt(12); 
			int dayOfMonth_n=28;
			
			Calendar calendar = new GregorianCalendar(year_n,month_n,dayOfMonth_n);
		    java.sql.Date expiration = new java.sql.Date(calendar.getTime().getTime());
			
			
			insert.setString(1,"cardtype_"+n1);
			insert.setString(2,tetr0+"-"+tetr1+"-"+tetr2+"-"+tetr3);
			insert.setDate(3,expiration);
			insert.setInt(4,clientID);
			insert.setString(5,fn+" "+ln);
			insert.executeQuery();	
		}
		}
		else{
			insert.setString(1,"No card");
			insert.setString(2,"No card");
			insert.setDate(3,null);
			insert.setInt(4,clientID);
			insert.setString(5,fn+" "+ln);
			insert.executeQuery();
		}
		}
    }
    
    public static void insert_data_into_facility(CallableStatement insert,Connection  db, ZipfGenerator Zipf) throws SQLException{
    	
    	//HOTEL FACILITIES
    	//Metritis plithous sinolikon deiukolinseon
		int counter = 0;

		//Epipedo ierarxias dieukolinseon 1
		for(int i = 1; i <= 2; i++){
			if(counter < 100){
		        //System.out.println("hotel_facility_"+i);
				insert.setString(1,"hotel_facility_"+i);
				insert.setString(2,null);
				insert.setString(3,null);
				insert.executeQuery();
				counter++;
			}
			
			//Epipedo ierarxias dieukolinseon 2
			for(int j = 1; j <= 3; j++){
				if(counter < 100){
					//System.out.println("hotel_facility_"+i+"_"+j);
					insert.setString(1,"hotel_facility_"+i+"_"+j);
					insert.setString(2,"hotel_facility_"+i);
					insert.setString(3,null);
					insert.executeQuery();
					counter++;
				}

				//Epipedo ierarxias dieukolinseon 3
				for(int k = 1; k <= 4; k++){
					if(counter < 100){
						//System.out.println("hotel_facility_"+i+"_"+j+"_"+k);
						insert.setString(1,"hotel_facility_"+i+"_"+j+"_"+k);
						insert.setString(2,"hotel_facility_"+i+"_"+j);
						insert.setString(3,null);
						insert.executeQuery();
						counter++;
					}

					//Epipedo ierarxias dieukolinseon 4
					for(int l = 1; l <= 4; l++){
						if(counter < 100){		
							//System.out.println("hotel_facility_"+i+"_"+j+"_"+k+"_"+l);
							insert.setString(1,"hotel_facility_"+i+"_"+j+"_"+k+"_"+l);
							insert.setString(2,"hotel_facility_"+i+"_"+j+"_"+k);
							insert.setString(3,null);
							insert.executeQuery();
							counter++;
						}
					}
				}
			}
		}
		
		//ROOM FACILITIES
		//Metritis plithous sinolikon deiukolinseon
		counter = 0;

		//Epipedo ierarxias dieukolinseon 1
		for(int i = 1; i <= 2; i++){
			if(counter < 60){
		        //System.out.println("hotel_facility_"+i);
				insert.setString(1,"room_facility_"+i);
				insert.setString(2,null);
				insert.setString(3,null);
				insert.executeQuery();
				counter++;
			}
			
			//Epipedo ierarxias dieukolinseon 2
			for(int j = 1; j <= 3; j++){
				if(counter < 60){
					//System.out.println("hotel_facility_"+i+"_"+j);
					insert.setString(1,"room_facility_"+i+"_"+j);
					insert.setString(2,"room_facility_"+i);
					insert.setString(3,null);
					insert.executeQuery();
					counter++;
				}

				//Epipedo ierarxias dieukolinseon 3
				for(int k = 1; k <= 4; k++){
					if(counter < 60){
						//System.out.println("hotel_facility_"+i+"_"+j+"_"+k);
						insert.setString(1,"room_facility_"+i+"_"+j+"_"+k);
						insert.setString(2,"room_facility_"+i+"_"+j);
						insert.setString(3,null);
						insert.executeQuery();
						counter++;
					}

					//Epipedo ierarxias dieukolinseon 4
					for(int l = 1; l <= 4; l++){
						if(counter < 60){		
							//System.out.println("hotel_facility_"+i+"_"+j+"_"+k+"_"+l);
							insert.setString(1,"room_facility_"+i+"_"+j+"_"+k+"_"+l);
							insert.setString(2,"room_facility_"+i+"_"+j+"_"+k);
							insert.setString(3,null);
							insert.executeQuery();
							counter++;
						}
					}
				}
			}
		}
    	
}
    
    public static void insert_data_into_hotelfacilities(CallableStatement insert,Connection  db, ZipfGenerator Zipf) throws SQLException{
    
    	int hotelID;
    	boolean temp=false;
    	int facil_total=0;
    	int facil_cnt=0;
    	String query_1 = "SELECT "+"\"idHotel\""+ " FROM hotel";
		PreparedStatement pstmt_1 = db.prepareStatement(query_1);
		ResultSet resultSet_1 = pstmt_1.executeQuery();
		
		while(resultSet_1.next()){  //all hotels
			facil_total=facil_total+facil_cnt;
			hotelID=resultSet_1.getInt(1);
			facil_cnt=10+Zipf.getRandInt(40);
			
    	    for(int i=0;i<facil_cnt;i++){  //all facilities
    	    	String hotel_facil;
    	        do{
    	    	int facil_num=1+Zipf.getRandInt(100);
    	    	String query_2 = "SELECT "+"\"nameFacility\""+ " FROM facility";
    			PreparedStatement pstmt_2 = db.prepareStatement(query_2);
    			ResultSet resultSet_2 = pstmt_2.executeQuery();
    			for(int j=0;j<facil_num;j++){
    			resultSet_2.next();}
    			hotel_facil=resultSet_2.getString(1);
    			
    			String query_3 = "SELECT "+"\"nameFacility\""+ " FROM hotelfacilities";
    			PreparedStatement pstmt_3 = db.prepareStatement(query_3);
    			ResultSet resultSet_3 = pstmt_3.executeQuery();
    			temp=false;
    			for(int k=0;k<facil_total;k++){resultSet_3.next();}
    			while(resultSet_3.next() && temp==false){
    				if(hotel_facil.equals(resultSet_3.getString(1))){
    					temp=true;
    				}
    				else{temp=false;}
    			}
    	        }while(temp);
    	        
    	        insert.setInt(1,hotelID);
				insert.setString(2,hotel_facil);
				insert.setString(3,null);
				insert.executeQuery();
    	    }
		}
    }
    
    public static void insert_data_into_roomfacilities(CallableStatement insert,Connection  db, ZipfGenerator Zipf) throws SQLException{
    
    	int roomID;
    	boolean temp=false;
    	int facil_total=0;
    	int facil_cnt=0;
    	String query_1 = "SELECT "+"\"idRoom\""+ " FROM room";
		PreparedStatement pstmt_1 = db.prepareStatement(query_1);
		ResultSet resultSet_1 = pstmt_1.executeQuery();
		
		while(resultSet_1.next()){  //all hotels
			facil_total=facil_total+facil_cnt;
			roomID=resultSet_1.getInt(1);
			facil_cnt=Zipf.getRandInt(6);
			
    	    for(int i=0;i<facil_cnt;i++){  //all facilities
    	    	String room_facil;
    	        do{
    	    	int facil_num=1+Zipf.getRandInt(60);
    	    	String query_2 = "SELECT "+"\"nameFacility\""+ " FROM facility";
    			PreparedStatement pstmt_2 = db.prepareStatement(query_2);
    			ResultSet resultSet_2 = pstmt_2.executeQuery();
    			for(int l=0;l<100;l++){resultSet_2.next();}
    			for(int j=0;j<facil_num;j++){
    			resultSet_2.next();}
    			room_facil=resultSet_2.getString(1);
    			
    			String query_3 = "SELECT "+"\"nameFacility\""+ " FROM roomfacilities";
    			PreparedStatement pstmt_3 = db.prepareStatement(query_3);
    			ResultSet resultSet_3 = pstmt_3.executeQuery();
    			temp=false;
    			for(int k=0;k<facil_total;k++){resultSet_3.next();}
    			while(resultSet_3.next() && temp==false){
    				if(room_facil.equals(resultSet_3.getString(1))){
    					temp=true;
    				}
    				else{temp=false;}
    			}
    	        }while(temp);
    	        
    	        insert.setInt(1,roomID);
				insert.setString(2,room_facil);
				insert.setString(3,null);
				insert.executeQuery();
    	    }
		}
    }
    
    
    public static int getEmployeeNum(){return employeeNum;}

}
