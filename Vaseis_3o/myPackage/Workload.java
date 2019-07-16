package myPackage;

import java.sql.*;
import java.util.*;

public class Workload {
	
	protected static double total_time_for_U1 = 0;
	protected static double total_time_for_U2 = 0;
	protected static double total_time_for_U3 = 0;
	protected static double total_time_for_U4 = 0;
	
	protected static double total_time_for_clients=0;
	protected static double total_time_for_creditcard=0;
	
	protected static double total_time_for_Q1 = 0;
    protected static double total_time_for_Q2 = 0;
    protected static double total_time_for_Q3 = 0;
    
	
	public static void Work_load(Connection db, ZipfGenerator Zipf) throws SQLException{
		
		CallableStatement insert_U1_1 = db.prepareCall("SELECT insert_client_info(?,?)");
		CallableStatement insert_U1_2 = db.prepareCall("SELECT insert_creditcard_info(?,?,?,?,?)");
		CallableStatement insert_U2 = db.prepareCall("SELECT insert_person_info(?,?,?,?,?,?,?,?)");
		CallableStatement insert_U3 = db.prepareCall("SELECT insert_roombooking_info(?,?,?,?,?,?,?)");
		CallableStatement delete_U4 = db.prepareCall("SELECT delete_roombooking_info(?,?)");
		CallableStatement search_Q1 = db.prepareCall("SELECT search_person_for_certain_date(?,?,?,?)");
		CallableStatement search_Q2 = db.prepareCall("SELECT search_concurrently(?)");
		CallableStatement calculate_Q3 = db.prepareCall("SELECT calculate_statistics(?,?)");
		
		System.out.println("------------------| Beggining simulation test for X days |------------------");
		
		for(int X=0;X<6;X++){
			Workload.insert_data_into_client(insert_U1_1, db, Zipf); //U1 Question - Part 1
	        Workload.insert_data_into_creditcard(insert_U1_2, db, Zipf); //U1 Question - Part 2
			total_time_for_U1 = (total_time_for_clients + total_time_for_creditcard);
			System.out.println("Total time for U1 on day "+X+" is:"+total_time_for_U1);
	        Workload.insert_data_into_person(insert_U2, db, Zipf); //U2 Question
			System.out.println("Total time for U2 on day "+X+" is:"+total_time_for_U2);
	        Workload.insert_data_into_roombooking(insert_U3, db, Zipf, X); //U3 Question
			System.out.println("Total time for U3 on day "+X+" is:"+total_time_for_U3);
	        Workload.delete_data_from_roombooking(delete_U4, db, Zipf, X); //U4 Question
			System.out.println("Total time for U4 on day "+X+" is:"+total_time_for_U4);
	    }
		
		Workload.search_person_for_certain_date(search_Q1, db, Zipf);
		System.out.println("Total time for Q1 is:"+total_time_for_Q1);
		Workload.search_concurrently(search_Q2, db, Zipf);
		System.out.println("Total time for Q2 is:"+total_time_for_Q2);
		Workload.calculate_statistics(calculate_Q3, db, Zipf, 6);
		System.out.println("Total time for Q3 is:"+total_time_for_Q3);
		
		total_time_for_Q1 = 0;
	    total_time_for_Q2 = 0;
	    total_time_for_Q3 = 0;
		
		System.out.println("\n------------------| Beggining simulation test for Y days |------------------");
		
		for(int Y=0;Y<5;Y++){
			Workload.insert_data_into_client(insert_U1_1, db, Zipf); //U1 Question - Part 1
	        Workload.insert_data_into_creditcard(insert_U1_2, db, Zipf); //U1 Question - Part 2
			total_time_for_U1 = (total_time_for_clients + total_time_for_creditcard);
			System.out.println("Total time for U1 on day "+Y+6+" is:"+total_time_for_U1);
	        Workload.insert_data_into_person(insert_U2, db, Zipf); //U2 Question
			System.out.println("Total time for U2 on day "+Y+6+" is:"+total_time_for_U2);
	        Workload.insert_data_into_roombooking(insert_U3, db, Zipf, 6+Y); //U3 Question
			System.out.println("Total time for U3 on day "+Y+6+" is:"+total_time_for_U3);
	        Workload.delete_data_from_roombooking(delete_U4, db, Zipf, 6+Y); //U4 Question
			System.out.println("Total time for U4 on day "+Y+6+" is:"+total_time_for_U4);
	    }
		
		Workload.search_person_for_certain_date(search_Q1, db, Zipf);
		System.out.println("Total time for Q1 is:"+total_time_for_Q1);
		Workload.search_concurrently(search_Q2, db, Zipf);
		System.out.println("Total time for Q2 is:"+total_time_for_Q2);
		Workload.calculate_statistics(calculate_Q3, db, Zipf, 11);
		System.out.println("Total time for Q3 is:"+total_time_for_Q3);
		
		total_time_for_Q1 = 0;
	    total_time_for_Q2 = 0;
	    total_time_for_Q3 = 0;
		
        System.out.println("\n------------------| Beggining simulation test for Z days |------------------");
		
		for(int Z=0;Z<4;Z++){
			Workload.insert_data_into_client(insert_U1_1, db, Zipf); //U1 Question - Part 1
	        Workload.insert_data_into_creditcard(insert_U1_2, db, Zipf); //U1 Question - Part 2
			total_time_for_U1 = (total_time_for_clients + total_time_for_creditcard);
			System.out.println("Total time for U1 on day "+Z+11+" is:"+total_time_for_U1);
	        Workload.insert_data_into_person(insert_U2, db, Zipf); //U2 Question
			System.out.println("Total time for U2 on day "+Z+11+" is:"+total_time_for_U2);
	        Workload.insert_data_into_roombooking(insert_U3, db, Zipf, 11+Z); //U3 Question
			System.out.println("Total time for U3 on day "+Z+11+" is:"+total_time_for_U3);
	        Workload.delete_data_from_roombooking(delete_U4, db, Zipf, 11+Z); //U4 Question
			System.out.println("Total time for U4 on day "+Z+11+" is:"+total_time_for_U4);
	    }
		
		Workload.search_person_for_certain_date(search_Q1, db, Zipf);
		System.out.println("Total time for Q1 is:"+total_time_for_Q1);
		Workload.search_concurrently(search_Q2, db, Zipf);
		System.out.println("Total time for Q2 is:"+total_time_for_Q2);
		Workload.calculate_statistics(calculate_Q3, db, Zipf, 15);
		System.out.println("Total time for Q3 is:"+total_time_for_Q3);
		
		total_time_for_Q1 = 0;
	    total_time_for_Q2 = 0;
	    total_time_for_Q3 = 0;
		
	}
	
    public static void insert_data_into_client(CallableStatement insert,Connection db, ZipfGenerator Zipf) throws SQLException{
    	
    	String[] city={"Thessaloniki","Athens","Chania","Heraklion","London","Manchester","Leeds","Madrid","Barcelona","Seville"};
   		String[] country={"Hellas","England","Spain"};
   		
   		double time_keeper,startTime,endTime;
   		time_keeper=0;
   		
   		String query_cnt = "SELECT count("+"\"idClient\""+ ") from client";
   	    PreparedStatement pstmt_cnt = db.prepareStatement(query_cnt);
   	    ResultSet resultSet_cnt = pstmt_cnt.executeQuery();
   	    resultSet_cnt.next();
   	    
   	    String query_max = "SELECT max("+"\"idPerson\""+ ") from person";
	    PreparedStatement pstmt_max = db.prepareStatement(query_max);
	    ResultSet resultSet_max = pstmt_max.executeQuery();
	    resultSet_max.next();
	    
   	    int code_of_client = resultSet_max.getInt(1)+1;
   	    int number_of_clients = 100;
   	    int document=100000000+resultSet_cnt.getInt(1);
		
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
	   				startTime = System.currentTimeMillis();	
	   				insert_person.executeQuery();
	   				endTime = System.currentTimeMillis();
	   				time_keeper = time_keeper + (endTime - startTime);
					
					
					insert.setInt(1,current_client_id);
					insert.setString(2,"ID_"+ document);
					startTime = System.currentTimeMillis();	
					insert.executeQuery();
					endTime = System.currentTimeMillis();
					time_keeper = time_keeper + (endTime - startTime);
					
					code_of_client++;
					document++;
				}
				total_time_for_clients = total_time_for_clients + time_keeper;
    }

    public static void insert_data_into_creditcard(CallableStatement insert,Connection db, ZipfGenerator Zipf) throws SQLException{
	
	double n;
	double time_keeper,startTime,endTime;
	time_keeper=0;
	int clientID,number_of_cards,n1,holder;
	
 	String query_1 = "SELECT "+"\"idPerson\""+ " FROM person";
	PreparedStatement pstmt_1 = db.prepareStatement(query_1);
	ResultSet resultSet_1 = pstmt_1.executeQuery();
	
	String query_2 = "SELECT max("+"\"idClient\""+ ") FROM client";
	PreparedStatement pstmt_2 = db.prepareStatement(query_2);
	ResultSet resultSet_2 = pstmt_2.executeQuery();
	resultSet_2.next();
	int max_cli=resultSet_2.getInt(1);
	
	for(int j=0;j<max_cli-100;j++){
		resultSet_1.next();}
	
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
		startTime = System.currentTimeMillis();
		insert.executeQuery();	
		endTime = System.currentTimeMillis();
		time_keeper = time_keeper + (endTime - startTime);		
	}
	}
	else{
		insert.setString(1,"No card");
		insert.setString(2,"No card");
		insert.setDate(3,null);
		insert.setInt(4,clientID);
		insert.setString(5,fn+" "+ln);
		startTime = System.currentTimeMillis();
		insert.executeQuery();	
		endTime = System.currentTimeMillis();
		time_keeper = time_keeper + (endTime - startTime);
	}
	}
	total_time_for_creditcard = total_time_for_creditcard + time_keeper;
    }

    public static void insert_data_into_person(CallableStatement insert,Connection  db, ZipfGenerator Zipf) throws SQLException{
    	
    	String[] city={"Thessaloniki","Athens","Chania","Heraklion","London","Manchester","Leeds","Madrid","Barcelona","Seville"};
   		String[] country={"Hellas","England","Spain"};
   		
   		double time_keeper,startTime,endTime;
   		time_keeper=0;
   		
   		String query_cnt = "SELECT count("+"\"idClient\""+ ") from client";
   	    PreparedStatement pstmt_cnt = db.prepareStatement(query_cnt);
   	    ResultSet resultSet_cnt = pstmt_cnt.executeQuery();
   	    resultSet_cnt.next();
   	    
   	    String query_max = "SELECT max("+"\"idPerson\""+ ") from person";
	    PreparedStatement pstmt_max = db.prepareStatement(query_max);
	    ResultSet resultSet_max = pstmt_max.executeQuery();
	    resultSet_max.next();
   		
   	    int code_of_person = resultSet_max.getInt(1)+1;
   	    double number_people=0.05*resultSet_cnt.getInt(1);
   	    int number_of_people= (int) number_people;
		
				for(int j=0;j<number_of_people;j++){  //all clients
					int current_person_id = code_of_person;
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
	   				insert_person.setInt(1,current_person_id);
	   				insert_person.setString(2,"Name_" + num_name);
	   				insert_person.setString(3,"Surname_" + num_surname);
	   				insert_person.setInt(4,sex);
	   				insert_person.setDate(5, date_of_birth);
	   				insert_person.setString(6,"Person_Address_" + current_person_id);
	   				insert_person.setString(7,city[cityNum]);
	   				insert_person.setString(8,countryString);
	   				startTime = System.currentTimeMillis();
	   				insert_person.executeQuery();
	   				endTime = System.currentTimeMillis();
	   				time_keeper = time_keeper + (endTime - startTime);
					
					code_of_person++;
				}
				total_time_for_U2 = total_time_for_U2 + time_keeper;
		}

    public static void insert_data_into_roombooking(CallableStatement insert,Connection db, ZipfGenerator Zipf,int X) throws SQLException{
    	
    	Random rand = new Random();
    	
    	double time_keeper,startTime,endTime;
   		time_keeper=0;
    	
    	String query_cnt = "SELECT count("+"\"idClient\""+ ") from client";
   	    PreparedStatement pstmt_cnt = db.prepareStatement(query_cnt);
   	    ResultSet resultSet_cnt = pstmt_cnt.executeQuery();
   	    resultSet_cnt.next();
   	    
   	    String query_htlb = "SELECT max(idhotelbooking) from hotelbooking";
	    PreparedStatement pstmt_htlb = db.prepareStatement(query_htlb);
	    ResultSet resultSet_htlb = pstmt_htlb.executeQuery();
	    resultSet_htlb.next();
   	    
	    int idhotelbooking=resultSet_htlb.getInt(1)+1;
	    int client_count=resultSet_cnt.getInt(1);
   	    double number_book=0.2*client_count;
	    int number_of_bookings= (int) number_book;
	    int[] client_cnt=new int[number_of_bookings];
	    int flag=0;
	    int j=0;
	    
	    String query_sel = "SELECT "+"\"idClient\""+ " FROM client";
		PreparedStatement pstmt_sel = db.prepareStatement(query_sel);
		ResultSet resultSet_sel = pstmt_sel.executeQuery();
		
		int[] client_temp_table=new int[client_count];
		int p=0;
		while(resultSet_sel.next()){
			client_temp_table[p]= resultSet_sel.getInt(1);
			p++;
		}
	    while(j<number_of_bookings){  //apothikeuei to 20% ton eggegramenon pelaton
	    	flag=0;
	    	int client_num=rand.nextInt((client_count-1) - 0 + 1) + 0;
	    	for(int k=0;k<number_of_bookings;k++){
	    	    if(client_temp_table[client_num]==client_cnt[k]){
	    	    	flag=1;
	    	    }
	    	}
			
	    	if(flag==0){
	    		client_cnt[j]=client_temp_table[client_num];
	    		j++;
	    	}
	    }
	    
	    for(j=0;j<number_of_bookings;j++){   //gia olous tous pelates
	    	
	    	int hotel=Zipf.getRandInt(100)+1;  //to ksenodoxeio
			Calendar cal = Calendar.getInstance();
			cal.add(Calendar.DAY_OF_YEAR, X);
			java.sql.Date startdate= new java.sql.Date(cal.getTime().getTime());
			
			int room_cnt; //domatia
			int n=Zipf.getRandInt(10)+1;
			if (n<=2){
				room_cnt=3;
			}
			else if(n<=5){
				room_cnt=2;
			}
			else{
				room_cnt=1;
			}
			
			int resid_cnt;  //dianiktereuseis
			n=Zipf.getRandInt(100)+1;
			if (n<=5){
				resid_cnt=5;
			}
			else if(n<=15){
				resid_cnt=4;
			}
			else if(n<=30){
				resid_cnt=3;
			}
			else if(n<=60){
				resid_cnt=2;
			}
			else{
				resid_cnt=1;
			}
	    	
			CallableStatement search_checkin = db.prepareCall("SELECT search_correct_checkin(?,?,?)");
			search_checkin.setInt(1,hotel);
			search_checkin.setLong(2,room_cnt);
			search_checkin.setDate(3, startdate);
			ResultSet resultSet_checkin = search_checkin.executeQuery();
			resultSet_checkin.next();
			
			int daysForward=resultSet_checkin.getInt(1);
			Calendar checkinday= cal;
			checkinday.add(Calendar.DAY_OF_YEAR, daysForward);
			java.sql.Date checkindate= new java.sql.Date(checkinday.getTime().getTime());
			
	    	int[] chosen_rooms= new int[room_cnt];
	    	for(int d=0;d<room_cnt;d++){  //dialegei ta domatia
	    		resultSet_checkin.next();
	    		chosen_rooms[d]=resultSet_checkin.getInt(1);
	    	}
	    	
	    	double disc;  //ekptosi
			n=Zipf.getRandInt(10)+1;
			if (n<=5){
				disc=0;
			}
			else if(n<=8){
				disc=0.2;
			}
			else{
				disc=0.5;
			}
			double total_amount=0;
			double[] rates=new double[room_cnt];
			for(int s=0;s<room_cnt;s++){
				String query_rate = "SELECT get_rate_with_room("+chosen_rooms[s]+")";
		   	    PreparedStatement pstmt_rate = db.prepareStatement(query_rate);
		   	    ResultSet resultSet_rate = pstmt_rate.executeQuery();
		   	    resultSet_rate.next();
		   	    rates[s]=resultSet_rate.getDouble(1);
		   	    total_amount=total_amount+(rates[s]*resid_cnt);
			}
			
			total_amount=total_amount-(total_amount*disc);
			
	    	double payed;
	    	n=Zipf.getRandInt(10)+1;
			if (n<=4){
				payed=total_amount;
			}
			else{
				payed=total_amount*0.2;
			}
			int[] client_cnt2=new int[room_cnt];
			for(int x=0;x<room_cnt;x++){  //apothikeuei tosous pelates osa ta domatia
		    	int client_num2=rand.nextInt((client_count-1) - 0 + 1) + 0;
		    	client_cnt2[x]=client_temp_table[client_num2];
		    }
			
			Calendar calToPass = checkinday;
			calToPass.add(Calendar.DAY_OF_YEAR, resid_cnt+1);
			java.sql.Date dateToPass= new java.sql.Date(calToPass.getTime().getTime());
	    	
			    CallableStatement insert_hotelbooking = db.prepareCall("SELECT insert_hotelbooking_info(?,?,?,?,?,?,?)");
			    insert_hotelbooking.setInt(1,idhotelbooking);
			    insert_hotelbooking.setDate(2,startdate);
			    insert_hotelbooking.setDate(3,dateToPass);
			    insert_hotelbooking.setDouble(4,total_amount);
			    insert_hotelbooking.setDouble(5,payed);
			    insert_hotelbooking.setInt(6,1);
			    insert_hotelbooking.setInt(7,client_cnt[j]);
			    startTime = System.currentTimeMillis();
			    insert_hotelbooking.executeQuery();
			    endTime = System.currentTimeMillis();
   				time_keeper = time_keeper + (endTime - startTime);
			    
			
	       for(int a=0;a<room_cnt;a++){
	    	   insert.setInt(1,idhotelbooking);
			   insert.setInt(2,chosen_rooms[a]);
			   insert.setInt(3,client_cnt2[a]);
			   insert.setDate(4,checkindate);
			   insert.setDate(5,dateToPass);
			   insert.setDate(6,dateToPass);
			   insert.setDouble(7,rates[a]);
			   startTime = System.currentTimeMillis();
			   insert.executeQuery();
			   endTime = System.currentTimeMillis();
  			   time_keeper = time_keeper + (endTime - startTime);
	       }
	       idhotelbooking++;
	    }
	    total_time_for_U3 = total_time_for_U3 + time_keeper;
    }
    
    public static void delete_data_from_roombooking(CallableStatement insert,Connection db, ZipfGenerator Zipf, int X) throws SQLException{
    	
    	double time_keeper,startTime,endTime;
   		time_keeper=0;
    	
    	Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DAY_OF_YEAR, X);
		java.sql.Date startdate= new java.sql.Date(cal.getTime().getTime());
    	
		CallableStatement query_cnt = db.prepareCall("SELECT count_specific_roombookings(?)");
    	query_cnt.setDate(1, startdate);
   	    ResultSet resultSet_cnt = query_cnt.executeQuery();
   	    resultSet_cnt.next();
    	
   	    int  room_cnt_del=resultSet_cnt.getInt(1);
   	    
   	    double room_cnt_del2=room_cnt_del*0.02;
   	    int room_cnt_del3= (int) room_cnt_del2;
   	    
   	    CallableStatement query_room = db.prepareCall("SELECT get_specific_roombookings_roomid(?)");
   	    query_room.setDate(1, startdate);
	    ResultSet resultSet_room = query_room.executeQuery();
	    
	    CallableStatement query_hotelbookid = db.prepareCall("SELECT get_specific_roombookings_hotelbookid(?)");
   	    query_hotelbookid.setDate(1, startdate);
	    ResultSet resultSet_hotelbookid = query_hotelbookid.executeQuery();
	    
	    int[] room_cand=new int [room_cnt_del];
	    int[] hotelbookid_cand= new int [room_cnt_del];
	    for (int k=0;k<room_cnt_del;k++){
	    	resultSet_room.next();
	    	room_cand[k]=resultSet_room.getInt(1);
	    	resultSet_hotelbookid.next();
	    	hotelbookid_cand[k]=resultSet_hotelbookid.getInt(1);
	    }
   	    int[] chosen_rooms = new int[room_cnt_del3];
   	    int[] chosen_hotelbookid= new int [room_cnt_del3];
   	    int i=0;
   	    while(i<room_cnt_del3){
   	    	int choose=Zipf.getRandInt(room_cnt_del);
   	    	int flag=0;
   	    	for (int k=0;k<room_cnt_del3;k++){
   		    		if(chosen_rooms[k]==room_cand[choose] && chosen_hotelbookid[k]==hotelbookid_cand[choose]){
   		    		   flag=1;
   		    	}
   	    	}
   	    	if(flag!=1){
   	    		chosen_rooms[i]=room_cand[choose];
		   	    chosen_hotelbookid[i]=hotelbookid_cand[choose];
		   	    i++;
   	    	}
   	    }
   	    
   	    for(int f=0;f<room_cnt_del3;f++){
   	    	insert.setInt(1,chosen_rooms[f]);
			insert.setInt(2,chosen_hotelbookid[f]);
			startTime = System.currentTimeMillis();
			insert.executeQuery();
			endTime = System.currentTimeMillis();
			time_keeper = time_keeper + (endTime - startTime);
   	    }
   	    total_time_for_U4=total_time_for_U4+time_keeper;
    }
    
    public static void search_person_for_certain_date(CallableStatement search,Connection db, ZipfGenerator Zipf) throws SQLException{
    	
     	    String[] city={"Thessaloniki","Athens","Chania","Heraklion","London","Manchester","Leeds","Madrid","Barcelona","Seville"};
    		
     	    double time_keeper,startTime,endTime;
      		time_keeper=0;
     	    
     	    Random rand = new Random();
    	    String query_max = "SELECT max("+"\"idClient\""+ ") from client";
    	    PreparedStatement pstmt_max = db.prepareStatement(query_max);
    	    ResultSet resultSet_max = pstmt_max.executeQuery();
    	    resultSet_max.next();
    	    
    	    String query_min = "SELECT min("+"\"idClient\""+ ") from client";
    	    PreparedStatement pstmt_min = db.prepareStatement(query_min);
    	    ResultSet resultSet_min = pstmt_min.executeQuery();
    	    resultSet_min.next();
    	    
    	    int max_client=resultSet_max.getInt(1);
    	    int min_client=resultSet_min.getInt(1);
    	 
    	    String query_cnt = "SELECT count("+"\"idClient\""+ ") from client";
    	    PreparedStatement pstmt_cnt = db.prepareStatement(query_cnt);
    	    ResultSet resultSet_cnt = pstmt_cnt.executeQuery();
    	    resultSet_cnt.next();
    	 
    	    double number_client=0.2*resultSet_cnt.getInt(1);
    	    int number_of_clients= (int) number_client;
    	  
       	    
    	    for(int k=0;k<number_of_clients;k++){
    	    	int flag=0;
    	    	int client_num=0;
    	    	while(flag==0){
    	    	client_num=rand.nextInt(max_client - min_client + 1) + min_client;
    	    	String query_sel = "SELECT "+"\"idClient\""+ " FROM client WHERE "+"\"idClient\""+ "="+client_num;
    			PreparedStatement pstmt_sel = db.prepareStatement(query_sel);
    			ResultSet resultSet_sel = pstmt_sel.executeQuery();
    			resultSet_sel.next();
    			
    	    	if(!resultSet_sel.wasNull()){
    	    		flag=1;
    	    	}
    	    	}
    	    	
    	    	
    	    	int cityNum=Zipf.zipf(10);			

    				int daysplus=Zipf.getRandInt(100)+1;
    				int resid_cnt=Zipf.getRandInt(5)+1;
    				
    				Calendar checkinday= Calendar.getInstance();
    				checkinday.add(Calendar.DAY_OF_YEAR, daysplus);
    				java.sql.Date checkindate= new java.sql.Date(checkinday.getTime().getTime());
    			
    				Calendar calToPass = checkinday;
    				calToPass.add(Calendar.DAY_OF_YEAR, resid_cnt);
    				java.sql.Date dateToPass= new java.sql.Date(calToPass.getTime().getTime());
    				
    				search.setInt(1,client_num);
    				search.setString(2,city[cityNum]);
    				search.setDate(3, checkindate);
    				search.setDate(4, dateToPass);
    				startTime = System.currentTimeMillis();
    				ResultSet resultSet_search = search.executeQuery();
    				endTime = System.currentTimeMillis();
    				time_keeper = time_keeper + (endTime - startTime);
    				while(resultSet_search.next()){
    					System.out.println("Person ID: " +resultSet_search.getInt(1));
    				}
        	}
    	    total_time_for_Q1=time_keeper;
    }
    
    public static void search_concurrently(CallableStatement insert,Connection  db, ZipfGenerator Zipf) throws SQLException{
    	
    	double time_keeper,startTime,endTime;
  		time_keeper=0;
    	
    	for(int j=0;j<10;j++){   
	    String query_min = "SELECT DISTINCT reservationdate FROM hotelbooking ORDER BY reservationdate";
	    PreparedStatement pstmt_min = db.prepareStatement(query_min);
	    ResultSet resultSet_min = pstmt_min.executeQuery();
	    
	    String query_cnt = "select count(distinct reservationdate) from hotelbooking;";
	    PreparedStatement pstmt_cnt = db.prepareStatement(query_cnt);
	    ResultSet resultSet_cnt = pstmt_cnt.executeQuery();
	    resultSet_cnt.next();
    	
	    int dates_cnt=resultSet_cnt.getInt(1);
	    Calendar[] cal2=new Calendar[dates_cnt];
	    int i=0;
	    while(resultSet_min.next()){
	    cal2[i]=Calendar.getInstance();
	    cal2[i].setTime(resultSet_min.getDate(1));
	    i++;
	    }
	
	    Calendar cal1=Calendar.getInstance();
	    cal1.add(Calendar.DAY_OF_YEAR, -1);
		
		java.sql.Date anotherDate=new java.sql.Date(cal1.getTime().getTime());
		
			CallableStatement search_concurrently= db.prepareCall("SELECT search_concurrently(?)");
		
			search_concurrently.setDate(1,anotherDate);
			startTime = System.currentTimeMillis();
			ResultSet resultSet_concurrently = search_concurrently.executeQuery();
			endTime = System.currentTimeMillis();
			time_keeper = time_keeper + (endTime - startTime);
			while(resultSet_concurrently.next()){
				 System.out.println("Person ID - Person name - Person lastname - Date - Number of stays");
		         System.out.println(resultSet_concurrently.getObject(1));
		        }
    	 }
    	total_time_for_Q2=time_keeper;
    }
    
    public static void calculate_statistics(CallableStatement calculate_statistics,Connection  db, ZipfGenerator Zipf,int X) throws SQLException{
    	
    	double time_keeper,startTime,endTime;
  		time_keeper=0;	
    	
     for(int i=0;i<10;i++){
    	 
      int rtype=Zipf.getRandInt(10)+1;
      calculate_statistics.setString(1,"typename_"+rtype);
      Calendar checkinday= Calendar.getInstance();
      checkinday.add(Calendar.DAY_OF_YEAR, X);
      java.sql.Date currentdate= new java.sql.Date(checkinday.getTime().getTime());
    
     calculate_statistics.setDate(2,currentdate);
     startTime = System.currentTimeMillis();
     ResultSet resultSet_statistics = calculate_statistics.executeQuery();
     endTime = System.currentTimeMillis();
	 time_keeper = time_keeper + (endTime - startTime);
     int month = 1;
     
      while(resultSet_statistics.next()){
       
       if (month==1){
        System.out.println("January: " +resultSet_statistics.getInt(1));
       }
       else if(month==2){
        System.out.println("February: " +resultSet_statistics.getInt(1));
       }
       else if(month==3){
        System.out.println("March: " +resultSet_statistics.getInt(1));
       }
       else if(month==4){
        System.out.println("April: " +resultSet_statistics.getInt(1));
       }
       else if(month==5){
        System.out.println("May: " +resultSet_statistics.getInt(1));
       }
       else if(month==6){
        System.out.println("June: " +resultSet_statistics.getInt(1));
       }
       else if(month==7){
        System.out.println("July: " +resultSet_statistics.getInt(1));
       }
       else if(month==8){
        System.out.println("August: " +resultSet_statistics.getInt(1));
       }
       else if(month==9){
        System.out.println("September: " +resultSet_statistics.getInt(1));
       }
       else if(month==10){
        System.out.println("October: " +resultSet_statistics.getInt(1));
       }
       else if(month==11){
        System.out.println("November: " +resultSet_statistics.getInt(1));
       }
       else if(month==12){
        System.out.println("December: " +resultSet_statistics.getInt(1));
       }
        month++;
      }
     }
     total_time_for_Q3=time_keeper;
    }
    
}
