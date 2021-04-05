package hsqlassingment;


import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;

public class fileData implements fileUtils {
Connection con=null;	
public void connectDatabase() throws ClassNotFoundException, SQLException {
	//Connection con=null;
	Class.forName("org.hsqldb.jdbc.JDBCDriver");
	con=DriverManager.getConnection("jdbc:hsqldb:mem://localhost/testdb", "SA", "");
}

boolean flag = false;

	public void inserData() throws SQLException{
		try {	
			connectDatabase();
			this.connectDatabase();
			

			if (flag == false)
			{		
			String sql1 = "create table nonConfidential1 (ID int , Isconfidential varchar(40) ,"
					+ " ProjectName  varchar(100),Street varchar(40),City  varchar(40)  ,"
					+ " State  varchar(40) , Zipcode  varchar(40),Country  varchar(40),"
					+ "LEEDSystemVersionDisplayName  varchar(40) , PointsAchieved  varchar(40) ,"
					+ " CertLevel  varchar(40) , CertDate  varchar(40) , IsCertified  varchar(40) ,"
					+ " OwnerTypes varchar(40) , GrossSqFoot  float, TotalPropArea  varchar(40) ,"
					+ " ProjectTypes  varchar(100) , OwnerOrganization  varchar(40) , RegistrationDate  varchar(40))";
			
			 con.createStatement().execute(sql1);
			 
			 flag = true;
			
			}
			
			  String sql = "INSERT INTO nonConfidential1(ID,Isconfidential,ProjectName,Street,City,State,Zipcode,Country,"
			  		+ "LEEDSystemVersionDisplayName,PointsAchieved,CertLevel,CertDate,IsCertified,"
			  		+ "OwnerTypes,GrossSqFoot,TotalPropArea,ProjectTypes,OwnerOrganization,RegistrationDate)"
			  		+ " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			  

	            PreparedStatement statement = con.prepareStatement(sql);
	            FileReader filereader=new FileReader("C:/Users/Sumit/Downloads/java_assignment/java_assignment/demo/target/demo.csv");
	           
	            CSVReader lineReader=new CSVReader(filereader);
	            String[] lineText = null;
	 
	            lineReader.readNext();  //skip header line
	            while ((lineText = lineReader.readNext()) != null)
	            {
	              
	            	String [] data=lineText;
	                int id  = Integer.parseInt(data[0].trim());
	                String isconf = data[1];
	                String project_name = data[2];
	                String street = data[3];
	                String city  = data[4];
	                String state = data[5];
	                String zipcode = data[6];
	                String country = data[7];
	                String leeds_system_display_name  = data[8];
	                String achived_points = data[9];
	                String cert_level = data[10];
	                String cert_date = data[11];
	                String is_certified  = data[12];
	                String owner_type = data[13];
	                float gross_sqfoot = Float.parseFloat(data[14].trim());
	                String project_type = data[15];
	                String total_area = data[16];
	                String owner_organization  = data[17];
	                String reg_date = data[18];
	 
	                statement.setLong(1, id);
	                statement.setString(2, isconf);
	                
	                statement.setString(3, project_name);
	                statement.setString(4, street);
	                
	                statement.setString(5, city);
	                statement.setString(6, state);
	                
	                statement.setString(7, zipcode);
	                statement.setString(8, country);
	                
	                statement.setString(9, leeds_system_display_name);
	                statement.setString(10, achived_points);
	 
	                statement.setString(11, cert_level);
	                statement.setString(12, cert_date);
	                
	                statement.setString(13, is_certified);
	                statement.setString(14, owner_type);
	                
	                statement.setFloat(15, gross_sqfoot);
	                statement.setString(16, project_type);
	                
	                statement.setString(17, total_area);
	                statement.setString(18, owner_organization);
	                
	                statement.setString(19, reg_date);
	                
	                statement.addBatch();
	               
	                int[] count=statement.executeBatch();
	            }
	            
	 
	            lineReader.close();
	           
	            statement.close();

		}
		catch(Exception e){
			e.printStackTrace(System.out);
		}
		
	     con.commit();
		con.close();


	}

	public void retriveData() throws ClassNotFoundException, SQLException, IOException {
		CSVWriter writer_q1=new CSVWriter(new FileWriter("C:/Users/sumit/Desktop/ans/q1.csv"));
		CSVWriter writer_q2=new CSVWriter(new FileWriter("C:/Users/sumit/Desktop/ans/q2.csv"));
		CSVWriter writer_q3=new CSVWriter(new FileWriter("C:/Users/sumit/Desktop/ans/q3.csv"));
		CSVWriter writer_q4=new CSVWriter(new FileWriter("C:/Users/sumit/Desktop/ans/q4.csv"));
		//******************Question 1**********************
		connectDatabase();
		String query = "select count(*) from nonConfidential1 where State='VA' ";
	      //Executing the query
	      Statement stmt = con.createStatement();
	      ResultSet rs = stmt.executeQuery(query);
	      //Retrieving the result
	      rs.next();
	      String count = rs.getString(1);
	      String [] data= {count};
  	      writer_q1.writeNext(data);
  	      writer_q1.flush();

	      //*****************Question 2*********************
	      connectDatabase();
	      String query1 = "select OwnerTypes,count(*) from nonConfidential1 where State='VA' group by OwnerTypes  ";
  	      //Executing the query
  	      Statement stmt1 = con.createStatement();
  	      ResultSet rs1 = stmt1.executeQuery(query1);
  	      //Retrieving the result
  	      while(rs1.next())
  	      {
  	      String owner_types=rs1.getString(1);
  	      String count_owner_type = rs1.getString(2);
  	      String [] count_of_ownertypes_data= {owner_types,count_owner_type};
	      writer_q2.writeNext(count_of_ownertypes_data);
	      writer_q2.flush();
  	      }
  	      
  	      //********************Question 3*******************
  	      connectDatabase();
  	    String query2 = "select sum(GrossSqFoot) from nonConfidential1 where State='VA' and IsCertified='Yes'";
	      //Executing the query
	      Statement stmt2 = con.createStatement();
	      ResultSet rs2 = stmt2.executeQuery(query2);
	      //Retrieving the result
	      int sum=0;
	      while(rs2.next())
	      {
	      int sum_gross_sqft = rs2.getInt(1);
	      sum=sum+sum_gross_sqft;
	      }
	      String sum_sqft=Integer.toString(sum);
	      String [] sum_of_sqft= {sum_sqft};
  	      writer_q3.writeNext(sum_of_sqft);
  	      writer_q3.flush();

	      //***************Question 4*************************
	      connectDatabase();
	      String query3 = "select Zipcode,count(*) from nonConfidential1 where State='VA' group by Zipcode";
  	      //Executing the query
  	      Statement stmt3 = con.createStatement();
  	      ResultSet rs3 = stmt3.executeQuery(query3);
  	      //Retrieving the result
  	      while(rs3.next())
  	      {
  	    	String zipcode = rs3.getString(1);
	  	      String zipcode_count=rs3.getString(2);
	  	      String [] count_zip= {zipcode,zipcode_count};
	  	      writer_q4.writeNext(count_zip);
	  	      writer_q4.flush();
  	      }

	}

	

}
