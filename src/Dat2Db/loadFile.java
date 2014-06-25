package Dat2Db;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;

public class loadFile {
	
	

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		long startTime = System.currentTimeMillis();//获取当前时间
		long endTime,midTime = startTime;
		String tableName;
		 
		 /*
		  * LOAD DATA LOCAL INFILE 'data.txt' INTO TABLE tbl_name 
			FIELDS TERMINATED BY ',' 
			OPTIONALLY ENCLOSED BY '"' 
			LINES TERMINATED BY '\n'
		  * */
		 
		 try {
			 
			 Connection conn = ConnectionSource.getConnection();
			 Statement stmt = conn.createStatement();
			 for(int i=1;i<10;i++){
				 if(i<10)
					 tableName = "00"+i;
				 else
					 tableName = "0"+i;
				 
				 endTime = System.currentTimeMillis();
				 System.out.println("LOAD DATA LOCAL INFILE 'D:/data/splitefiles/file"+i+".txt' INTO TABLE trace_"+tableName+" FIELDS TERMINATED BY ' ' OPTIONALLY ENCLOSED BY '' LINES TERMINATED BY '\r\n' (uid,iid,time);");
				 stmt.execute("LOAD DATA LOCAL INFILE 'D:/data/splitefiles/file"+i+".txt' INTO TABLE trace_"+tableName+" FIELDS TERMINATED BY ' ' OPTIONALLY ENCLOSED BY '' LINES TERMINATED BY '\r\n' (uid,iid,time);");
				
				 System.out.println("LOAD FILE "+i+" 程序运行时间："+(endTime-midTime)+"ms");
				 midTime = endTime;
			 }
			  

			 ConnectionSource.closeAll(stmt,conn);  
			  
		  } catch (Exception e) {  
			  e.printStackTrace();
		  } 
		  
		
		 System.out.println("complete\n");
		 endTime = System.currentTimeMillis();
		 System.out.println("程序运行时间："+(endTime-startTime)/60000+"min");
	}

}
