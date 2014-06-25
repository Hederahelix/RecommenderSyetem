package Dat2Db;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;



public class Dat2db {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		long startTime = System.currentTimeMillis();//获取当前时间
		long endTime,midTime = startTime;
		 BufferedReader reader = null;
		 		 
		 PreparedStatement pst[] = new PreparedStatement[32];
		 String[] res;
		 Double timestamp;
		 String tableName;
		 
		 int line[] = new int[32],sum=0;
		 
		 
		 try {
			 
			 Connection conn = ConnectionSource.getConnection();
			 for(int i=0;i<32;i++){
				 if(i<10)
					 tableName = "00"+i;
				 else
					 tableName = "0"+i;
				 
				 pst[i] = conn.prepareStatement("insert into trace_"+tableName+" values (null,?,?,?)");
				 line[i] = 1;
			 }
			  conn.setAutoCommit(false);   
		      int datenum = 0;  	
			  reader = new BufferedReader(new InputStreamReader(new FileInputStream("F:/10000000w.txt"),"UTF-8"));   
			  String tempString = null;
			  
			  String date;
			  
			  // 一次读入一行，直到读入null为文件结束
			  while ((tempString = reader.readLine()) != null) 
			  {
	            // 显示行号
	            
	            res = tempString.split(" ");
	            timestamp = Double.parseDouble(res[2].trim());
	            datenum = ((int)(timestamp - 1393603200)/86400+1);
	            
	            if(datenum < 10)
	            	date = "00"+datenum;
	            else
	            	date =  "0"+datenum;
	            
	            
	            pst[datenum].setInt(1, Integer.parseInt(res[0].trim())); 
	            pst[datenum].setInt(2, Integer.parseInt(res[1].trim())); 
	            pst[datenum].setDouble(3, timestamp);
	            
	            
	            sum++;
	            if(sum%100000 == 0){
	            	endTime = System.currentTimeMillis();
	            	System.out.println("line" + sum +" 插入10w  程序运行时间："+(endTime-midTime)+"ms");
					midTime = endTime;
	            }
	            	
	            pst[datenum].addBatch();//用PreparedStatement的批量处理 
	           
	           
	            for(int i=1;i<32;i++)	            
	            	if(line[i]%10000==0){
	            		pst[i].executeBatch();   
		                conn.commit();   
		                pst[i].clearBatch(); 
	            	}
	            
	            line[datenum]++;
			  }
			  
			  for(int i=1;i<32;i++){
            		pst[i].executeBatch();   
	                conn.commit();   
	                pst[i].clearBatch(); 
            	}
			  
			  reader.close();
			  ConnectionSource.closeAll(conn);  
			  
		  } catch (Exception e) {  
			  e.printStackTrace();
		  } 
		  
		
		 System.out.println("complete\n");
		 endTime = System.currentTimeMillis();
		 System.out.println("程序运行时间："+(endTime-startTime)/60000+"min");
	}

}
