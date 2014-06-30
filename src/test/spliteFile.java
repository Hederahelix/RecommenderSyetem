package test;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import Dat2Db.ConnectionSource;

public class spliteFile {
	
	public void spliteByTime() throws IOException{
		int sum=0,datenum = 0;	
		String[] res;
		Double timestamp;
		String tempString = null; 		
		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("D:/data/task_res_caixingwang.txt"),"UTF-8"));
        BufferedWriter[] bw = new BufferedWriter[32];
        
        for(int i=1;i<32;i++)
        {
        	bw[i] = new BufferedWriter(new FileWriter("D:/data/splitefilesbytime/file"+i+".txt"));       	
        }
        
		while ((tempString = reader.readLine()) != null) {
			sum++;
			res = tempString.split(" ");
			timestamp = Double.parseDouble(res[2].trim());
            datenum = ((int)(timestamp - 1393603200)/86400+1);
			bw[datenum].write(tempString);
			bw[datenum].newLine();//换行
			
			if(sum % 1000000 == 0)
			{
				System.out.println("100w");
			}

		}
		
		for(int i=1;i<32;i++)
        {
			bw[i].flush();
        	bw[i].close();  
        }
		reader.close();
		
		System.out.println(sum);
	}
	
	public static void splite(String filename,int num) throws IOException{
		int sum=1;
		String tempString = null;
		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("D:/data/task_res_caixingwang.txt"),"UTF-8"));
		FileWriter writer = new FileWriter(filename);
        BufferedWriter bw = new BufferedWriter(writer);
        
		while ((tempString = reader.readLine()) != null) {

			bw.write(tempString);
			bw.newLine();//换行
			sum++;
			if(sum % num == 0)
			{
				break;
			}
		}
		
		bw.flush();
		bw.close();
		reader.close();
	}
	
	
	
	public void spliteByLine(int linenum) throws IOException{
		int sum=0,num=1;
		String tempString = null;
		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("D:/data/task_res_caixingwang.txt"),"UTF-8"));
		FileWriter writer = null;
        BufferedWriter bw = null;
        
		while ((tempString = reader.readLine()) != null) {
			
			if(sum % linenum == 0)
			{
				if(bw != null)
				{
					bw.flush();
					bw.close();
					System.out.println("file"+num+" complete");
				}
				
				writer = new FileWriter("D:/data/splitefilesbyuid/file"+num+".txt");
				bw = new BufferedWriter(writer);
				num++;
			}
			
			
			bw.write(tempString);
			bw.newLine();//换行
			
			sum++;

		}
		bw.flush();
		bw.close();
		reader.close();
	}
	
	public static void splite4fengniao() throws IOException, SQLException{
		int sum = 0,line = 0;
		String tempString = null; 		
		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("D:/data/Cfengniao.txt"),"UTF-8"));
        
        //.*?"title":"([^"]*)".*?"contents":"([^"]*)".*?"iid":"([^"]*)"
        String Regex=".*?\"title\":\"([^\"]*)\".*?\"contents\":\"([^\"]*)\".*?\"iid\":\"([^\"]*)\"";
		Pattern p=Pattern.compile(Regex);
		Matcher matcher;

		
		Connection conn = ConnectionSource.getConnection();
		conn.setAutoCommit(false);
		PreparedStatement pst = conn.prepareStatement("insert into news values (null,?,?,?); ");
		
		while ((tempString = reader.readLine()) != null) {
			
			line++;
			matcher = p.matcher(tempString);
            if (matcher.find()) 
            {
        		//titile contents iid
            	pst.setString(1, matcher.group(1).trim()); 
            	pst.setString(2, matcher.group(2).trim()); 
            	pst.setString(3, matcher.group(3).trim()); 
            	pst.addBatch();
            	
    			sum++;
            }else
            	System.out.println("error :"+line);
             
			
			
			
			if(sum % 5000 == 0)
			{
				pst.executeBatch();   
                conn.commit();   
                pst.clearBatch();
                
			}
			
			if(sum % 10000 == 0){
				System.out.println("complete :"+sum);
			}

		}
		
		pst.executeBatch();   
        conn.commit();   
        pst.clearBatch();
        
		
		reader.close();
		
		System.out.println(sum+" "+line);
	} 
	
	
	
	public static void main(String[] args){
		
		long startTime = System.currentTimeMillis();//获取当前时间
		try {
			splite4fengniao();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		long endTime = System.currentTimeMillis();
		System.out.println("程序运行时间："+(endTime-startTime)/60000+"min");
	}
	
}