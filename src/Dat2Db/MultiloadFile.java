package Dat2Db;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import Dat2Db.MultiDat2db.task;

public class MultiloadFile {
	
	class task implements Callable<Integer>{
		private int id;
		private String filename;
		private String tableName;
		private String strPath = "D:\\error_MultiDat2db.txt";
		
		public task(int id,String filename,String tableName){
			this.filename = filename;
			this.id = id;
			this.tableName = tableName;
			//System.out.println("----------------------id="+id+" start="+start+" end="+end+"----------------------");
		}
		
		public Integer call(){
			
			long startTime = System.currentTimeMillis();//获取当前时间
		      //Long operations	 
			Connection conn;
			Statement stmt;
			try {
				conn = ConnectionSource.getConnection();
				stmt = conn.createStatement();
				System.out.println("LOAD DATA LOCAL INFILE '"+filename+"' INTO TABLE trace_"+tableName+" FIELDS TERMINATED BY ' ' OPTIONALLY ENCLOSED BY '' LINES TERMINATED BY '\r\n' (uid,iid,time);");
				stmt.execute("LOAD DATA LOCAL INFILE '"+filename+"' INTO TABLE trace_"+tableName+" FIELDS TERMINATED BY ' ' OPTIONALLY ENCLOSED BY '' LINES TERMINATED BY '\r\n' (uid,iid,time);");
				ConnectionSource.closeAll(stmt,conn);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				try{
					File file = new File(strPath);
					if (!file.exists()) {
					   file.createNewFile();
					 }				  
				   FileWriter fileWriter = new FileWriter(strPath,true);


				   String nextLine = System.getProperty("line.separator");
				  
				   fileWriter.write(e+nextLine);
				   fileWriter.flush();
				   fileWriter.close();
				}catch(Exception e1){
					
				}
			}
			
			
			long endTime = System.currentTimeMillis();
			System.out.println("id="+id+"  程序运行时间："+(endTime-startTime)+"ms");
			return 1;
			    
		 }
	}
	
	public void prepare() throws IOException{
		long startTime = System.currentTimeMillis();//获取当前时间
		int sum=0,datenum = 0;	
		String[] res;
		Double timestamp;
		String tempString = null; 		
		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("D:/data/task_res_caixingwang.txt"),"UTF-8"));
        BufferedWriter[] bw = new BufferedWriter[32];
        
        for(int i=1;i<32;i++)
        {
        	bw[i] = new BufferedWriter(new FileWriter("D:/data/splitefiles/file"+i+".txt"));       	
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
        	bw[i].close();      	
        }
		reader.close();
		
		System.out.println(sum);
		long endTime = System.currentTimeMillis();
		System.out.println("prepare 程序运行时间："+(endTime-startTime)/60000+"min");
	}

	public void start() throws InterruptedException{
		int THREADNUM =15;
	
		ExecutorService threadPool = Executors.newFixedThreadPool(THREADNUM);  
		CompletionService<Integer> pool = new ExecutorCompletionService<Integer>(threadPool);
		String tableName;
		
		for(int i=1;i<32;i++){
			if(i<10)
				 tableName = "00"+i;
			 else
				 tableName = "0"+i;
		
			pool.submit(new task(i,"D:/data/file"+i+".txt","trace_"+tableName));
		}
		threadPool.shutdown();
		
		//System.out.println("----------------------"+sum+"----------------------");
		while(!threadPool.awaitTermination(2, TimeUnit.SECONDS)){
			System.out.println("WAIT PHASE2");
		}
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		long startTime = System.currentTimeMillis();//获取当前时间
		MultiloadFile mf = new MultiloadFile();

		try {
			mf.prepare();
			mf.start();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		long endTime = System.currentTimeMillis();
		System.out.println("程序运行时间："+(endTime-startTime)/60000+"min");
	}

}
