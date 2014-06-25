package Dat2Db;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Date;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.sound.sampled.Line;
import javax.xml.stream.events.StartDocument;



public class MultiDat2db {
	
	class task implements Callable<Integer>{
		
		private Record rec[];
		private int id;
		private int start;
		private int end;
		private String strPath = "D:\\error_MultiDat2db.txt";
		
		public task(Record rec[],int id,int start,int end){
			this.rec = rec;
			this.id = id;
			this.start = start;
			this.end = end;
			//System.out.println("----------------------id="+id+" start="+start+" end="+end+"----------------------");
		}
		
		public Integer call(){
			
			long startTime = System.currentTimeMillis();//获取当前时间
		      //Long operations	 
			PreparedStatement pst[] = new PreparedStatement[32];
			int line[] = new int[32];
			String tableName;
			Connection conn = null;
			int tmp = 0;
			int datenum = 0;  
			try {
				 
				 conn = ConnectionSource.getConnection();
				 for(int i=1;i<32;i++)
				 {
					 if(i<10)
						 tableName = "00"+i;
					 else
						 tableName = "0"+i;
					 
				     pst[i] = conn.prepareStatement("insert into trace_"+tableName+" values (null,?,?,?)");
				     line[i] = 1;
				 }
				 
				 conn.setAutoCommit(false);   
			     	
				 String date;
				  
				
				 for(int i=start;i<end;i++)
				 {
		            // 显示行号
					 
					 //测试测试测试测试测试测试测试
					 if(rec[i] == null)
						 System.out.println("error i="+i);
					 				 
		            tmp = (int) rec[i].getTimestamp();
					datenum = ((tmp - 1393603200)/86400+1);
		            
					if(datenum < 10)
						date = "00"+datenum;
					else
						date =  "0"+datenum;
		            
		            
					pst[datenum].setInt(1, rec[i].getUid()); 
					pst[datenum].setInt(2, rec[i].getIid()); 
					pst[datenum].setDouble(3, rec[i].getTimestamp());
					pst[datenum].addBatch();//用PreparedStatement的批量处理 
		           
		           
		            for(int j=1;j<32;j++)	            
		        	   if(line[j]%5000==0){
		            		pst[j].executeBatch();   
			                conn.commit();   
			                pst[j].clearBatch(); 
		            	}
		            
		             line[datenum]++;
				  }//for
				  
				  for(int j=1;j<32;j++)	            
				  {
						pst[j].executeBatch();   
		                conn.commit();   
		                pst[j].clearBatch(); 
				  }
				  
				  long endTime = System.currentTimeMillis();
				  System.out.println("----------------------id="+id+" start="+start+" end="+end+" 程序运行时间："+(endTime-startTime)+"ms----------------------");
				  
			   } catch (Exception e) {  
				  e.printStackTrace(); 
				  File file = new File(strPath);
					if (!file.exists()) {
					   file.createNewFile();
					 }				  
					 FileWriter fileWriter = new FileWriter(strPath,true);


					 String nextLine = System.getProperty("line.separator");
					  
					 fileWriter.write(e+nextLine);
					 fileWriter.flush();
					 fileWriter.close();
			   }finally{
				   for(int j=1;j<32;j++)
						ConnectionSource.close(pst[j]);
					  ConnectionSource.close(conn); 
				   return id;	
			   } 
			  
			    
		 }
	}

	public void start(){
		String[] res;
		int THREADNUM =15;
		int CAPACITY = 40000;
		//Record rec[][] = new Record[THREADNUM][CAPACITY];
		int leisure[] = new int[THREADNUM];
		BufferedReader reader = null;
		String tempString = null;
		int sum=0,nowid=-1;
		
	    ExecutorService threadPool = Executors.newFixedThreadPool(THREADNUM);  
		CompletionService<Integer> pool = new ExecutorCompletionService<Integer>(threadPool);
		
		   
		Record rec[] = new Record[CAPACITY];
		for(int i=0;i<THREADNUM;i++)
		{
			
			leisure[i] = 1;
		}
			
		
		try {
			reader = new BufferedReader(new InputStreamReader(new FileInputStream("F:/task_res_caixingwang.txt"),"UTF-8"));
			while ((tempString = reader.readLine()) != null) 
			{
				//选择新的队列去注入
				if(nowid == -1 || leisure[nowid] == 0)//还没有选择队列或者原来的队列已经开始执行
				{
					//选择没有满的队里
					nowid=-1;
					for(int i=0;i<THREADNUM;i++)
					{
						if(leisure[i] == 1)
						{
							nowid = i;
							break;
						}
					}
					//没有空闲的线程，等待其中一个线程结束
					if(nowid == -1){
						
						if((nowid = pool.take().get()) == -1){
							System.out.println("error");
							leisure[nowid] = 1;
				    	}else{
				    		leisure[nowid] = 1;
				    	}
						
					}
					
					
				}//if
				
				//注入没有满的队里
				res = tempString.split(" ");
				rec[sum%CAPACITY] = new Record(Integer.parseInt(res[0]), Integer.parseInt(res[1]), Double.parseDouble(res[2]));
				
				
				//队列已满
				if(sum%CAPACITY == CAPACITY-1)
				{
					leisure[nowid] = 0;//开始执行任务，不在空闲
					pool.submit(new task(rec,nowid,0,CAPACITY));
					rec = new Record[CAPACITY];
					//System.out.println("----------------------"+sum+"----------------------");
				}
				
				if(sum == 1112345)
					break;
				
				sum++;
	
			}//while
			
			if(sum%CAPACITY!=0)
				pool.submit(new task(rec,nowid,0,sum%CAPACITY));
			
			threadPool.shutdown();
			
			//System.out.println("----------------------"+sum+"----------------------");
			while(!threadPool.awaitTermination(2, TimeUnit.SECONDS)){
				System.out.println("WAIT PHASE2");
            }
			
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}


	public static void main(String[] args) throws IOException {
		
		long startTime = System.currentTimeMillis();//获取当前时间
		MultiDat2db mdDat2db2 = new MultiDat2db();
		mdDat2db2.start();
		
		long endTime = System.currentTimeMillis();
		System.out.println("程序运行时间："+(endTime-startTime)/60000+"min");
	}
}


