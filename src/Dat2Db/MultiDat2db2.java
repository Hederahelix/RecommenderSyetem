package Dat2Db;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;



public class MultiDat2db2 {
	private File file;
	private int lineNum;
	
	class task implements Callable<Integer>{

		private int id;
		private int start;
		private int end;
		private String strPath = "D:\\error_MultiDat2db.txt";
		
		public task(int id,int start,int end){			
			this.id = id;
			this.start = start;
			this.end = end;
			//System.out.println("----------------------id="+id+" start="+start+" end="+end+"----------------------");
		}
		
		public Integer call(){
			
			long startTime = System.currentTimeMillis();//获取当前时间
			long endTime,midTime;
		      //Long operations	 
			PreparedStatement pst[] = new PreparedStatement[32];
			int line[] = new int[32];
			String tableName;
			Connection conn = null;
			int tmp = 0;
			int datenum = 0;  
			BufferedReader reader = null;
			try {
				 
				 conn = ConnectionSource.getConnection();
				 conn.setAutoCommit(false); 
				 
				 for(int i=1;i<32;i++)
				 {
					 if(i<10)
						 tableName = "00"+i;
					 else
						 tableName = "0"+i;
					 
				     pst[i] = conn.prepareStatement("insert into trace_"+tableName+" values (null,?,?,?)");
				     line[i] = 1;
				 }
				 
				
				 reader = new BufferedReader(new FileReader(file));
	            // 一次读入一行，直到读入null为文件结束
	             for(int i=0;i<start-1;i++)
	             {   // 显示行号
	            	 reader.readLine();
	             }
				 
				   
			     	
				 String date,tempString;
				 Record rec;
				 String[] res;
				 int num = 0;
				 midTime = System.currentTimeMillis();//获取当前时间
				 for(int i=start;i<=end;i++)
				 {
		            // 显示行号
					 if((tempString = reader.readLine()) == null)
						 break;
					 
					 num++;
					 res = tempString.split(" ");
					 rec = new Record(Integer.parseInt(res[0]), Integer.parseInt(res[1]), Double.parseDouble(res[2]));
					
					 				 
		            tmp = (int) rec.getTimestamp();
					datenum = ((tmp - 1393603200)/86400+1);
		            
					if(datenum < 10)
						date = "00"+datenum;
					else
						date =  "0"+datenum;
		            
		            
					pst[datenum].setInt(1, rec.getUid()); 
					pst[datenum].setInt(2, rec.getIid()); 
					pst[datenum].setDouble(3, rec.getTimestamp());
					pst[datenum].addBatch();//用PreparedStatement的批量处理 
					line[datenum]++;
					
					if(num%100000 == 0){
						endTime = System.currentTimeMillis();
						System.out.println("----------------------id="+id+" 插入10w  程序运行时间："+(endTime-midTime)+"ms----------------------");
						midTime = endTime;
					}
		           
		            for(int j=1;j<32;j++)	            
		        	   if(line[j]%5000==0){
		            		pst[j].executeBatch();   
			                conn.commit();   
			                pst[j].clearBatch(); 
		            	}
		            
		             
				  }//for
				  
				  for(int j=1;j<32;j++)	            
				  {
						pst[j].executeBatch();   
		                conn.commit();   
		                pst[j].clearBatch(); 
				  }
				  
				  endTime = System.currentTimeMillis();
				  System.out.println("----------------------id="+id+" start="+start+" end="+end+" num="+num+" 程序运行时间："+(endTime-startTime)+"ms----------------------");
				  
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
				   if (reader != null) {
					   try {
						reader.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				   }
				   
				   return id;	
			   } 	  
			    
		 }
	}

	public void prepare(String fileName){
		file = new File(fileName);
        BufferedReader reader = null;
        try {
            
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            lineNum = 1;
            // 一次读入一行，直到读入null为文件结束
            while (reader.readLine() != null) {
                // 显示行号
            	lineNum++;
            }

            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
	}
	
	public void start() throws InterruptedException{
		int THREADNUM = 15;
		ExecutorService threadPool = Executors.newFixedThreadPool(THREADNUM);  
		CompletionService<Integer> pool = new ExecutorCompletionService<Integer>(threadPool);
		int CAPACITY = lineNum/THREADNUM+1;//向上取整
		System.out.println("CAPACITY="+CAPACITY);
		for(int i=0;i<THREADNUM;i++){
			pool.submit(new task(i,i*CAPACITY+1, (i+1)*CAPACITY));
		}
		
		threadPool.shutdown();
		
		
		while(!threadPool.awaitTermination(2, TimeUnit.SECONDS)){
			//System.out.println("WAIT PHASE2");
        }
	}
	
	public static void main(String[] args) throws IOException {
		
		long startTime = System.currentTimeMillis();//获取当前时间
		MultiDat2db2 md = new MultiDat2db2();
		try {
			md.prepare("F:/10000000w.txt");
			md.start();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		long endTime = System.currentTimeMillis();
		System.out.println("程序运行时间："+(endTime-startTime)/60000+"min");
	}
}

