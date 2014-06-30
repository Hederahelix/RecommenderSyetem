package Cal4Sim;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import net.paoding.analysis.analyzer.PaodingAnalyzer;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;

import Dat2Db.ConnectionSource;


public class MultiPaoDingAnalysis {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		long startTime = System.currentTimeMillis();//��ȡ��ǰʱ��
		 
		try {
			ExecutorService threadPool = Executors.newFixedThreadPool(30);  
			CompletionService<Integer> pool = new ExecutorCompletionService<Integer>(threadPool);
			
			
			Connection conn = ConnectionSource.getConnection();
			PreparedStatement pst = conn.prepareStatement("select iid,contents from news where id > ? limit 10000");
	        
	        
	        Statement st = conn.createStatement();
			ResultSet set = st.executeQuery("select count(id) from news");
			set.next();
			int total = set.getInt(1);//��������
			int tasknum = 0;int complete = 0;
			ConnectionSource.close(st);
			
			for(int i=0;i*10000<total;i++)
			{
				pst.setInt(1, i*10000);
				set = pst.executeQuery();
				
				while (set.next()) 
		        {
		        	pool.submit(new analysis(set.getInt(1),set.getString(2)));
		        	tasknum++;	
	            }
				
				while (true) 
		        {
		    	   if(pool.take().get() != 0){
		    		   complete++;
		    		   System.out.println(complete);
		    	   }else{
		    		   complete++;
		    		   System.out.println("error");
		    	   }
		    		   
		    	   
		    	   if(complete == tasknum)
		    		   break;
	           }
			   set.close();
			}
	        
	        
	        
	       threadPool.shutdown();
	       ConnectionSource.closeAll(pst,conn);
	       System.out.println("phase 2");
     
	       try { 
	             
	            while(!threadPool.awaitTermination(2, TimeUnit.SECONDS)){
	            	
	            }
	            
	        } catch (InterruptedException e) { 
	            e.printStackTrace(); 
	        } 

	       
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		long endTime = System.currentTimeMillis();
		System.out.println("��������ʱ�䣺"+(endTime-startTime)/60000+"min");
		
	}

}

class analysis implements Callable<Integer>{
	private int iid;
	private String contents;
	
	private String strPath = "./error.txt";
	
	public analysis(int iid,String contents) 
	{ 
		this.iid = iid; 
		this.contents = contents; 
	} 
	
	public Integer call(){
	      //Long operations
		int codenum = 1;
		PreparedStatement pst = null;
		Connection conn = null;
		try {
			conn = ConnectionSource.getConnection();
			pst = conn.prepareStatement("insert into test values (null,?,?,?,?)");
			conn.setAutoCommit(false);
			Analyzer analyzer = new PaodingAnalyzer(); //����һ��������  
			TokenStream tokenStream = analyzer.tokenStream(contents, new StringReader(contents)); //�õ�token���е������  
			
			
			
			HashMap<String, Integer> hm  = new HashMap<String, Integer>(); 
			float sum = 0;//�ִʸ���
			try {  
			    Token t; 
			   
			    while ((t = tokenStream.next()) != null)  
			    {  
			    	String temp = t.termText().trim();
			    	if(hm.containsKey(temp))
			    		hm.put(temp, hm.get(temp)+1);	
			    	else
			    		hm.put(temp, 1);
			    	sum++;
			    	
			    }  
			} catch (IOException e) {  
			    e.printStackTrace();  
			} 
			
			Iterator iter = hm.entrySet().iterator();
			int line = 1;
			while (iter.hasNext()) 
			{
				Map.Entry<String, Integer> entry = (Map.Entry<String, Integer>) iter.next();
				String key = entry.getKey();
				float val = entry.getValue()/sum;
				pst.setString(1, key); 
			    pst.setFloat(2, val);
			    pst.setFloat(3, 0); 
			    pst.setInt(4, iid);
			    pst.addBatch();//��PreparedStatement���������� 
			   
			    if(line%1000==0){//��������500���������ʱ�����ύ 
	            	pst.executeBatch();   
	                conn.commit();   
	                pst.clearBatch();  
	            }
			    line++;
			}
			
			if(line%1000!=0){//��������500���������ʱ�����ύ 
	        	pst.executeBatch();   
	            conn.commit();   
	            pst.clearBatch();  
	        }
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			File file = new File(strPath);
			if (!file.exists()) {
			   file.createNewFile();
			 }
			 
			   //true ��ʾ׷��д��
			 FileWriter fileWriter = new FileWriter(strPath,true);

			  //����д��
			 String nextLine = System.getProperty("line.separator");
			  
			 fileWriter.write(nextLine+ "iid:"+iid+" contents:"+contents);
			 fileWriter.flush();
			 fileWriter.close();
			 codenum = 0;
		}finally{
			ConnectionSource.closeAll(pst,conn);
			return codenum;
		}
		
		
	    
	 }
}