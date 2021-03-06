package Common;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import net.paoding.analysis.analyzer.PaodingAnalyzer;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;





public class wordSegment {
	
	
	public void segmentWordbyPd(ResultSet set,BufferedWriter[] bw,String splitChar) throws SQLException, IOException{
		long startTime = System.currentTimeMillis();//获取当前时间	
        String tempString;
        int datenum,iid;
        
		Analyzer analyzer = new PaodingAnalyzer(); //定义一个解析器  
		int line = 1;
		while (set.next()) 
        {
			//将文章分词									   //contents
			TokenStream tokenStream = analyzer.tokenStream(set.getString(2), new StringReader(set.getString(2))); //得到token序列的输出流  
			HashMap<String, Integer> hm  = new HashMap<String, Integer>(); 
			float sum = 0;//分词个数
			Token t; 
			//统计分词个数   
		    while ((t = tokenStream.next()) != null)  
		    {  
		    	String temp = t.termText().trim();
		    	if(hm.containsKey(temp))
		    		hm.put(temp, hm.get(temp)+1);	
		    	else
		    		hm.put(temp, 1);
		    	sum++;
		    	
		    }
		    //计算词频
		    Iterator iter = hm.entrySet().iterator();

			while (iter.hasNext()) 
			{
				Map.Entry<String, Integer> entry = (Map.Entry<String, Integer>) iter.next();
				String key = entry.getKey();
				float val = entry.getValue()/sum;
				iid = set.getInt(1);
				datenum = (int) Math.ceil((float)iid/30000);/////////////////////////////////
				tempString = key.trim()+splitChar+val+splitChar+0+splitChar+iid+splitChar+1;
				bw[datenum].write(tempString);
				bw[datenum].newLine();//换行
			}
			line++;			
				
			
        }//while
		
		

		long endTime = System.currentTimeMillis();
		System.out.println("处理10000文章  分词程序运行时间："+(endTime-startTime)/60000+"min");
	}
	
	public void segmentWordbyNlpir(ResultSet set,BufferedWriter[] bw,String splitChar) throws SQLException, IOException{
		long startTime = System.currentTimeMillis();//获取当前时间	
        String tempString;
        int datenum,iid;
        

		int line = 1;
		String token[];
		while (set.next()) 
        {
			//将文章分词									   //contents
			token = Nlpir.spliteword(set.getString(2), "utf-8"); 
			HashMap<String, Integer> hm  = new HashMap<String, Integer>(); 
			float sum = 0;//分词个数
			
			//统计分词个数   
			for(int i=0;i<token.length;i++)
		    {  
		    	String temp = token[i];
		    		
		    	if(hm.containsKey(temp))
		    		hm.put(temp, hm.get(temp)+1);	
		    	else
		    		hm.put(temp, 1);
		    	sum++;
		    	
		    }
		    //计算词频
		    Iterator iter = hm.entrySet().iterator();

			while (iter.hasNext()) 
			{
				Map.Entry<String, Integer> entry = (Map.Entry<String, Integer>) iter.next();
				String key = entry.getKey();
				float val = entry.getValue()/sum;
				iid = set.getInt(1);
				datenum = (int) Math.ceil((float)iid/30000);/////////////////////////////////
				tempString = key.trim()+splitChar+val+splitChar+0+splitChar+iid+splitChar+1;
				bw[datenum].write(tempString);
				bw[datenum].newLine();//换行
			}
			line++;			
				
			
        }//while
		
		

		long endTime = System.currentTimeMillis();
		System.out.println("处理10000文章  分词程序运行时间："+(endTime-startTime)/60000+"min");
	}
	
	public void segmentWordbyNlpir2(ResultSet set,BufferedWriter bw,String splitChar) throws SQLException, IOException{
		long startTime = System.currentTimeMillis();//获取当前时间	
        String tempString;
        int datenum,iid;
        

		int line = 1;
		String token[];
		while (set.next()) 
        {
			//将文章分词									   //contents
			token = Nlpir.spliteword(set.getString(2), "utf-8"); 
			HashMap<String, Integer> hm  = new HashMap<String, Integer>(); 
			float sum = 0;//分词个数
			
			//统计分词个数   
			for(int i=0;i<token.length;i++)
		    {  
		    	String temp = token[i];
		    		
		    	if(hm.containsKey(temp))
		    		hm.put(temp, hm.get(temp)+1);	
		    	else
		    		hm.put(temp, 1);
		    	sum++;
		    	
		    }
		    //计算词频
		    Iterator iter = hm.entrySet().iterator();

			while (iter.hasNext()) 
			{
				Map.Entry<String, Integer> entry = (Map.Entry<String, Integer>) iter.next();
				String key = entry.getKey();
				float val = entry.getValue()/sum;
				iid = set.getInt(1);

				tempString = key.trim()+splitChar+val+splitChar+0+splitChar+iid+splitChar+1;
				bw.write(tempString);
				bw.newLine();//换行
			}
			line++;			
				
			
        }//while
		
		

		long endTime = System.currentTimeMillis();
		System.out.println("处理10000文章  分词程序运行时间："+(endTime-startTime)/60000+"min");
	}
	
	public void Segment4Content(String tableName,String[] files,String splitChar) throws SQLException, InterruptedException, IOException{

		Connection conn = ConnectionSource.getConnection();
		PreparedStatement pst = conn.prepareStatement("select iid,contents from "+tableName+" where id > ? limit 10000");
		
		//内容总数
		Statement st = conn.createStatement();
		ResultSet set = st.executeQuery("select count(id) from "+tableName);
		set.next();
		int total = set.getInt(1);//新闻总数
		st.close();
		
		int totalfile = files.length;
		BufferedWriter[] bw = new BufferedWriter[totalfile];
        for(int i=0;i<totalfile;i++)
        {
        	bw[i] = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(files[i]), "UTF-8"));       	
        }
		
		//分词生成文件
		for(int i=0;i*10000<total;i++)
		{
			pst.setInt(1, i*10000);
			set = pst.executeQuery();
			if(bw.length == 1)
				segmentWordbyNlpir2(set,bw[0],splitChar); 
			else
				segmentWordbyNlpir(set,bw,splitChar);
			set.close();
		}
		
		for(int i=0;i<totalfile;i++)
        {
			bw[i].flush();
        	bw[i].close();  
        }
		
	    ConnectionSource.closeAll(pst,conn);
	}
	
	public void multiSegment4Content(String tableName,String[] files,String splitChar) throws SQLException, InterruptedException, IOException{
		ExecutorService threadPool = Executors.newCachedThreadPool();  
		
		Connection conn = ConnectionSource.getConnection();
		PreparedStatement pst = conn.prepareStatement("select iid,contents from "+tableName+" where id > ? limit 10000");
		
		//内容总数
		Statement st = conn.createStatement();
		ResultSet set = st.executeQuery("select count(id) from "+tableName);
		set.next();
		int total = set.getInt(1);//新闻总数
		set.close();
		st.close();
		

		Semaphore semp = new Semaphore(10);
		
		for(int i=0;i*10000<total;i++)
		{
			pst.setInt(1, i*10000);
			set = pst.executeQuery();
			semp.acquire();//等待有线程完成任务，再新建新的任务
			threadPool.submit(new segmentTask(i,set,semp));
			set.close();
		}
		
		
	    
		threadPool.shutdown();
	    ConnectionSource.closeAll(pst,conn);
	    System.out.println("phase 2");
	    
	    while(!threadPool.awaitTermination(2, TimeUnit.SECONDS)){
	    	
        }
		
	}
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
	}

}

class segmentTask implements Callable<Integer>{
	private int id;
	private ResultSet set;
	private Semaphore semp;
	private String tmpfile;
	
	public segmentTask(int id,ResultSet set,Semaphore semp){
		this.set = set;
		this.id = id;
		this.semp = semp;
		tmpfile = "D:/data/tmp/"+id+".txt"; 
	}
	
	//set.getInt(1),set.getString(2)
	public Integer call() throws SQLException, IOException{
		
		long startTime = System.currentTimeMillis();//获取当前时间
		
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(tmpfile), "UTF-8"));       	
        wordSegment ws = new wordSegment();
        ws.segmentWordbyNlpir2(set, bw, "\t");
        bw.flush();
        bw.close();
        
		long endTime = System.currentTimeMillis();
		System.out.println("id="+id+"  程序运行时间："+(endTime-startTime)+"ms");
		semp.release();
		return 1;
		    
	 }
}
