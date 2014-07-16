package RecommendSystem;

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
	
	public void segmentWordbyNlpir(ResultSet set,BufferedWriter bw,String splitChar) throws SQLException, IOException{
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
	
	public void Segment4Content(String tableName,String file,String splitChar) throws SQLException, InterruptedException, IOException{

		Connection conn = ConnectionSource.getConnection();
		PreparedStatement pst = conn.prepareStatement("select iid,contents from "+tableName+" where id > ? limit 10000");
		
		//内容总数
		Statement st = conn.createStatement();
		ResultSet set = st.executeQuery("select count(id) from "+tableName);
		set.next();
		int total = set.getInt(1);//新闻总数
		st.close();
		

		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), "UTF-8"));       	
        
		
		//分词生成文件
		for(int i=0;i*10000<total;i++)
		{
			pst.setInt(1, i*10000);
			set = pst.executeQuery();
			segmentWordbyNlpir(set,bw,splitChar);
			set.close();
		}
		
		
		bw.flush();
        bw.close();  
        
		
	    ConnectionSource.closeAll(pst,conn);
	}

	private void analysis(ResultSet set,PreparedStatement pst1,PreparedStatement pst2,int total,HashMap<String, Integer> hm) throws SQLException
	{ 
		
		while (set.next()) 
		{
			int id = set.getInt(1);
			String token =  set.getString(2);
			float tf = set.getFloat(3);
			float count = 0;
			if(hm.containsKey(token))
			{
				count = hm.get(token);//词语出现文章总数
			}
			else
			{
				pst2.setString(1, token); 
				ResultSet rs = pst2.executeQuery(); 
				rs.next();
				count = rs.getInt(1);//词语出现文章总数
				rs.close();
				hm.put(token, (int) count);
			}
			 
			float idf = (float) Math.log(total/count);
			float tfidf = tf*idf;
			pst1.setFloat(1, tfidf);
			pst1.setInt(2, id);
			//pst1.executeUpdate();
			pst1.addBatch();
       }
		
	}
	
	public void calTfidf(String newsName,String tokenName) throws SQLException
	{
		Connection conn = ConnectionSource.getConnection();
		conn.setAutoCommit(false);
		PreparedStatement pst1 = conn.prepareStatement("update "+tokenName+" set tfidf=? where id=?");
		PreparedStatement pst2 = conn.prepareStatement("select count(id) from "+tokenName+" where token=?");
		PreparedStatement pst3 = conn.prepareStatement("select * from "+tokenName+" where id > ? limit 10000");
		Statement st = conn.createStatement();
		HashMap<String, Integer> hm = new HashMap<String, Integer>();//存储所有token的出现该词的文章总数
		int num = 1;
		
		//文章总数
		ResultSet set = st.executeQuery("select count(id) from "+newsName);
		set.next();
		int total = set.getInt(1);//文章总数
		set.close();
		//分词总数
		set = st.executeQuery("select count(id) from "+tokenName+"");
		set.next();
		int sum = set.getInt(1);//分词总数
		set.close();
		long startTime,endTime;
		
		for(int i=0;i*10000<sum;i++)
		{
			startTime = System.currentTimeMillis();//获取当前时间
			pst3.setInt(1, i*10000);
			set = pst3.executeQuery();
			analysis(set,pst1,pst2,total,hm);
			//提交
			pst1.executeBatch();   
	        conn.commit();   
	        pst1.clearBatch();
	        
			set.close();
			endTime = System.currentTimeMillis();//获取当前时间
			System.out.println((i+1)*10000+" 耗时："+(endTime-startTime)/60000+"min");
		}
		
		ConnectionSource.close(pst1);
		ConnectionSource.close(pst2);
		ConnectionSource.close(pst3);
		ConnectionSource.close(st);
		ConnectionSource.close(conn);
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
	}

}

