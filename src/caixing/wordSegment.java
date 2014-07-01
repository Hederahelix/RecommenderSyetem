package caixing;

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

import fengNiao.dat2Db;
import Dat2Db.ConnectionSource;





public class wordSegment {
	
	/*class task implements Callable<Integer>{
		private int id;
		private ResultSet set;
		private Semaphore semp;
		
		public task(int id,ResultSet set,Semaphore semp){
			this.set = set;
			this.id = id;
			this.semp = semp;
		}
		
		//set.getInt(1),set.getString(2)
		public Integer call() throws SQLException, IOException{
			
			long startTime = System.currentTimeMillis();//��ȡ��ǰʱ��
			
			Connection conn = ConnectionSource.getConnection();	
			PreparedStatement pst = conn.prepareStatement("insert into token values (null,?,?,?,?,?)");
			conn.setAutoCommit(false);
			Analyzer analyzer = new PaodingAnalyzer(); //����һ��������  
			System.out.println("xian cheng kai shi");
			while (set.next()) 
	        {
				System.out.println("fen ci kai shi");
				//�����·ִ�
				TokenStream tokenStream = analyzer.tokenStream(set.getString(2), new StringReader(set.getString(2))); //�õ�token���е������  
				HashMap<String, Integer> hm  = new HashMap<String, Integer>(); 
				float sum = 0;//�ִʸ���
				Token t; 
				//ͳ�Ʒִʸ���   
			    while ((t = tokenStream.next()) != null)  
			    {  
			    	String temp = t.termText().trim();
			    	if(hm.containsKey(temp))
			    		hm.put(temp, hm.get(temp)+1);	
			    	else
			    		hm.put(temp, 1);
			    	sum++;
			    	
			    }
			    //�����Ƶ
			    Iterator iter = hm.entrySet().iterator();
				int line = 1;
				while (iter.hasNext()) 
				{
					Map.Entry<String, Integer> entry = (Map.Entry<String, Integer>) iter.next();
					String key = entry.getKey();
					float val = entry.getValue()/sum;
					pst.setString(1, key); //token
				    pst.setFloat(2, val);//idf
				    pst.setFloat(3, 0); //tfidf
				    pst.setString(4, set.getString(1));//iid
				    pst.setInt(5, 1);//type
				    pst.addBatch();//��PreparedStatement���������� 
				   
				    if(line%5000==0){//��������500���������ʱ�����ύ 
		            	pst.executeBatch();   
		                conn.commit();   
		                pst.clearBatch();  
		                System.out.println(id+": "+line);
		            }
				    line++;
				}
				pst.executeBatch();   
                conn.commit();   
                pst.clearBatch();  
                System.out.println("fen ci jie shu");
	        }		
			set.close();
			ConnectionSource.closeAll(pst,conn);
			long endTime = System.currentTimeMillis();
			System.out.println("id="+id+"  ��������ʱ�䣺"+(endTime-startTime)+"ms");
			//semp.release();
			return 1;
			    
		 }
	}
	
	
	public void Segment4NewsContent() throws SQLException, InterruptedException{
		ExecutorService threadPool = Executors.newCachedThreadPool();  
		
		Connection conn = ConnectionSource.getConnection();
		PreparedStatement pst = conn.prepareStatement("select iid,contents from news where id > ? limit 10000");
		Statement st = conn.createStatement();
		ResultSet set = st.executeQuery("select count(id) from news");
		set.next();
		int total = set.getInt(1);//��������
		ConnectionSource.close(st);
		Semaphore semp = new Semaphore(10);
		
		for(int i=0;i*10000<total;i++)
		{
			pst.setInt(1, i*10000);
			set = pst.executeQuery();
			//semp.acquire();//�ȴ����߳�����������½��µ�����
			System.out.println(i);
			threadPool.submit(new task(i,set,semp));
		}
		
		threadPool.shutdown();
	    ConnectionSource.closeAll(pst,conn);
	    System.out.println("phase 2");
	    
	    while(!threadPool.awaitTermination(2, TimeUnit.SECONDS)){
	    	System.out.println("wait 2");
        }
	}
	*/
	
	/*
	 * BufferedWriter[] bw = new BufferedWriter[totalfile];
	 * for(int i=1;i<totalfile;i++)
        {
        	bw[i] = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("D:/data/fengniao/splitefilesbyiid/file"+i+".txt"), "UTF-8"));       	
        }
	 * */
	public int segmentWordbyPd(ResultSet set,BufferedWriter[] bw) throws SQLException, IOException{
		long startTime = System.currentTimeMillis();//��ȡ��ǰʱ��	
		int totalfile = 110;
		
        String tempString;
        String splitChar = " ";
        int datenum,iid;
        
        
        
        
        
		Analyzer analyzer = new PaodingAnalyzer(); //����һ��������  
		int line = 1;
		while (set.next()) 
        {
			//�����·ִ�
			TokenStream tokenStream = analyzer.tokenStream(set.getString(2), new StringReader(set.getString(2))); //�õ�token���е������  
			HashMap<String, Integer> hm  = new HashMap<String, Integer>(); 
			float sum = 0;//�ִʸ���
			Token t; 
			//ͳ�Ʒִʸ���   
		    while ((t = tokenStream.next()) != null)  
		    {  
		    	String temp = t.termText().trim();
		    	if(hm.containsKey(temp))
		    		hm.put(temp, hm.get(temp)+1);	
		    	else
		    		hm.put(temp, 1);
		    	sum++;
		    	
		    }
		    //�����Ƶ
		    Iterator iter = hm.entrySet().iterator();

			while (iter.hasNext()) 
			{
				Map.Entry<String, Integer> entry = (Map.Entry<String, Integer>) iter.next();
				String key = entry.getKey();
				float val = entry.getValue()/sum;
				iid = set.getInt(1);
				datenum = (int) Math.ceil((float)iid/30000);
				tempString = key.trim()+splitChar+val+splitChar+0+splitChar+iid+splitChar+1;
				bw[datenum].write(tempString);
				bw[datenum].newLine();//����
			}
			line++;			
				
			
        }//while
		
		for(int i=1;i<totalfile;i++)
        {
			bw[i].flush();
        	bw[i].close();  
        }
		set.close();
		
		long endTime = System.currentTimeMillis();
		System.out.println("����10000����  �ִʳ�������ʱ�䣺"+(endTime-startTime)/60000+"min");
		return totalfile;
	}
	
	public void segmentWordbyNlpir(ResultSet set) throws SQLException, IOException{
		long startTime = System.currentTimeMillis();//��ȡ��ǰʱ��	
		Connection conn = ConnectionSource.getConnection();
		conn.setAutoCommit(false);
		PreparedStatement pst = conn.prepareStatement("insert into token4caixin values (null,?,?,?,?,?); ");
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("D:/data/caixin/token/file.txt"), "UTF-8")); 
        String tempString;
        String splitChar = "|";
        int datenum,iid;
        String token[];
        
		int line = 1;
		while (set.next()) 
        {
			token = Nlpir.spliteword(set.getString(2), "utf-8");
			HashMap<String, Integer> hm  = new HashMap<String, Integer>(); 
			float sum = 0;//�ִʸ���

			//ͳ�Ʒִʸ���   
			for(int i=0;i<token.length;i++)
		    {  
		    	String temp = token[i];
		    	if(temp.length()>255)
		    	{
		    		System.out.println(set.getString(2));
		    		System.out.println(temp);
		    		System.in.read();
		    	}	
		    		
		    	if(hm.containsKey(temp))
		    		hm.put(temp, hm.get(temp)+1);	
		    	else
		    		hm.put(temp, 1);
		    	sum++;
		    	
		    }
		    //�����Ƶ
		    Iterator iter = hm.entrySet().iterator();

			while (iter.hasNext()) 
			{
				Map.Entry<String, Integer> entry = (Map.Entry<String, Integer>) iter.next();
				String key = entry.getKey();
				float val = entry.getValue()/sum;
				iid = set.getInt(1);
				tempString = key+splitChar+val+splitChar+0+splitChar+iid+splitChar+1;

				pst.setString(1, key);
				pst.setFloat(2, val);
				pst.setFloat(3, 0);
				pst.setString(4, ""+iid);
				pst.setInt(5, 1);
				pst.addBatch();
			}
			line++;			
			if(line % 500 == 0)
			{
				pst.executeBatch();   
                conn.commit();   
                pst.clearBatch();
                
			}	
			
        }//while
		
		pst.executeBatch();   
        conn.commit();   
        pst.clearBatch();
        
        pst.close();
        conn.close();
		set.close();
		
		long endTime = System.currentTimeMillis();
		System.out.println("����10000����  �ִʳ�������ʱ�䣺"+(endTime-startTime)/60000+"min");
	}
	
	public void segmentWordbyNlpir2(ResultSet set,BufferedWriter bw) throws SQLException, IOException{
		long startTime = System.currentTimeMillis();//��ȡ��ǰʱ��	
		
		String tempString;
        String splitChar = " ";
        int datenum,iid;
        String token[];
        
		int line = 1;
		while (set.next()) 
        {
			token = Nlpir.spliteword(set.getString(2), "utf-8");
			HashMap<String, Integer> hm  = new HashMap<String, Integer>(); 
			float sum = 0;//�ִʸ���

			//ͳ�Ʒִʸ���   
			for(int i=0;i<token.length;i++)
		    {  
		    	String temp = token[i];
		    	if(temp.length()>255)
		    	{
		    		System.out.println(set.getString(2));
		    		System.out.println(temp);
		    		System.in.read();
		    	}	
		    		
		    	if(hm.containsKey(temp))
		    		hm.put(temp, hm.get(temp)+1);	
		    	else
		    		hm.put(temp, 1);
		    	sum++;
		    	
		    }
		    //�����Ƶ
		    Iterator iter = hm.entrySet().iterator();

			while (iter.hasNext()) 
			{
				Map.Entry<String, Integer> entry = (Map.Entry<String, Integer>) iter.next();
				String key = entry.getKey();
				float val = entry.getValue()/sum;
				iid = set.getInt(1);
				tempString = key+splitChar+val+splitChar+0+splitChar+iid+splitChar+1;

				bw.write(tempString);
				bw.newLine();//����
			}
			line++;			
			
			
        }//while
		
		bw.flush();
		
		
		set.close();
		
		long endTime = System.currentTimeMillis();
		System.out.println("����10000����  �ִʳ�������ʱ�䣺"+(endTime-startTime)/60000+"min");
	}
	
	public void Segment4NewsContent() throws SQLException, InterruptedException, IOException{
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("D:/data/caixin/token/file.txt"), "UTF-8")); 
        
		Connection conn = ConnectionSource.getConnection();
		PreparedStatement pst = conn.prepareStatement("select iid,contents from news4caixin where id > ? limit 10000");
		Statement st = conn.createStatement();
		ResultSet set = st.executeQuery("select count(id) from news4caixin");
		set.next();
		int total = set.getInt(1);//��������
		ConnectionSource.close(st);
		int totalfile = 0;
		//�ִ������ļ�
		for(int i=0;i*10000<total;i++)
		{
			pst.setInt(1, i*10000);
			set = pst.executeQuery();
			segmentWordbyNlpir2(set,bw); 
		}
		bw.close(); 
		//��ʼ���ִʽ���������ݿ�
		System.out.println("��ʼ���ִʽ���������ݿ�");
		dat2Db.loadwithoutindex("D:/data/caixin/token/file.txt", "token4caixin");
		
	    ConnectionSource.closeAll(pst,conn);
	}
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		long startTime = System.currentTimeMillis();//��ȡ��ǰʱ��
		wordSegment ws = new wordSegment();
		try {
			ws.Segment4NewsContent();
		} catch (SQLException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		long endTime = System.currentTimeMillis();
		System.out.println("��������ʱ�䣺"+(endTime-startTime)/60000+"min");
	}

}
