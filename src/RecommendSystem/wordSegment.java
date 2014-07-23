package RecommendSystem;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
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

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;

import redis.clients.jedis.Jedis;

public class wordSegment {
	
	///////////////////////////////////////////������ResultSet������ ������ һ���߳�ֻ����һ��resultset
	public void segmentWordbyPd(ResultSet set,BufferedWriter[] bw,String splitChar) throws SQLException, IOException{
		long startTime = System.currentTimeMillis();//��ȡ��ǰʱ��	
        String tempString;
        int datenum,iid;
        
		Analyzer analyzer = new PaodingAnalyzer(); //����һ��������  
		int line = 1;
		while (set.next()) 
        {
			//�����·ִ�									   //contents
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
				datenum = (int) Math.ceil((float)iid/30000);/////////////////////////////////
				tempString = key.trim()+splitChar+val+splitChar+0+splitChar+iid+splitChar+1;
				bw[datenum].write(tempString);
				bw[datenum].newLine();//����
			}
			line++;			
				
			
        }//while
		
		

		long endTime = System.currentTimeMillis();
		System.out.println("����10000����  �ִʳ�������ʱ�䣺"+(endTime-startTime)/60000+"min");
	}
	
/*	public void segmentWordbyNlpir(HashMap<Integer, String> subset,Jedis jedis,String splitChar) throws SQLException, IOException{
		String res[];
		for (Iterator it = subset.keySet().iterator(); it.hasNext(); ) 
		{
		    int iid = (int) it.next();
		    String contents = subset.get(iid);
		    
		    res = Nlpir.spliteword(contents, "utf-8"); 
			HashMap<String, Integer> hm  = new HashMap<String, Integer>(); 
			float sum = 0;//�ִʸ���
			
			//ͳ�Ʒִʸ���   �� ��Ƶ
			for(int i=0;i<res.length;i++)
		    {  
		    	String temp = res[i];
		    		
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
				String token = entry.getKey();
				float tf = entry.getValue()/sum;
				jedis.append(token.trim(), splitChar+iid+splitChar+tf);	
			}
		}
	}
	
	
	public void Segment4Content(String newsName,String tokenName) throws SQLException, InterruptedException, IOException{

		Connection conn = ConnectionSource.getConnection();
		PreparedStatement pst = conn.prepareStatement("select iid,contents from "+newsName+" where id > ? limit 10000");
		conn.setAutoCommit(false);

		
		//String tmpfile = "F:/data/tmpFile.txt", splitChar="\t";
		//BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(tmpfile), "UTF-8"));       	
        
		//0.Prepare
		Semaphore semp = new Semaphore(20);
		ExecutorService threadPool = Executors.newFixedThreadPool(20);
		
		Statement st = conn.createStatement();
		ResultSet set = st.executeQuery("select count(id) from "+newsName);
		set.next();
		int total = set.getInt(1);//��������
		
		//1.Segment Word
		int i; 
		String[] loadsql = new String[(int) Math.ceil((float)total/10000)];
		for(i=0;i*10000<total;i++)
		{
			pst.setInt(1, i*10000);
			set = pst.executeQuery();
			//segmentWordbyNlpir(set,bw,splitChar);
			semp.acquire();//�ȴ����߳�����������½��µ�����
			System.out.println("--------------------------------"+i+"th thread start");
			//threadPool.submit(new segmentTask(i,set,semp));
			
			loadsql[i] = "LOAD DATA LOCAL INFILE 'F:/data/xisihutong/tmp/token"+i+".txt' INTO TABLE "+tokenName+" FIELDS TERMINATED BY '\t'"
					+ " (token,tf,tfidf,iid);";
			set.close();
		}
		
		threadPool.shutdown();
	    System.out.println("phase 2");
	    
	    while(!threadPool.awaitTermination(1, TimeUnit.MINUTES)){
	    	System.out.println("phase 2");
        }
		//bw.flush();
       // bw.close();  
        
        //2.Import into database
        String[] createIndex={"CREATE INDEX `index1` ON "+tokenName+" (iid);","CREATE INDEX `index2` ON "+tokenName+" (token);","CREATE INDEX `index3` ON "+tokenName+" (tfidf,iid);"};	
        new dat2Db().loadwithoutindex(null, createIndex, loadsql);
        
        //3.Calculate TFIDF
        calTfidf(newsName,tokenName);
        PreparedStatement pst1 = conn.prepareStatement("update "+tokenName+" set tfidf=? where id=?");
		PreparedStatement pst2 = conn.prepareStatement("select count(id) from "+tokenName+" where token=?");
		PreparedStatement pst3 = conn.prepareStatement("select * from "+tokenName+" where id > ? limit 10000");
		HashMap<String, Integer> hm = new HashMap<String, Integer>();//�洢����token�ĳ��ָôʵ���������
		set = st.executeQuery("select count(id) from "+newsName);
		set.next();
		total = set.getInt(1);//��������
		
        
		
		st.close();
        pst.close();
        conn.close();
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
				count = hm.get(token);//���������������
			}
			else
			{
				pst2.setString(1, token); 
				ResultSet rs = pst2.executeQuery(); 
				rs.next();
				count = rs.getInt(1);//���������������
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
		HashMap<String, Integer> hm = new HashMap<String, Integer>();//�洢����token�ĳ��ָôʵ���������
		int num = 1;
		
		//��������
		ResultSet set = st.executeQuery("select count(id) from "+newsName);
		set.next();
		int total = set.getInt(1);//��������
		set.close();
		//�ִ�����
		set = st.executeQuery("select count(id) from "+tokenName+"");
		set.next();
		int sum = set.getInt(1);//�ִ�����
		set.close();
		
		long startTime,endTime;
		for(int i=0;i*10000<sum;i++)
		{
			startTime = System.currentTimeMillis();//��ȡ��ǰʱ��
			pst3.setInt(1, i*10000);
			set = pst3.executeQuery();
			analysis(set,pst1,pst2,total,hm);
			//�ύ
			pst1.executeBatch();   
	        conn.commit();   
	        pst1.clearBatch();
	        
			set.close();
			endTime = System.currentTimeMillis();//��ȡ��ǰʱ��
			System.out.println((i+1)*10000+" ��ʱ��"+(endTime-startTime)/60000+"min");
		}
		
		ConnectionSource.close(pst1);
		ConnectionSource.close(pst2);
		ConnectionSource.close(pst3);
		ConnectionSource.close(st);
		ConnectionSource.close(conn);
	}
	*/
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
	}

}



class segmentTask1 implements Callable<Integer>{
	private int id;
	private HashMap<Integer, String> subset;
	private Semaphore semp;
	private Jedis jedis;
	private Logger logger = Logger.getLogger(segmentTask1.class);
	
	public segmentTask1(int id,HashMap<Integer, String> subset,Semaphore semp){
		this.subset = subset;
		this.id = id;
		this.semp = semp;
		
	}
	
	
	public Integer call(){
		
		long startTime = System.currentTimeMillis();//��ȡ��ǰʱ��	
		jedis = redisUtil.getJedis();	
        
        String res[];
        HashMap<String, Integer> hm = new HashMap<String, Integer>();
		for (Iterator it = subset.keySet().iterator(); it.hasNext(); ) 
		{
		    int iid = (int) it.next();
		    String contents = subset.get(iid);//iid contents
		    
		    res = Nlpir.spliteword(contents, "utf-8"); 
		    hm.clear(); 
			float sum = 0;//�ִʸ���
			
			//ͳ�Ʒִʸ���
			for(int i=0;i<res.length;i++)
		    {  
		    	String temp = res[i].trim();
		    		
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
				String token = entry.getKey();
				float tf = entry.getValue()/sum;
				jedis.append(token, " "+iid+" "+tf);	
			}
		}
        
		long endTime = System.currentTimeMillis();
		System.out.println("id="+id+"  WordSegment��������ʱ�䣺"+(endTime-startTime)+"ms");
		redisUtil.returnResource(jedis);
		semp.release();
		return 1;
		    
	 }
}

class segmentTask2 implements Callable<Integer>{
	private int id;
	private int total;
	private List<String> set;
	private Semaphore semp;
	private Jedis jedis;
	private int dbNum;
	private String filePath;
	private Logger logger = Logger.getLogger(segmentTask2.class);
	
	
	public segmentTask2(int id,int total,List<String> set,Semaphore semp,int dbNum,String filePath){
		this.set = set;
		this.id = id;
		this.total = total;
		this.semp = semp;
		this.dbNum = dbNum;
		this.filePath = filePath;
	}
	
	//set.getInt(1) set.getString(2)
	public Integer call(){
		
		long startTime = System.currentTimeMillis();//��ȡ��ǰʱ��
		jedis = redisUtil.getJedis();
		String tmpfile;
		BufferedWriter[] bw = new BufferedWriter[dbNum];
		try {
			for(int i=0;i<dbNum;i++)
			{
				tmpfile = filePath+"token"+id+"for"+i+".txt"; //"F:/data/xisihutong/tmp/token"+id+"for"+i+".txt"; 
				bw[i] = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(tmpfile), "UTF-8"));
			}
				
		} catch (UnsupportedEncodingException | FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error("line345  ����", e);
		}  
		
        String tmp,token,linestring; int iid; float tf = 0;
        Iterator it = set.iterator();
        while(it.hasNext())
        {
        	token = (String) it.next();   
        	String[] res = jedis.get(token).split(" ");
        	float count = (res.length-1)/2;
        	float idf = (float) Math.log(total/count);
        	
        	for(int i=1;i<res.length;i+=2)//������һ���ո�
        	{
        		iid = Integer.parseInt(res[i]);
        		tf = Float.parseFloat(res[i+1]);
        		float tfidf = tf*idf;
        		linestring = token+"\t"+tf+"\t"+tfidf+"\t"+iid;
				try {
					bw[iid%dbNum].write(linestring);
					bw[iid%dbNum].newLine();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					logger.error("line370  ����  linestring = "+linestring, e);
				}//����
        	}
        	
        }
        
        
        try {
        	for(int i=0;i<dbNum;i++)
			{
        		bw[i].flush();
        		bw[i].close();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error("line420  ����  ", e);
		}
        
        
		long endTime = System.currentTimeMillis();
		System.out.println("id="+id+"  calTFIDF��������ʱ�䣺"+(endTime-startTime)+"ms");
		redisUtil.returnResource(jedis);
		semp.release();
		return 1;
		    
	 }
}


class loadTask implements Callable<Integer>{
	private int dbid;
	private int fileNum;
	private String filePath;
	private String tokenName;
	private Logger logger = Logger.getLogger(loadTask.class);
	
	public loadTask(int dbid,int fileNum,String filePath,String tokenName){
		this.dbid = dbid;
		this.fileNum = fileNum;
		this.filePath = filePath;
	}
	
	
	public Integer call(){
		
		long startTime = System.currentTimeMillis();//��ȡ��ǰʱ��	
		
		String[] loadsql = new String[fileNum]; String fileName,dbName;
		String[] createIndex = new String[fileNum*3];
		
		dbName = tokenName+"_"+dbid;
		for(int i=0;i<fileNum;i++)
		{
			fileName = filePath+"token"+i+"for"+dbid+".txt";
			
			loadsql[i] = "LOAD DATA LOCAL INFILE '"+fileName+"' INTO TABLE "+dbName+" FIELDS TERMINATED BY '\t'"
					+ " (token,tf,tfidf,iid);";
			
			createIndex[i*3]="CREATE INDEX `index1` ON "+dbName+" (iid);";
			createIndex[i*3+1]="CREATE INDEX `index2` ON "+dbName+" (token);";
			createIndex[i*3+2]="CREATE INDEX `index3` ON "+dbName+" (tfidf,iid);";	
	        
		}
		try {
			new dat2Db().loadwithoutindex(null, createIndex, loadsql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
		long endTime = System.currentTimeMillis();
		System.out.println("dbid="+dbid+"  load file ��������ʱ�䣺"+(endTime-startTime)+"ms");
		
		return 1;
		    
	 }
}