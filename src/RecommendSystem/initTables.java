package RecommendSystem;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;

public class initTables {
	private Logger logger = Logger.getLogger(initTables.class);
	
	private void insert4news(String newsName,String hashName,String filename,String Regex) throws IOException, SQLException{
		int sum = 0,line = 0;
		String tempString = null,tmpfile="f:/data/tmp/newsName.txt"; 		
		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(filename),"UTF-8"));
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(tmpfile), "UTF-8")); 
		Pattern p=Pattern.compile(Regex);
		Matcher matcher;
		
		Connection conn = ConnectionSource.getConnection();
		HashMap<Integer, Integer> hm = new HashMap<Integer, Integer>();
		
		String title,contents,key; int iid;
		

		
		while ((tempString = reader.readLine()) != null) 
		{//title contents iid
			
			line++;
			matcher = p.matcher(tempString);
            if (matcher.find()) 
            {
            	title = matcher.group(1).trim();
            	contents = matcher.group(2).trim();
            	iid = Integer.parseInt(matcher.group(3).trim());
            	            	
            	if(hm.containsKey(iid))
            	{
            		logger.info("the same news in"+filename+":title "+title+" and contents "+contents);
            	}else{
            		hm.put(iid, sum);
            		sum++;
            		tempString = title+"\t"+contents+"\t"+sum;
            		bw.write(tempString);
            		bw.newLine();
            	}
            	
            	
            }else{
            	logger.info("can not match in"+filename+":"+tempString);
            }
            	
	
		}
		
		
        System.out.println("有效新闻共"+sum+" 新闻一共有"+line);
        bw.flush();
        bw.close();
        
        //load newstable
        String[] loadsql = {"LOAD DATA LOCAL INFILE '"+tmpfile+"' INTO TABLE "+newsName+" FIELDS TERMINATED BY '\t' (title,contents,iid);"};
        String[] createIndex = {"CREATE INDEX `index1` ON "+newsName+" (iid);"};
		try {
			new dat2Db().loadwithoutindex(null, null, loadsql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
		
		//load hashtable
		Iterator<Integer> iterator = hm.keySet().iterator();
		bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("f:/data/tmp/hashiid.txt"), "UTF-8")); 
	    while(iterator.hasNext())
	    {
	    	iid = iterator.next();
	    	bw.write(hm.get(iid)+"\t"+iid);//sum iid
	    	bw.newLine();
		}
	    bw.flush();
        bw.close();
        loadsql[0] = "LOAD DATA LOCAL INFILE '"+tmpfile+"' INTO TABLE "+hashName+" FIELDS TERMINATED BY '\t' (hashiid,iid);";
        createIndex[0] = "CREATE INDEX `index1` ON "+hashName+" (iid);";
        
		try {
			new dat2Db().loadwithoutindex(null, null, loadsql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        conn.close();
		reader.close();
	}
	
	public void initTrace(String traceName,String newsName,String trainFile,String testFile) throws SQLException, IOException
	{
		Connection conn = ConnectionSource.getConnection();
		conn.setAutoCommit(false);
		String sql = "CREATE TABLE `"+traceName+"` ("
						+  "`id` int(11) NOT NULL AUTO_INCREMENT,"
						+  "`uid` int(11) DEFAULT NULL,"
						+  "`iid` int(11) DEFAULT NULL,"
						+  "`time` int(11) DEFAULT NULL,"
						+  "`type` int(11) DEFAULT '0',"
						+  "PRIMARY KEY (`id`)"
						+  ")ENGINE=InnoDB DEFAULT CHARSET=utf8";
		
		Statement stm = conn.createStatement();
		stm.execute(sql);
		
		
		HashMap<Integer, Integer> iidhash = new HashMap<Integer, Integer>();
		HashMap<Integer, Integer> uids = new HashMap<Integer, Integer>();//trace中记录数大于1的uid
		
		
		
        String[] loadsql = {"LOAD DATA LOCAL INFILE '"+trainFile+"' INTO TABLE "+traceName+" FIELDS TERMINATED BY '\t' (uid,iid,time);",
        		"LOAD DATA LOCAL INFILE '"+testFile+"' INTO TABLE "+traceName+" FIELDS TERMINATED BY '\t' (uid,iid,time);"};
        try {
			new dat2Db().loadwithoutindex(null, null, loadsql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
        ResultSet set = stm.executeQuery("select hashiid,iid from iid4xisihutong");
		while(set.next())
		{
			iidhash.put(set.getInt(2), set.getInt(1));//iid hashiid
		}
		set.close();
		
		int sum = 0,sparenum = 0;
		int id,iid,uid,time,hashiid;
		
		set = stm.executeQuery("select * from "+traceName);
		while (set.next()) 
		{//title contents iid
			sum++;
			uid = set.getInt(2);
        	iid = set.getInt(3);   	
        	
        	if(iidhash.containsKey(iid))
        	{
        		if(uids.containsKey(uid))
					uids.put(uid, uids.get(uid)+1);
				else
					uids.put(uid, 1);
        	}else{
        		sparenum++;
        	} 			
		}
		
		System.out.println(traceName+" 一共有"+sum+"新闻 但其中有"+sparenum+"新闻没有出现在新闻集合");
		
		
		set.beforeFirst();
		String tempFile = "F:/data/tmp/trace.txt";
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(tempFile), "UTF-8")); 
		while(set.next())
		{
			uid = set.getInt(2);
        	iid = set.getInt(3);
        	time = set.getInt(4);
        	
			if((iidhash.containsKey(iid))&&(uids.get(uid)>1))
			{
				hashiid = iidhash.get(iid);
				bw.write(uid+"\t"+hashiid+"\t"+time);
				bw.newLine();
				
			}
		}
		set.close();
		bw.flush();
        bw.close();
        
		stm.execute("truncate "+traceName);
		String[] createIndex = {"CREATE INDEX `index1` ON "+traceName+" (uid,type);","CREATE INDEX `index2` ON "+traceName+" (iid);"};
		String[] loadsql2 = {"LOAD DATA LOCAL INFILE '"+tempFile+"' INTO TABLE "+traceName+" FIELDS TERMINATED BY '\t' (uid,iid,time);"};
		
		new dat2Db().loadwithoutindex(null,createIndex,loadsql2);
		
		
		System.out.println("开始划分训练集");
		set = stm.executeQuery("select uid,id from "+traceName+" order by uid,time desc");
		PreparedStatement pst = conn.prepareStatement("update "+traceName+" set type = 1 where id=?");
		HashMap<Integer, Integer> hm = new HashMap<Integer, Integer>(); int num = 0;
		while(set.next())
		{	
			uid = set.getInt(1);
			id = set.getInt(2);
			
			if(!hm.containsKey(uid))
			{
				hm.put(uid, 0);
				pst.setInt(1, id);
				pst.addBatch();
				num++;
			}
			
			if(num%5000 == 0)
			{
				pst.executeBatch();   
                conn.commit();   
                pst.clearBatch();
			}
		}
		pst.executeBatch();   
        conn.commit();   
        pst.clearBatch();
        set.close();
        
        System.out.println("完成");
		
		stm.close();
		conn.close();
	}
	
	
	public void initToken(String newsName,String tokenName,int dbNum,String filePath) throws SQLException, InterruptedException
	{
		Connection conn = ConnectionSource.getConnection();
		Statement stm = conn.createStatement();
		
		String sql,dbName;
		for(int i=0;i<dbNum;i++)
		{
			dbName = tokenName+"_"+i;
			sql = "CREATE TABLE `"+dbName+"` ("
					+  "`id` int(11) NOT NULL AUTO_INCREMENT,"
					+  "`token` varchar(255) NOT NULL,"
					+  "`tf` float NOT NULL,"
					+  "`tfidf` float NOT NULL,"
					+  "`iid` int(11) DEFAULT NULL,"
					+  "PRIMARY KEY (`id`)"
					+  ")ENGINE=MyISAM DEFAULT CHARSET=utf8";
			stm.execute(sql);
		}
		
		
		
		
		
		PreparedStatement pst = conn.prepareStatement("select iid,contents from "+newsName+" where id > ? limit 10000");
		//0.Prepare Work
		Semaphore semp = new Semaphore(20);
		ExecutorService threadPool = Executors.newFixedThreadPool(20);
		Jedis jedis = redisUtil.getJedis();
		jedis.flushAll();
		
		
		ResultSet set = stm.executeQuery("select count(id) from "+newsName);
		set.next();
		int total = set.getInt(1);//新闻总数
		set.close();
		
		//1.Segment Word
		HashMap<Integer, String> subset;
	
		for(int i=0;i*10000<total;i++)
		{
			pst.setInt(1, i*10000);
			set = pst.executeQuery();
			subset = new HashMap<Integer, String>();
			while (set.next()) 
			{
				subset.put(set.getInt(1), set.getString(2));//iid contents
			}
			set.close();
			semp.acquire();//等待有线程完成任务，再新建新的任务
			System.out.println("---"+i+"th thread for segment  start");
			threadPool.submit(new segmentTask1(i,subset,semp));
		}
		
		threadPool.shutdown();
		
	    
	    
	    while(!threadPool.awaitTermination(1, TimeUnit.MINUTES)){
	    	System.out.println("------------wait for segment complete");
        }
	    
	    System.out.println("------------segment work complete");
	    //2.Calculate TFIDF and write to tmpfile
	    threadPool = Executors.newFixedThreadPool(20);
	    semp = new Semaphore(20);
	    
	    
	    Set keys = jedis.keys("*");
	    Iterator it = keys.iterator();
	    List<String> subKeys = new ArrayList();//token (iid tf)
	    int filenum=0,i;
	    for(i=0;it.hasNext();i++) 
	    {
	    	subKeys.add((String) it.next());//iid tf
			if((i%5000==0)&&(i!=0))
			{
				semp.acquire();//等待有线程完成任务，再新建新的任务
				System.out.println("---"+i+"th thread for TFIDF start");
				threadPool.submit(new segmentTask2(filenum,total,subKeys,semp,dbNum,filePath));//分30个表
				subKeys = new ArrayList();
				filenum++;
			}
		}
	    
	    semp.acquire();//等待有线程完成任务，再新建新的任务
		System.out.println("---"+i+"th thread for TFIDF start");		
		threadPool.submit(new segmentTask2(filenum,total,subKeys,semp,dbNum,filePath));
		filenum++;
	    threadPool.shutdown();
		
	    
	    while(!threadPool.awaitTermination(1, TimeUnit.MINUTES)){
	    	System.out.println("------------wait for TFIDF complete");
        }
	    System.out.println("------------TFIDF work complete");
	    redisUtil.returnResource(jedis);
	    //3.Import into database
	    
	    System.out.println("------------load work start");
	    threadPool = Executors.newFixedThreadPool(dbNum);
	    for(i=0;i<dbNum;i++)
		{
	    	System.out.println("---"+i+"th thread for load file start");
			threadPool.submit(new loadTask(i,filenum,filePath,tokenName));//分30个表
		}
	    
	    /*String[] loadsql = new String[filenum];
	    for(i=0;i<filenum;i++)
		{
	    	loadsql[i] = "LOAD DATA LOCAL INFILE 'F:/data/xisihutong/tmp/token"+i+".txt' INTO TABLE "+tokenName+" FIELDS TERMINATED BY '\t'"
					+ " (token,tf,tfidf,iid);";
		}
	    String[] createIndex={"CREATE INDEX `index1` ON "+tokenName+" (iid);","CREATE INDEX `index2` ON "+tokenName+" (token);","CREATE INDEX `index3` ON "+tokenName+" (tfidf,iid);"};	
        new dat2Db().loadwithoutindex(null, createIndex, loadsql);*/
        System.out.println("------------load work complete");
		
        stm.close();
        pst.close();
		conn.close();
	}
	
	public void initNews(String newsName, String hashName, String filename, String Regex) throws SQLException
	{
		Connection conn = ConnectionSource.getConnection();
		Statement stm = conn.createStatement();
		
		String sql = "CREATE TABLE `"+newsName+"` ("
						+  "`id` int(11) NOT NULL AUTO_INCREMENT,"
						+  "`title` varchar(255) DEFAULT NULL,"
						+  "`contents` longtext,"
						+  "`iid` int(11) DEFAULT NULL,"
						+  "PRIMARY KEY (`id`)"
						+  ")ENGINE=InnoDB DEFAULT CHARSET=utf8";
		
		
		stm.execute(sql);
		
		sql = "CREATE TABLE `"+hashName+"` ("
				+  "`id` int(11) NOT NULL AUTO_INCREMENT,"
				+  "`hashiid` int(11) DEFAULT NULL,"
				+  "`iid` int(11) DEFAULT NULL,"
				+  "PRIMARY KEY (`id`)"
				+  ")ENGINE=InnoDB DEFAULT CHARSET=utf8";
		
		stm.execute(sql);
		
		try {
			insert4news(newsName,hashName,filename,Regex);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		

		stm.close();
		conn.close();
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
