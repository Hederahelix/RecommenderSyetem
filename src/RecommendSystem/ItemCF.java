package RecommendSystem;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;

public class ItemCF {
	
	public void startIcfex(String traceTable,String[] tokenTable,String recommendFile,String weightFile,int itemNum) throws SQLException, InterruptedException{
		Connection conn = ConnectionSource.getConnection();
		Statement st = conn.createStatement();
		//uid
		int userNum = 100; String dbName;
		st.execute("create temporary table uids (id int(11) not null AUTO_INCREMENT,uid int(11) not null,primary key (id))  select distinct uid from "+traceTable);//uid
		PreparedStatement pst1 = conn.prepareStatement("select uid from uids where id>? limit "+userNum);//选择用户浏览的所有新闻
		

		int dbNum = tokenTable.length;
		PreparedStatement[] pst2 = new PreparedStatement[dbNum];			
		for(int i=0;i<dbNum;i++)
		{
			pst2[i] = conn.prepareStatement("SELECT DISTINCT token,tfidf FROM "+tokenTable[i]+" WHERE iid = ? ORDER BY tfidf DESC LIMIT 30");
		}	
				
		ConcurrentHashMap<Integer, String> hit2 = new ConcurrentHashMap<Integer, String>(); //iid {token tfidf}
		ConcurrentHashMap<Integer, Integer> iids = new ConcurrentHashMap<Integer, Integer>(); //iids
		
		int uid,iid,iidnum =0,total; ResultSet set; String tmpstring; ResultSet userset;int[] uids;
		
		//1.get all news iid and put them into iids(HashMap)
		//2.get all tokens and put them into hit2(HashMap)
		ResultSet newset = st.executeQuery("select distinct iid from "+traceTable);//只比较测试集合状态集出现过的新闻
		while(newset.next())
		{
			iid = newset.getInt(1);
			iids.put(iidnum++, iid);//加载iid
			
			pst2[iid%dbNum].setString(1, ""+iid);
			set = pst2[iid%dbNum].executeQuery();
			//加载分词
			tmpstring = ""; 
			while (set.next()) 
			{
				tmpstring += "{"+set.getString(1).trim()+" "+set.getString(2).trim()+"}";//{token tfidf}
			}
			
			hit2.put(iid, tmpstring);
			set.close();
		}
		newset.close();
		
		set = st.executeQuery("select count(id) from uids");
		set.next();
		total = set.getInt(1);//新闻总数
		set.close();
		for(int i=0;i<dbNum;i++)
		{
			pst2[i].close();
		}	
		
		System.out.println("准备阶段结束");
		
		Semaphore semp = new Semaphore(20);
		ExecutorService threadPool = Executors.newFixedThreadPool(20);
		
		int j;
		
		for(int i=0;i*userNum<total;i++)
		{
			pst1.setInt(1, i*userNum);
			userset = pst1.executeQuery();//select uid from uids where uid>? limit 1000
			uids = new int[userNum];
			
			for(j=0;userset.next();j++)
			{
				uids[j] = userset.getInt(1);
			}
			if(j<userNum-1)
				uids[j]=-1;
			
			userset.close();
			
			semp.acquire();//等待有线程完成任务，再新建新的任务
			System.out.println("--------------------------------"+i+"th thread start");
			threadPool.submit(new simTask(i,1,uids,hit2,iids,iidnum,semp,itemNum,userNum,traceTable,recommendFile,weightFile));
			
			if(i%10==0)
				System.out.println("--------------------------------"+i);
		}
		

		threadPool.shutdown();
		
	    System.out.println("phase 2");
	    
	    while(!threadPool.awaitTermination(1, TimeUnit.MINUTES)){
	    	
        }
	    
	    //redisUtil.destory();
	    st.execute("drop table uids;");//uid
	    pst1.close();
	    st.close();
	    conn.close();
	    
	}	
	
	public void startIcf(String traceTable,String[] tokenTable,String recommendFile,String weightFile,int itemNum) throws SQLException, InterruptedException{
		Connection conn = ConnectionSource.getConnection();
		Statement st = conn.createStatement();
		//uid
		st.execute("create temporary table uids (id int(11) not null AUTO_INCREMENT,uid int(11) not null,primary key (id))  select distinct uid from "+traceTable);//uid
				
		int userNum = 100; String dbName;
		PreparedStatement pst1 = conn.prepareStatement("select uid from uids where id>? limit "+userNum);//选择用户浏览的所有新闻
		int dbNum = tokenTable.length;
		PreparedStatement[] pst2 = new PreparedStatement[dbNum];
				
		for(int i=0;i<dbNum;i++)
		{
			pst2[i] = conn.prepareStatement("SELECT DISTINCT token,tfidf FROM "+tokenTable[i]+" WHERE iid = ? ORDER BY tfidf DESC LIMIT 30");
		}	
				
		
		ConcurrentHashMap<Integer, String> hit2 = new ConcurrentHashMap<Integer, String>(); //iid {token tfidf}
		ConcurrentHashMap<Integer, Integer> iids = new ConcurrentHashMap<Integer, Integer>(); //iids
		
		int uid,iid,iidnum =0,total; ResultSet set; String tmpstring; ResultSet userset;int[] uids;
		
		//1.get all news iid and put them into iids(HashMap)
		//2.get all tokens and put them into hit2(HashMap)
		//ResultSet newset = st.executeQuery("select distinct iid from "+newsTable);
		ResultSet newset = st.executeQuery("select distinct iid from "+traceTable);
		while(newset.next())
		{
			iid = newset.getInt(1);
			iids.put(iidnum++, iid);//加载iid
			
			pst2[iid%dbNum].setString(1, ""+iid);
			set = pst2[iid%dbNum].executeQuery();
			//加载分词
			tmpstring = ""; 
			while (set.next()) 
			{
				tmpstring += "{"+set.getString(1).trim()+" "+set.getString(2).trim()+"}";//{token tfidf}
			}
			
			hit2.put(iid, tmpstring);
			set.close();
		}
		newset.close();
		
		set = st.executeQuery("select count(id) from uids");
		set.next();
		total = set.getInt(1);//新闻总数
		set.close();
		for(int i=0;i<dbNum;i++)
		{
			pst2[i].close();
		}	
		
		System.out.println("准备阶段结束");
		
		Semaphore semp = new Semaphore(20);
		ExecutorService threadPool = Executors.newFixedThreadPool(20);
		
		int j;
		
		for(int i=0;i*userNum<total;i++)
		{
			pst1.setInt(1, i*userNum);
			userset = pst1.executeQuery();//select uid from uids where uid>? limit 1000
			uids = new int[userNum];
			
			for(j=0;userset.next();j++)
			{
				uids[j] = userset.getInt(1);
			}
			if(j<userNum-1)
				uids[j]=-1;
			
			userset.close();
			
			semp.acquire();//等待有线程完成任务，再新建新的任务
			System.out.println("--------------------------------"+i+"th thread start");
			threadPool.submit(new simTask(i,1,uids,hit2,iids,iidnum,semp,itemNum,userNum,traceTable,recommendFile,weightFile));
			
			if(i%10==0)
				System.out.println("--------------------------------"+i);
		}
		

		threadPool.shutdown();
		
	    System.out.println("phase 2");
	    
	    while(!threadPool.awaitTermination(1, TimeUnit.MINUTES)){
	    	
        }
	    
	    //redisUtil.destory();
	    st.execute("drop table uids;");//uid
	    pst1.close();
	    st.close();
	    conn.close();
	    
	}	

	public void calMultiEffect(String traceTable,String tokenTable,String recommendFile,String weightFile,int itemNum) throws SQLException, InterruptedException{
		Connection conn = ConnectionSource.getConnection();
		Statement st = conn.createStatement();
		//uid
		st.execute("create temporary table uids (id int(11) not null AUTO_INCREMENT,uid int(11) not null,primary key (id))  select distinct uid from "+traceTable);//uid
				
		int userNum = 100;
		PreparedStatement pst1 = conn.prepareStatement("select uid from uids where id>? limit "+userNum);//选择用户浏览的所有新闻
		PreparedStatement pst2 = conn.prepareStatement("SELECT DISTINCT token,tfidf FROM "+tokenTable+" WHERE iid = ? ORDER BY tfidf DESC LIMIT 30");
		
		ConcurrentHashMap<Integer, String> hit2 = new ConcurrentHashMap<Integer, String>(); //iid token
		ConcurrentHashMap<Integer, Integer> iids = new ConcurrentHashMap<Integer, Integer>(); //iids
		
		int uid,iid,iidnum =0,total; ResultSet set; String tmpstring; ResultSet userset;int[] uids;
		
		//1.get all news iid and put them into iids(HashMap)
		//2.get all tokens and put them into hit2(HashMap)
		ResultSet newset = st.executeQuery("select distinct iid from "+traceTable);
		while(newset.next())
		{
			iid = newset.getInt(1);
			iids.put(iidnum++, iid);//加载iid
			
			pst2.setString(1, ""+iid);
			set = pst2.executeQuery();
			//加载分词
			tmpstring = ""; 
			while (set.next()) 
			{
				tmpstring += "{"+set.getString(1).trim()+" "+set.getString(2).trim()+"}";//{token tfidf}
			}
			
			hit2.put(iid, tmpstring);
			set.close();
		}
		newset.close();
		
		set = st.executeQuery("select count(id) from uids");
		set.next();
		total = set.getInt(1);//新闻总数
		set.close();
		
		
		System.out.println("准备阶段结束");
		
		Semaphore semp = new Semaphore(20);
		ExecutorService threadPool = Executors.newFixedThreadPool(20);
		
		int j;
		
		for(int i=0;i*userNum<total;i++)
		{
			pst1.setInt(1, i*userNum);
			userset = pst1.executeQuery();//select uid from uids where uid>? limit 1000
			uids = new int[userNum];
			
			for(j=0;userset.next();j++)
			{
				uids[j] = userset.getInt(1);
			}
			if(j<userNum-1)
				uids[j]=-1;
			
			userset.close();
			
			semp.acquire();//等待有线程完成任务，再新建新的任务
			System.out.println("--------------------------------"+i+"th thread start");
			threadPool.submit(new simTask(i,1,uids,hit2,iids,iidnum,semp,itemNum,userNum,traceTable,recommendFile,weightFile));
			
			if(i%10==0)
				System.out.println("--------------------------------"+i);
		}
		

		threadPool.shutdown();
		
	    System.out.println("phase 2");
	    
	    while(!threadPool.awaitTermination(1, TimeUnit.MINUTES)){
	    	
        }
	    
	    //redisUtil.destory();
	    st.execute("drop table uids;");//uid
	    pst1.close();
	    pst2.close();
	    st.close();
	    conn.close();
	    
	}
	
	
	public void calMultiEffectByTrace(String traceTable,String recommendFile,String weightFile,int itemNum) throws SQLException, InterruptedException{
		Connection conn = ConnectionSource.getConnection();
		Statement st = conn.createStatement();
		//uid
		st.execute("create temporary table uids (id int(11) not null AUTO_INCREMENT,uid int(11) not null,primary key (id))  select distinct uid from "+traceTable);//uid
				
		int userNum = 100;
		PreparedStatement pst1 = conn.prepareStatement("select uid from uids where id>? limit "+userNum);//选择用户浏览的所有新闻
		PreparedStatement pst2 = conn.prepareStatement("SELECT DISTINCT uid FROM "+traceTable+" WHERE iid = ? and type = 0");
		
		ConcurrentHashMap<Integer, Integer> iids = new ConcurrentHashMap<Integer, Integer>(); //iids
		ConcurrentHashMap<Integer, String>  hit2 = new ConcurrentHashMap<Integer, String>(); //iid uids
		
		int uid,iid,iidnum =0,total; ResultSet set; String tmpstring; ResultSet userset;int[] uids;
		
		//1.get all news iid and put them into iids(HashMap)
		//2.get all tokens and put them into hit2(HashMap)
		ResultSet newset = st.executeQuery("select distinct iid from "+traceTable);
		while(newset.next())
		{
			iid = newset.getInt(1);
			iids.put(iidnum++, iid);//加载iid
			
			pst2.setInt(1, iid);
			set = pst2.executeQuery();
			//加载分词
			tmpstring = ""; 
			while (set.next()) 
			{
				tmpstring += set.getString(1).trim()+" ";
			}
			
			hit2.put(iid, tmpstring.trim());//iid uid
			set.close();
		}
		newset.close();
		
		set = st.executeQuery("select count(id) from uids");
		set.next();
		total = set.getInt(1);//新闻总数
		set.close();
		
		
		System.out.println("准备阶段结束");
		
		Semaphore semp = new Semaphore(1);
		ExecutorService threadPool = Executors.newFixedThreadPool(20);
		
		int j;
		
		for(int i=45;i*userNum<total;i++)
		{
			pst1.setInt(1, i*userNum);
			userset = pst1.executeQuery();//select uid from uids where uid>? limit 1000
			uids = new int[userNum];
			
			for(j=0;userset.next();j++)
			{
				uids[j] = userset.getInt(1);
			}
			if(j<userNum-1)
				uids[j]=-1;
			
			userset.close();
			
			semp.acquire();//等待有线程完成任务，再新建新的任务
			System.out.println("--------------------------------"+i+"th thread start");
			threadPool.submit(new simTask(i,0,uids,hit2,iids,iidnum,semp,itemNum,userNum,traceTable,recommendFile,weightFile));
			
			if(i%10==0)
				System.out.println("--------------------------------"+i);
		}
		

		threadPool.shutdown();
		
	    System.out.println("phase 2");
	    
	    while(!threadPool.awaitTermination(1, TimeUnit.MINUTES)){
	    	
        }
	    
	    //redisUtil.destory();
	    st.execute("drop table uids;");//uid
	    pst1.close();
	    pst2.close();
	    st.close();
	    conn.close();
	    
	}
	
}

class simTask implements Callable<Integer>{

	private int id;
	private int[] uids;
	private int type;
	private int iidnum;
	private int itemNum;
	private ConcurrentHashMap<Integer, String> hit2;
	private ConcurrentHashMap<Integer, Integer> iids;
	private Semaphore semp;
	private ItemCF iCF;
	private int usernum;
	private String traceTable;
	private String recommendFile;
	private String weightFile;
	private static Lock lock = new ReentrantLock();
	private int jhit;
	private int jhitOld;
	private Logger logger = Logger.getLogger(simTask.class);
	
	private int recomiids[];
	private float sims[];
	private int minid;
	private float minsim;
	private Jedis jedis;
	private ArrayList<Float> newsWeight;
	
	public simTask(int id,int type,int[] uids,ConcurrentHashMap<Integer, String> hit2,ConcurrentHashMap<Integer, Integer> iids,int iidnum,
			Semaphore semp,int itemNum,int usernum,String traceTable,String recommendFile,String weightFile){
		this.id = id;
		this.type = type;
		this.uids = uids;
		this.hit2 = hit2;
		this.iids = iids;
		this.iidnum = iidnum;
		this.semp = semp;
		this.itemNum = itemNum;
		this.usernum = usernum;
		this.traceTable = traceTable;
		this.recommendFile = recommendFile;
		this.weightFile = weightFile;
		iCF = new ItemCF();
		jedis = redisUtil.getJedis();
		newsWeight = new ArrayList<Float>();
		jhit = 0;
	}
	
	
	public float calSim(int iid2,ResultSet lookedNews)
	{
		float finalsim = 0;
		if(type == 0)
			finalsim = calSimbyTrace(iid2,lookedNews);
		else
			finalsim = calSimbyText(iid2,lookedNews);
		return finalsim;
	}
	
///////////////基于行为
	public ArrayList<Integer> getTrace(int iid1,ConcurrentHashMap<Integer, String> hm1) 
	{	
		if(hm1.containsKey(iid1))
		{
			ArrayList<Integer> res = new ArrayList<Integer>();
			String tmp = hm1.get(iid1);//{token tfidf}
			String[] uids = tmp.split(" ");
			for(int i=0;i<uids.length;i++)
				if(!uids[i].trim().equals(""))
					res.add(Integer.parseInt(uids[i].trim()));

	        	
			return res;
		}
			
		else
			return null;
		
	}
	
	public float calSimilarByTrace(ArrayList<Integer> hm1,ArrayList<Integer> hm2)
	{
		
		float similar = 0;
		int tmp1,tmp2,tmp3 = 0;
		Iterator iter;
		
		//计算两个文章的相似度
		tmp1 = hm1.size();
		tmp2 = hm2.size();
		
		tmp3 = 0;
		iter = hm1.iterator();
		while (iter.hasNext()) 
		{
			if(hm2.contains(iter.next()))
			{
				tmp3++;
			}
		}
		
		similar = tmp2/(tmp1+tmp2-tmp3);

		return similar;
	}
	
	public float calSimbyTrace(int iid2,ResultSet lookedNews)
	{
		float finalsim = 0,sim;
		int   num = 0,iid1 = 0;
		String key,tmp;
		ArrayList<Integer> hm1 = null,hm2 = null;	
		
		try {
			while(lookedNews.next())//all the news which uid has look
			{
				
				iid1 = lookedNews.getInt(1);
				num++;
				
				if(iid1 == iid2)
				{
					finalsim += 1;
					continue;
				}
				
				if(iid1<iid2)	
					key = iid2+" "+iid1;
				else
					key = iid1+" "+iid2;
				
				if((tmp = jedis.get(key))!=null)
				{
					sim = Float.parseFloat(tmp);
					jhit++;
				}else
				{
					
					hm1 = getTrace(iid1,hit2);//the users who looked the news iid1
					hm2 = getTrace(iid2,hit2);//the users who looked the news iid2
					if(hm1==null||hm2==null)
					{
						logger.error("iid1 = "+iid1+" iid2 = "+iid2+" hm1 or hm2 userids is null");
						sim = 0;
					}
					else
					{
						sim = calSimilarByTrace(hm1, hm2);
					}
					
					jedis.setnx(key, ""+sim);
				}
				
				if(Float.isNaN(sim))
				{
					logger.error("iid1 = "+iid1+" iid2 = "+iid2+" sim is NAN");		
				}	
				
				finalsim += sim;				
			}
		} catch (NumberFormatException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}//while
	
		finalsim = finalsim/num;	
		return finalsim;
	}

	
///////////////基于文本	
	public HashMap<String, Float> getToken(int iid1,ConcurrentHashMap<Integer, String> hm1) 
	{
		
		if(hm1.containsKey(iid1))
		{
			HashMap<String, Float> res = new HashMap<String, Float>();
			String tmp = hm1.get(iid1);//{token tfidf}
			Pattern p = Pattern.compile("\\{(.*?) (.*?)\\}");
			Matcher matcher = p.matcher(tmp);

            while(matcher.find()) 
            {
            	res.put(matcher.group(1).trim(), Float.parseFloat(matcher.group(2).trim()));
            }

            
            	
			return res;
		}
			
		else
		{
			logger.error("没有"+iid1+"的分词");
			return null;
		}
			
	}
	
	public float calSimilarByText(HashMap<String, Float> hm1,HashMap<String, Float> hm2)
	{
		
		float similar = 0,tmp1,tmp2,tmp3;
		Iterator iter;
		Map.Entry<String, Float> entry;
		
		//计算两个文章的相似度
		tmp1 = tmp2 = tmp3 = 0;
		iter = hm1.entrySet().iterator();
		while (iter.hasNext()) 
		{
			entry = (Map.Entry<String, Float>) iter.next();
			String key = entry.getKey();
			tmp2 += Math.pow(entry.getValue(),2);//x1*x1
			if(hm2.containsKey(key))
			{
				tmp1 += entry.getValue()*(hm2.get(key));//x1*x2
				
			}
		}
		if(tmp2 == 0)
			return 0;
		
		tmp2 = (float) Math.sqrt(tmp2);
		iter = hm2.entrySet().iterator();
		while (iter.hasNext()) 
		{
			entry = (Map.Entry<String, Float>) iter.next();
			tmp3 += Math.pow(entry.getValue(),2);//x2*x2
		}
		if(tmp3 == 0)
			return 0;
		
		tmp3 = (float) Math.sqrt(tmp3);
		
		if(tmp2!=0&&tmp3!=0)
			similar = tmp1/(tmp2*tmp3);
		else
			similar = 0;
	//	System.out.println(similar);
		return similar;
	}
	
	public float calSimbyText(int iid2,ResultSet lookedNews)
	{
		float finalsim = 0,sim;
		int   num = 0,iid1 = 0;
		String key,tmp;
		HashMap<String, Float> hm1,hm2;
		
		try {
			while(lookedNews.next())//all the news which uid has look
			{
				
				iid1 = lookedNews.getInt(1);
				num++;
				
				if(iid1 == iid2)
				{
					finalsim += 1;
					continue;
				}
				
				if(iid1<iid2)	
					key = iid2+" "+iid1;
				else
					key = iid1+" "+iid2;
				
				if((tmp = jedis.get(key))!=null)
				{
					sim = Float.parseFloat(tmp);
					jhit++;
				}else
				{
					hm1 = getToken(iid1,hit2);
					hm2 = getToken(iid2,hit2);		
					if(hm1==null||hm2==null)
					{
						logger.error("iid1 = "+iid1+" iid2 = "+iid2+" hm1 or hm2 token is null");	
						sim = 0; 
					}else
					{
						sim = calSimilarByText(hm1, hm2);
					}
						
					
					jedis.setnx(key, ""+sim);
				}
				
				if(Float.isNaN(sim))
				{
					logger.error("iid1 = "+iid1+" iid2 = "+iid2+" sim is NAN");		
				}	
				
				finalsim += sim;				
			}//while
		} catch (NumberFormatException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error("line613 报错  ", e);
		}
		
		
	
		finalsim = finalsim/num;	
		return finalsim;
	}


	public void initRecomiids()
	{
		minsim=-1;
		minid=0;
		recomiids = new int[itemNum]; 
		sims = new float[itemNum];
		
		for(int x=0;x<itemNum;x++)
			sims[x] = -1;
	}
	
	public void addRecomiids(int iid,float sim)
	{
		if(sim>minsim)
		{
			recomiids[minid] = iid;
			sims[minid] = sim;
			minsim = sim;
			
			for(int k=0;k<itemNum;k++)
			{
				if(sims[k]<minsim)
				{
					minsim = sims[k];
					minid = k;
				}
			}
		}//if
	}
	
	public String printRecomiids(int uid)
	{
		String recommendlist="";
		int temp1;
		float temp2;
		//排序
		for (int i=0;i<itemNum;i++) 
		{  
			for (int j=i+1;j<itemNum;j++) 
			{  
			    if (sims[i]<sims[j]) 
			    {  
				     temp1 = recomiids[i];  
				     recomiids[i] = recomiids[j];  
				     recomiids[j] = temp1;  
				     
				     temp2 = sims[i];  
				     sims[i] = sims[j];  
				     sims[j] = temp2; 
			    }  	   
			}  
		}
		
		for(int k=0;k<itemNum;k++)
		{
			recommendlist += uid+"\t"+recomiids[k]+"\t"+sims[k]+"\n";
		}
		return recommendlist;
	}
	
	public String printWeights(int uid)
	{
		String weightlist="";
		Iterator<Float> it = newsWeight.iterator();
		int num=0;
		
		while(it.hasNext())
		{
			weightlist += uid+"\t"+iids.get(num++)+"\t"+it.next()+"\n";
		}
		
		return weightlist;
	}
	
	public Integer call(){	
		long startTime,endTime,midTime;		
		startTime = System.currentTimeMillis();//获取当前时间
		Connection conn = null;
		PreparedStatement pst1 = null,pst2 = null;
		try{
			conn = ConnectionSource.getConnection();
			pst1 = conn.prepareStatement("select iid from "+traceTable+" where uid=? and type=0");//选择用户浏览的所有新闻
			pst2 = conn.prepareStatement("select count(iid) from "+traceTable+" where uid=? and type=0");//选择用户浏览的所有新闻
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error("line686  报错  ", e);
		}
		
		ResultSet set = null; int newsnum = 0;
		int iid2; float finalsim = 0;
		String recommendlist="",weightlist="";
		
		jhitOld = 0;
		for(int i=0;i<usernum;i++)//for all user
		{
			if(uids[i]==-1)
				break;
			
			midTime = System.currentTimeMillis();//获取当前时间
			initRecomiids();//init recommend list for new user
			
			try{
				pst2.setInt(1, uids[i]);
				set = pst2.executeQuery();//get all the news which uid has look		
				set.next();
				newsnum = set.getInt(1);
				set.close();
				
				pst1.setInt(1, uids[i]);
				set = pst1.executeQuery();//get all the news which uid has look			
			
/*				System.out.println(id+"th thread "+i+"th user id = "+uids[i]+" num = "+newsnum);*/
	
				for(int j=0;j<iidnum;j++)//all the news
				{
					iid2 = iids.get(j); 						
					finalsim = calSim(iid2,set);
					set.beforeFirst();
					newsWeight.add(finalsim);
					addRecomiids(iid2,finalsim);			
				}//所有新闻
			
				endTime = System.currentTimeMillis();
				System.out.println(id+"th thread "+i+"th user rows num = "+newsnum+" rows allnews = "+iidnum*newsnum+" hit = "
						+((float)(jhit-jhitOld))/(iidnum*newsnum)+" cost time = "+(endTime-midTime)+"ms");
				
				midTime = endTime;jhitOld = jhit;
				
				recommendlist += printRecomiids(uids[i]);
				weightlist += printWeights(uids[i]);
				
				newsWeight.clear();
				set.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				logger.error("line686  报错  uid="+uids[i], e);
			}
		}
		//print recommend list
		
		lock.lock();
		FileWriter writer1 = null,writer2 = null;
		try {
			 writer1 = new FileWriter(recommendFile, true);  
			 writer2 = new FileWriter(weightFile, true);  
			 writer1.write(recommendlist);  
			 writer2.write(weightlist);  
		} catch (IOException e) {
			// TODO Auto-generated catch block
			logger.error("line763 报错 ", e);
			e.printStackTrace();
		}
		finally {
		  try {
			writer1.close();
			writer2.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		  
		  lock.unlock();
		}
		
		try {
			pst1.close();
			pst2.close();
			conn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
       

		redisUtil.returnResource(jedis);
        
		endTime = System.currentTimeMillis();
		System.out.println(id+"th thread cost time = "+(endTime-startTime)+"ms");
		semp.release();
		return 1;
		    
	 }
}



