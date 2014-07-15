package Common;

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

import redis.clients.jedis.Jedis;

public class ItemCF {

	
	//划分训练集
	public void init(String tableName) throws SQLException{
		Connection conn = ConnectionSource.getConnection();
		conn.setAutoCommit(false);
		PreparedStatement pst1 = conn.prepareStatement("select id from "+tableName+" where uid=? order by time desc limit 1");
		PreparedStatement pst2 = conn.prepareStatement("update "+tableName+" set type=1 where id=?");
		Statement st = conn.createStatement();
		
		ResultSet set = st.executeQuery("select distinct(uid) from "+tableName),set2;
		int uid,id,num=0;
		long startTime,endTime;
		startTime = System.currentTimeMillis();//获取当前时间
		while(set.next())
		{
			uid = set.getInt(1);
			pst1.setInt(1, uid);
			set2 = pst1.executeQuery();//select id from trace4caixin where uid=? order by time desc limit 1
			while(set2.next())
			{
				id = set2.getInt(1);
				pst2.setInt(1, id);
				pst2.addBatch();//update trace4caixin set type=1 where id=?
			}
			set2.close();
			num++;
			if(num%5000 == 0)
			{
				pst2.executeBatch();   
	            conn.commit();   
	            pst2.clearBatch();
	            endTime = System.currentTimeMillis();//获取当前时间
				System.out.println(5000+" 耗时："+(endTime-startTime)+"ms");
				startTime = endTime;
			}
		}
		pst2.executeBatch();   
        conn.commit();   
        pst2.clearBatch();
        
		set.close();
		st.close(); 
		
		
		
		pst1.close();
		pst2.close();
		conn.close();
	}
	/*
	public void add2hit1(int iid1,int iid2,float sim,ConcurrentHashMap<String, Float> hit1) 
	{
		int tmp;
		String key;
		if(iid1<iid2)
		{
			tmp = iid1;
			iid1 = iid2;
			iid2 = tmp;
		}
		key = iid1+" "+iid2;
		hit1.put(key, sim);
	}
	
	public float hit1(int iid1,int iid2,ConcurrentHashMap<String, Float> hit1) 
	{
		int tmp;
		String key;
		if(iid1<iid2)
		{
			tmp = iid1;
			iid1 = iid2;
			iid2 = tmp;
		}
		key = iid1+" "+iid2;
		if(hit1.containsKey(key))
			return hit1.get(key);
		else
			return -1;
	}
	
	public HashMap<String, Float> hit2(int iid1,ConcurrentHashMap<Integer, String> hm1) 
	{
		
		if(hm1.containsKey(iid1))
		{
			HashMap<String, Float> res = new HashMap<String, Float>();
			String tmp = hm1.get(iid1);//{token,tfidf}
			Pattern p = Pattern.compile("\\{(.*?) (.*?)\\}");
			Matcher matcher = p.matcher(tmp);

            while(matcher.find()) 
            {
            	res.put(matcher.group(1).trim(), Float.parseFloat(matcher.group(2).trim()));
            }

            
            	
			return res;
		}
			
		else
			return null;
	}*/
	
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
				tmpstring += "{"+set.getString(1).trim()+" "+set.getString(2).trim()+"}";
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
	
	/*//根据uid计算uid相似集合
	public void calEffect() throws SQLException{
		//10000个用户
		float accur,tmpaccur;
		accur = 0;
		int maxuid,minuid,uid;
		Connection conn = ConnectionSource.getConnection();
		Statement st = conn.createStatement();
		st.execute("create temporary table uids (id int(11) not null AUTO_INCREMENT,uid int(11) not null,primary key (id))  select distinct uid from trace4caixin;");//uid
		st.execute("create temporary table usersatis (id int(11) not null AUTO_INCREMENT,uid int(11) not null,satis float not null,primary key (id)) ENGINE=MEMORY");
		st.execute("create temporary table result (id int(11) not null AUTO_INCREMENT,uid int(11) not null,iid int(11) not null,primary key (id),INDEX  index4iid(iid)) ENGINE=MEMORY");
		st.execute("create temporary table uidres (id int(11) not null AUTO_INCREMENT,iid int(11) not null,sim float not null,primary key (id),INDEX  index4iid(iid)) ENGINE=MEMORY");
		
		
		PreparedStatement pst1 = conn.prepareStatement("select iid from trace4caixin where uid=? and type=0");//选择用户浏览的所有新闻
		PreparedStatement pst2 = conn.prepareStatement("select iid from news4caixin");//所有新闻
		PreparedStatement pst3 = conn.prepareStatement("insert into result values (null,?,?)");//所有新闻
		PreparedStatement pst4 = conn.prepareStatement("select iid from trace4caixin where uid=? and type=1");//选择用户浏览的所有新闻
		PreparedStatement pst5 = conn.prepareStatement("insert into uidres values (null,?,?)");//选择用户浏览的所有新闻
		
		
		ResultSet set,set2;//新闻总数

		
		set = st.executeQuery("SELECT MAX(uid),MIN(uid) FROM uids");//用户
		set.next();
		maxuid = set.getInt(1);
		minuid = set.getInt(2);
		set.close();
	
		
		ArrayList res;
		int useriid;
		
		long startTime,endTime;
		startTime = System.currentTimeMillis();//获取当前时间
		//-----
		uid = (int)(minuid+Math.random()*(maxuid-minuid+1));//随机选取uid
		set = st.executeQuery("SELECT uid FROM uids where uid>"+uid+" limit 1");//用户
		set.next();
		uid = set.getInt(1);
		set.close();
		
		pst4.setInt(1, uid);//查找uid用户的所有新闻
		set = pst4.executeQuery();//select * from trace4caixin where uid=? and type=1
		set.next();
		useriid = set.getInt(1);
		set.close();
		
		pst1.setInt(1, uid);//查找uid用户的所有新闻
		set = pst1.executeQuery();//select * from trace4caixin where uid=? and type=0
		
		Calculate cal = new Calculate();
		float sim,maxsim=0;
		int newsnum=0;
		maxsim=0;
		int num=0;
		set2 = st.executeQuery("select iid from news4caixin");
		while(set2.next())
		{
			set.beforeFirst();
			String iid1 = set2.getString(1);
			sim = 0;
			newsnum = 0;
			System.out.print("num:"+num);
			while(set.next())//uid度过的所有新闻
			{
				String iid2 = set.getString(1);
				if(!iid1.equals(iid2))
				{
					System.out.print(" iid1:"+iid1+" iid2:"+iid2);
					sim += cal.calSimilarByText("token4caixin",iid1,iid2);
					System.out.println(" sim:"+ sim);
				}
				
				newsnum++;
			}
			
			sim = sim/newsnum;//iid1和uid的相识度
			pst5.setInt(1, Integer.parseInt(iid1));
			pst5.setFloat(2, sim);
			pst5.executeUpdate();
			//pst5.addBatch();
			num++;
			if(num%5000==0)
			{
				pst5.executeBatch();   
		        conn.commit();   
		        pst5.clearBatch();
			}
			
		}
		pst5.executeBatch();   
        conn.commit();   
        pst5.clearBatch();
        
		set2.close();
		
		
		set = st.executeQuery("select distinct iid from uidres order by sim desc limit 30;");
		while(set.next())
		{
			String iid = ""+set.getInt(1);
			if(iid.equals(useriid))
			{
				
			}
			newsnum++;
		}
		
		
		 endTime = System.currentTimeMillis();//获取当前时间
		System.out.println(5000+" 耗时："+(endTime-startTime)+"ms");
		//-----
		
		
		for(int num=0;num<1000;num++)
		{
			uid = (int)(minuid+Math.random()*(maxuid-minuid+1));//随机选取uid
			
			pst4.setInt(1, uid);//查找uid用户的所有新闻
			set = pst1.executeQuery();//select * from trace4caixin where uid=? and type=0
			useriid = set.getInt(1);
			set.close();
			
			pst1.setInt(1, uid);//查找uid用户的所有新闻
			set = pst1.executeQuery();//select * from trace4caixin where uid=? and type=0
			
			
			for(int i=0;i*10000<total;i++)
			{
				pst2.setInt(1, i*10000);//select iid from iids where id > ? limit 10000
				set2 = pst2.executeQuery();//查找所有新闻
				res = calSimilar(set,set2,useriid);
				//取出res插入数据库
				set2.close();
			}
			set.close();
		}
		
		
		
		st.execute("drop table uids;");
		st.execute("drop table result;");
		st.execute("drop table usersatis;");
		st.execute("drop table uidres;");
		
		pst1.close();
		pst2.close();
		pst3.close();
		pst4.close();
		pst5.close();
		
		conn.close();
	}
	*/
	public static void main(String[] args) {
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
	private Calculate cal;
	private int usernum;
	private String traceTable;
	private String recommendFile;
	private String weightFile;
	private static Lock lock = new ReentrantLock();
	private int jhit;
	private int jhitOld;
	private FileWriter errorWriter;
	
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
		cal = new Calculate();
		jedis = redisUtil.getJedis();
		newsWeight = new ArrayList<Float>();
		jhit = 0;
		try {
			errorWriter = new FileWriter("F:/data/caixin/error/error"+id%10+".txt", true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  
	}
	
	public ArrayList<Integer> getTrace(int iid1,ConcurrentHashMap<Integer, String> hm1) 
	{	
		if(hm1.containsKey(iid1))
		{
			ArrayList<Integer> res = new ArrayList<Integer>();
			String tmp = hm1.get(iid1);//{token,tfidf}
			String[] uids = tmp.split(" ");
			for(int i=0;i<uids.length;i++)
				if(!uids[i].trim().equals(""))
					res.add(Integer.parseInt(uids[i].trim()));

	        	
			return res;
		}
			
		else
			return null;
		
	}
	
	public HashMap<String, Float> getToken(int iid1,ConcurrentHashMap<Integer, String> hm1) 
	{
		
		if(hm1.containsKey(iid1))
		{
			HashMap<String, Float> res = new HashMap<String, Float>();
			String tmp = hm1.get(iid1);//{token,tfidf}
			Pattern p = Pattern.compile("\\{(.*?) (.*?)\\}");
			Matcher matcher = p.matcher(tmp);

            while(matcher.find()) 
            {
            	res.put(matcher.group(1).trim(), Float.parseFloat(matcher.group(2).trim()));
            }

            
            	
			return res;
		}
			
		else
			return null;
	}
	
	public float calSim(int iid2,ResultSet lookedNews) throws SQLException, IOException
	{
		float finalsim = 0;
		if(type == 0)
			finalsim = calSimbyTrace(iid2,lookedNews);
		else
			finalsim = calSimbyText(iid2,lookedNews);
		return finalsim;
	}
	
	public float calSimbyTrace(int iid2,ResultSet lookedNews) throws SQLException, IOException
	{
		float finalsim = 0,sim;
		int   num = 0,iid1 = 0;
		String key,tmp;
		ArrayList<Integer> hm1 = null,hm2 = null;	
		
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
					errorWriter.write("iid1 = "+iid1+" iid2 = "+iid2+" hm1 or hm2 userids is null");
					sim = 0;
				}
				else
				{
					sim = cal.calSimilarByTrace(hm1, hm2);
				}
				
				jedis.setnx(key, ""+sim);
			}
			
			if(Float.isNaN(sim))
			{
				errorWriter.write("iid1 = "+iid1+" iid2 = "+iid2+" sim is NAN");			
			}	
			
			finalsim += sim;				
		}//while
	
		finalsim = finalsim/num;	
		return finalsim;
	}
	
	public float calSimbyText(int iid2,ResultSet lookedNews) throws SQLException, IOException
	{
		float finalsim = 0,sim;
		int   num = 0,iid1 = 0;
		String key,tmp;
		HashMap<String, Float> hm1,hm2;
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
					errorWriter.write("iid1 = "+iid1+" iid2 = "+iid2+" hm1 or hm2 token is null");
					sim = 0; 
				}else
				{
					sim = cal.calSimilarByText(hm1, hm2);
				}
					
				
				jedis.setnx(key, ""+sim);
			}
			
			if(Float.isNaN(sim))
			{
				errorWriter.write("iid1 = "+iid1+" iid2 = "+iid2+" sim is NAN");			
			}	
			
			finalsim += sim;				
		}//while
	
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
	
	public Integer call() throws SQLException, IOException{	
		long startTime,endTime,midTime;		
		startTime = System.currentTimeMillis();//获取当前时间
		Connection conn = ConnectionSource.getConnection();
		PreparedStatement pst1 = conn.prepareStatement("select iid from "+traceTable+" where uid=? and type=0");//选择用户浏览的所有新闻
		PreparedStatement pst2 = conn.prepareStatement("select count(iid) from "+traceTable+" where uid=? and type=0");//选择用户浏览的所有新闻
		ResultSet set; int newsnum = 0;
		int iid2; float finalsim = 0;
		String recommendlist="",weightlist="";
		
		jhitOld = 0;
		for(int i=0;i<usernum;i++)//for all user
		{
			if(uids[i]==-1)
				break;
			
			midTime = System.currentTimeMillis();//获取当前时间
			initRecomiids();//init recommend list for new user
			
			
			pst2.setInt(1, uids[i]);
			set = pst2.executeQuery();//get all the news which uid has look		
			set.next();
			newsnum = set.getInt(1);
			set.close();
			
			pst1.setInt(1, uids[i]);
			set = pst1.executeQuery();//get all the news which uid has look			
			
			System.out.println(id+"th thread "+i+"th user id = "+uids[i]+" num = "+newsnum);

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
		}
		//print recommend list
		
		lock.lock();
		FileWriter writer1 = new FileWriter(recommendFile, true);  
		FileWriter writer2 = new FileWriter(weightFile, true);  
		try {
			 writer1.write(recommendlist);  
			 writer2.write(weightlist);  
		}
		finally {
		  writer1.close();
		  writer2.close();
		  lock.unlock();
		}
		
		pst1.close();
        conn.close();
        errorWriter.close();

		redisUtil.returnResource(jedis);
        
		endTime = System.currentTimeMillis();
		System.out.println(id+"th thread cost time = "+(endTime-startTime)+"ms");
		semp.release();
		return 1;
		    
	 }
}



