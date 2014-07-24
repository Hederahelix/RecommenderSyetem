package test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import net.sf.javaml.clustering.Clusterer;
import net.sf.javaml.clustering.KMeans;
import net.sf.javaml.core.Dataset;
import net.sf.javaml.core.DefaultDataset;
import net.sf.javaml.core.Instance;
import net.sf.javaml.core.SparseInstance;

import org.apache.log4j.Logger;

import Common.ConnectionSource;
import redis.clients.jedis.Jedis;

public class test {
	
	public static String escape(String sql_str){ 
		sql_str = sql_str.replaceAll("\\\\", "\\\\\\\\");
		sql_str = sql_str.replaceAll("\"", "\\\\\"");
		sql_str = sql_str.replaceAll("'", "\\\\'");
		sql_str = sql_str.replaceAll("%", "\\\\%");
		sql_str = sql_str.replaceAll("_", "\\\\_");
	    return sql_str; 
	}

	public static void main(String[] args) throws InterruptedException, SQLException {
		// TODO Auto-generated method stub
		/*String subKeys[] = new String[1000];
		for(int i=0;i<5;i++)
			subKeys[i] = " ";
		
		System.out.print(subKeys.length);
		
		String tmp="201620949 0.0055350554";
		System.out.println(tmp.split(" ")[0]+"   "+tmp.split(" ")[1]);
		int iid = Integer.parseInt(tmp.split(" ")[0]);
		float tf =  Float.parseFloat(tmp.split(" ")[1]);
		System.out.println(iid+"   "+tf);
		
		String usedBytes = "30000000";
		int size = usedBytes.length();
		if(size>8)
		{
			String usedGBytes = usedBytes.substring(0,size - 9);
			usedGBytes = usedGBytes+"."+usedBytes.charAt(size- 9);
			float j = Float.parseFloat(usedGBytes);
			System.out.println(j);
		}
		
		
		Jedis jedis = redisUtil.getJedis();	
		jedis.flushAll();
		String token = "number";
		for(int i=0;i<10;i++)
			jedis.append(token, " "+i+" "+i);
		
		String[] res = jedis.get(token).split(" ");
    	float count = (res.length-1)/2;//jedis.llen(token);
    	
    	System.out.println("count = "+count);
    	int iid; float tf;
    	for(int i=1;i<res.length;i+=2)//跳过第一个空格
    	{
    		iid = Integer.parseInt(res[i]);
    		tf = Float.parseFloat(res[i+1]);
    		
    		System.out.println(token+"\t"+tf+"\t"+iid);
    	}
		*/
		/*ExecutorService threadPool = Executors.newFixedThreadPool(20);
		
		for(int i=0;i<100;i++)
		{
			threadPool.submit(new segmentTask1(i));
		}
		
		threadPool.shutdown();
	    System.out.println("phase 2");
	    
	    while(!threadPool.awaitTermination(1, TimeUnit.MINUTES)){
	    	System.out.println("phase 2");
        }*/
		//System.out.println(escape("'_\"\\%"));
		
		/*Connection conn = ConnectionSource.getConnection();
		Statement stmt = conn.createStatement();
		String tablename,sql = "CREATE TABLE IF NOT EXISTS token4xisihutong_all(`id` int(11) NOT NULL AUTO_INCREMENT,"
				+ "`token` varchar(255) NOT NULL,"
				+ "`tf` float NOT NULL,"
				+ "`tfidf` float NOT NULL,"
				+ "`iid` int(11) DEFAULT NULL,"
				+ "PRIMARY KEY (`id`),"
				+ "KEY `index1` (`iid`),"
				+ "KEY `index2` (`token`),"
				+ "KEY `index3` (`tfidf`,`iid`)"
				+ ") ENGINE=MERGE UNION=(";
		Common.dat2Db db = new Common.dat2Db();
		
		for(int i=0;i<100;i++)
		{	
			if(i==0)
				sql += "token4xisihutong_"+i;
			else
				sql += ","+"token4xisihutong_"+i;
			
		}
		sql+= ")";
		stmt.execute(sql);	
		
		stmt.close();
		conn.close();*/
		
		
		
		Connection conn = ConnectionSource.getConnection();
		PreparedStatement pst = conn.prepareStatement("SELECT DISTINCT token FROM token4xisihutong_all WHERE iid = ? ORDER BY tfidf DESC LIMIT 30");	
		HashMap<String, Integer> tokenNum = new HashMap<String, Integer>();
		
		Statement st = conn.createStatement();
		ResultSet iids = st.executeQuery("select distinct iid from trace4xisihutong"),set;
		String token; int tokenid = 0,tokennum = 0;
		
		
		while(iids.next())
		{
			int iid = iids.getInt(1);	
			pst.setInt(1, iid);
			set =pst.executeQuery();
			while (set.next()) 
			{
				token = set.getString(1).trim();
				if(!tokenNum.containsKey(token))
					tokenNum.put(token, tokennum++);
			}
			set.close();
		}
		
		iids.beforeFirst();
		Dataset dataset = new DefaultDataset();
		long startTime = System.currentTimeMillis();//获取当前时间
		while(iids.next())
		{
			Instance instance= new SparseInstance(tokenid);
			int iid = iids.getInt(1);	
			pst.setInt(1, iid);
			set =pst.executeQuery();
			while (set.next()) 
			{
				token = set.getString(1).trim();
				tokenid = tokenNum.get(token);
				instance.put(tokenid, 1.0);
			}
			set.close();
			dataset.add(instance);
		}
		
	    int k = 3000;
	        
	    Clusterer km = new KMeans(k); 
	    Dataset[] clusters = km.cluster(dataset);
	    long endTime = System.currentTimeMillis();
		System.out.println("insert4news 程序运行时间："+(endTime-startTime)/60000+"min");
	   
		
	}
		
		

}

class segmentTask1 implements Callable<Integer>{
	private int id;
	public  static Logger x = Logger.getLogger(segmentTask1.class);
	
	public segmentTask1(int id){
		this.id = id;
		
	}
	
	//set.getInt(1),set.getString(2)
	public Integer call(){
		System.out.println(id+"th start");	
		Connection conn = null;
		PreparedStatement pst = null;
		try {
			conn = ConnectionSource.getConnection();
			pst = conn.prepareStatement("update test set tfidf=? where id=?");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
		for(int i=0;i<5;i++)
		{
			
			try {
				Thread.sleep(10000);
				pst.setInt(1, i);
				pst.setInt(2, i);
				pst.executeQuery();
			} catch (SQLException | InterruptedException e) {
				// TODO Auto-generated catch block
				x.error("error ", e);
			}
			
			
		}
		System.out.println(id+"th complete");	
		try {
			pst.close();
			conn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		return 1;
		    
	 }
}
