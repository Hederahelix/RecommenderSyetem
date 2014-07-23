package test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import Common.ConnectionSource;
import redis.clients.jedis.Jedis;

public class test {

	public static void main(String[] args) throws InterruptedException {
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
		ExecutorService threadPool = Executors.newFixedThreadPool(20);
		
		for(int i=0;i<100;i++)
		{
			threadPool.submit(new segmentTask1(i));
		}
		
		threadPool.shutdown();
	    System.out.println("phase 2");
	    
	    while(!threadPool.awaitTermination(1, TimeUnit.MINUTES)){
	    	System.out.println("phase 2");
        }

		
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
