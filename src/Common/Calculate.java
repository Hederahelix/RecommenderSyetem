package Common;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Calculate {
	
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
	
	
	public float calSimilarByText(String tableName,String iid1,String iid2)
	{
		
		float similar = 0,tmp1,tmp2,tmp3;
		try {
			//String sql = "SELECT DISTINCT token,tfidf FROM token WHERE iid = ? ORDER BY `tfidf` LIMIT 30";
			//选出文章的30个特征词
			
			Connection conn = ConnectionSource.getConnection();

			/*PreparedStatement pst = conn.prepareStatement("(SELECT DISTINCT token,tfidf,iid FROM "+tableName+" WHERE iid = ? ORDER BY tfidf DESC LIMIT 30) union "
					+ "(SELECT DISTINCT token,tfidf,iid FROM "+tableName+" WHERE iid = ? ORDER BY tfidf DESC LIMIT 30)");*/
			PreparedStatement pst = conn.prepareStatement("SELECT DISTINCT token,tfidf,iid FROM "+tableName+" WHERE iid = ? ORDER BY tfidf DESC LIMIT 30");
			
			ResultSet set;
			HashMap<String, Float> hm1 = new HashMap<String, Float>(); //token tfidf
			HashMap<String, Float> hm2 = new HashMap<String, Float>(); 
			Iterator iter;
			Map.Entry<String, Float> entry;
			
			//獲取兩篇文章的30個特征值
			pst.setString(1, iid1);
			set = pst.executeQuery();
			while (set.next()) 
			{
				hm1.put(set.getString(1).trim(), set.getFloat(2));
			}
			set.close();
			pst.setString(1, iid2);
			set = pst.executeQuery();
			
			while (set.next()) 
			{
				hm2.put(set.getString(1).trim(), set.getFloat(2));
			}
			set.close();
			pst.close();
			
			/*
			pst.setString(1, iid1);
			pst.setString(2, iid2);
			set = pst.executeQuery();
			
			
			while (set.next()) 
			{
				if(set.getString(3).equals(iid1))
					hm1.put(set.getString(1).trim(), set.getFloat(2));
				else
					hm2.put(set.getString(1).trim(), set.getFloat(2));
			}
			set.close();
			pst.close();*/
			
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
			
			tmp2 = (float) Math.sqrt(tmp2);
			iter = hm2.entrySet().iterator();
			while (iter.hasNext()) 
			{
				entry = (Map.Entry<String, Float>) iter.next();
				tmp3 += Math.pow(entry.getValue(),2);//x2*x2
			}
			tmp3 = (float) Math.sqrt(tmp3);
			System.out.println(tmp1+" "+tmp2+" "+tmp3);
			
			similar = tmp1/(tmp2*tmp3);
			ConnectionSource.closeAll(conn,pst);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	//	System.out.println(similar);
		return similar;
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
		
		tmp2 = (float) Math.sqrt(tmp2);
		iter = hm2.entrySet().iterator();
		while (iter.hasNext()) 
		{
			entry = (Map.Entry<String, Float>) iter.next();
			tmp3 += Math.pow(entry.getValue(),2);//x2*x2
		}
		tmp3 = (float) Math.sqrt(tmp3);
		
		similar = tmp1/(tmp2*tmp3);

	//	System.out.println(similar);
		return similar;
	}
	
	public float calSimilarByText(ResultSet set)
	{
		String iid1 = null;
		boolean flag = true;
		long startTime = System.currentTimeMillis();//获取当前时间
		float similar = 0,tmp1,tmp2,tmp3;
		try {
			
			HashMap<String, Float> hm1 = new HashMap<String, Float>(); //token tfidf
			HashMap<String, Float> hm2 = new HashMap<String, Float>(); 
			Iterator iter;
			Map.Entry<String, Float> entry;

			while (set.next()) 
			{
				if(flag){
					iid1 = set.getString(3);
					flag = false;
				}
				
				if(set.getString(3).equals(iid1))
					hm1.put(set.getString(1).trim(), set.getFloat(2));
				else
					hm2.put(set.getString(1).trim(), set.getFloat(2));
			}
			
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
			
			tmp2 = (float) Math.sqrt(tmp2);
			iter = hm2.entrySet().iterator();
			while (iter.hasNext()) 
			{
				entry = (Map.Entry<String, Float>) iter.next();
				tmp3 += Math.pow(entry.getValue(),2);//x2*x2
			}
			tmp3 = (float) Math.sqrt(tmp3);
			
			similar = tmp1/(tmp2*tmp3);

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return similar;
	}
	
	
	
	public static void main(String[] args) throws SQLException, ClassNotFoundException {
		// TODO Auto-generated method stub

		/*String url = "jdbc:mysql://127.0.0.1/recommender";  
	    String name = "com.mysql.jdbc.Driver";  
	    String user = "root";  
	    String password = "root";
		Class.forName(name);//指定连接类型  
        long startTime,endTime;
        String sql = null;
        Connection  conn;
        Statement st;
        int startid;
        //1
		startTime = System.currentTimeMillis();//获取当前时间
		startid = 0;
		for(int i=0;i<1000;i++)
		{
			conn = DriverManager.getConnection(url, user, password);//获取连接  
			st = conn.createStatement();
			st.execute("RESET QUERY CACHE");
			sql = "SELECT * FROM token4caixin WHERE id = "+(i+startid);
			st.executeQuery(sql);
			st.close();
			conn.close();
		}
		endTime = System.currentTimeMillis();//获取当前时间
		System.out.println("1："+(endTime-startTime)+"ms");
		//2
		startTime = System.currentTimeMillis();//获取当前时间
		
		conn = DriverManager.getConnection(url, user, password);//获取连接  
		st = conn.createStatement();
		for(int i=0;i<1000;i++)
		{
			sql = "SELECT * FROM token4caixin WHERE id = "+(i+startid);
			st.execute("RESET QUERY CACHE");
			st.executeQuery(sql);
		}
		st.close();
		conn.close();
		endTime = System.currentTimeMillis();//获取当前时间
		System.out.println("2："+(endTime-startTime)+"ms");
		//3
		startTime = System.currentTimeMillis();//获取当前时间
		
		for(int i=0;i<1000;i++)
		{
			conn = ConnectionSource.getConnection();//获取连接  
			st = conn.createStatement();
			sql = "SELECT * FROM token4caixin WHERE id = "+(i+startid);
			st.execute("RESET QUERY CACHE");
			st.executeQuery(sql);
			st.close();
			conn.close();
		}
		endTime = System.currentTimeMillis();//获取当前时间
		System.out.println("3："+(endTime-startTime)+"ms");	
		//4
		startTime = System.currentTimeMillis();//获取当前时间
		
		conn = DriverManager.getConnection(url, user, password);//获取连接  
		st = conn.createStatement();
		for(int i=0;i<1000;i++)
		{
			if(i==0)
				sql = "(SELECT * FROM token4caixin WHERE id = "+(i+startid)+" )";
			else
				sql += "union (SELECT * FROM token4caixin WHERE id = "+(i+startid)+" )";
		}
		st.execute("RESET QUERY CACHE");
		st.executeQuery(sql);
		st.close();
		conn.close();
		endTime = System.currentTimeMillis();//获取当前时间
		System.out.println("4："+(endTime-startTime)+"ms");*/
		Connection conn = ConnectionSource.getConnection();
		String[] dbname = new String[100];
		for(int i=0;i<100;i++)
		{
			dbname[i] = "token4xisihutong_"+i;
		}
		PreparedStatement[] pst2 = new PreparedStatement[100];			
		for(int i=0;i<100;i++)
		{
			pst2[i] = conn.prepareStatement("SELECT DISTINCT token,tfidf FROM "+dbname[i]+" WHERE iid = ? ORDER BY tfidf DESC LIMIT 30");
		}
		pst2[3100706 %100].setString(1, ""+3100706 );
		ResultSet set = pst2[3100706 %100].executeQuery();
		
		HashMap<String, Float> hm1 = new HashMap();HashMap<String, Float> hm2;
		while (set.next()) 
		{
			hm1.put(set.getString(1).trim(), Float.parseFloat(set.getString(2).trim()));
		}
		
		set.close();
		System.out.println(new Calculate().calSimilarByText("token4xisihutong", "100645962", "100645699"));
	}
}
