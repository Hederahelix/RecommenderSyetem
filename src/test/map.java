package test;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

import Common.ConnectionSource;

public class map {


	//将iid替换为原始iid
	public void map2() throws SQLException, IOException {
		// TODO Auto-generated method stub
		Connection conn = ConnectionSource.getConnection();
		PreparedStatement pst = conn.prepareStatement("select * from trace4caixin where id > ? limit 10000");//选择用户浏览的所有新闻
		Statement st = conn.createStatement();
		
		ResultSet set = st.executeQuery("select iid,iidold from imap4caixin");
		HashMap<Integer, Integer> hm = new HashMap<Integer, Integer>();
		while(set.next())
		{
			hm.put(set.getInt(1), set.getInt(2));
		}
		set.close();
		
		BufferedWriter bw  = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("F:/data/caixin/trace/trace4caixin_original.txt"), "UTF-8"));    
        	
		set = st.executeQuery("select count(id) from trace4caixin");
		set.next();
	
		int total = set.getInt(1);//新闻总数
		st.close();
		
		System.out.println("开始转换");
		int iid,id,iidold,num=0,uid,time;
		for(int i=0;i*10000<total;i++)
		{
			long startTime = System.currentTimeMillis();//获取当前时间
			pst.setInt(1, i*10000);
			set = pst.executeQuery();
			while(set.next())
			{
				uid = set.getInt(2);
				iid = set.getInt(3);
				time = set.getInt(4);
				
				bw.write(uid+"\t"+iid+"\t"+time);
				bw.newLine();//换行
				
			}
			set.close();
			
	        
			if(i%10 == 0 && i!=0)
			{
				long endTime = System.currentTimeMillis();
				System.out.println(i+"*w 运行时间："+(endTime-startTime)+"ms");
			}
				
		}
		
		bw.flush();
    	bw.close();  
		
		System.out.println("替换完成");
		pst.close();
		conn.close();
		
	}
	//剔除掉iid不在news4caixin中的用户记录
	public void map3() throws SQLException, IOException {
		// TODO Auto-generated method stub
		Connection conn = ConnectionSource.getConnection();
		PreparedStatement pst = conn.prepareStatement("select * from trace4caixin where id > ? limit 10000");//选择用户浏览的所有新闻
		Statement st = conn.createStatement();
		
		ResultSet set = st.executeQuery("select iid from news4caixin");
		HashMap<Integer, Integer> hm = new HashMap<Integer, Integer>();
		while(set.next())
		{
			hm.put(set.getInt(1), 0);
		}
		set.close();
		
		BufferedWriter bw  = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("F:/data/caixin/sample/trace4caixin_original_delete.txt"), "UTF-8"));    
		
		set = st.executeQuery("select count(id) from trace4caixin");
		set.next();
	
		int total = set.getInt(1);//新闻总数
		st.close();
		
		System.out.println("开始转换");
		int iid,id,iidold,num=0,uid,time;
		for(int i=0;i*10000<total;i++)
		{
			long startTime = System.currentTimeMillis();//获取当前时间
			pst.setInt(1, i*10000);
			set = pst.executeQuery();
			while(set.next())
			{
				uid = set.getInt(2);
				iid = set.getInt(3);
				time = set.getInt(4);
				if(hm.containsKey(iid))
				{
					bw.write(uid+"\t"+iid+"\t"+time);
					bw.newLine();//换行
				}	
				
			}
			set.close();
			
	        
			if(i%10 == 0 && i!=0)
			{
				long endTime = System.currentTimeMillis();
				System.out.println(i+"*w 运行时间："+(endTime-startTime)+"ms");
			}
				
		}
		
		bw.flush();
    	bw.close();  
    	

		System.out.println("替换完成");
		pst.close();
		conn.close();
		
	}
	//删掉度小于2的用户的行为
	public void map4() throws SQLException, IOException {
		// TODO Auto-generated method stub
		Connection conn = ConnectionSource.getConnection();
		PreparedStatement pst = conn.prepareStatement("select * from trace4caixin where id > ? limit 10000");//选择用户浏览的所有新闻
		Statement st = conn.createStatement();
		
		ResultSet set = st.executeQuery("select uid from trace4caixin group by uid having count(id) > 1");
		HashMap<Integer, Integer> hm = new HashMap<Integer, Integer>();
		while(set.next())
		{
			hm.put(set.getInt(1), 0);
		}
		set.close();
		
		BufferedWriter bw  = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("F:/data/caixin/trace/trace4caixin_original_delete_degree.txt"), "UTF-8"));    
		
		set = st.executeQuery("select count(id) from trace4caixin");
		set.next();
	
		int total = set.getInt(1);//新闻总数
		st.close();
		
		System.out.println("开始转换");
		int iid,id,iidold,num=0,uid,time;
		for(int i=0;i*10000<total;i++)
		{
			long startTime = System.currentTimeMillis();//获取当前时间
			pst.setInt(1, i*10000);
			set = pst.executeQuery();
			while(set.next())
			{
				uid = set.getInt(2);
				iid = set.getInt(3);
				time = set.getInt(4);
				if(hm.containsKey(uid))
				{
					bw.write(uid+"\t"+iid+"\t"+time);
					bw.newLine();//换行
				}	
				
			}
			set.close();
			
	        
			if(i%10 == 0 && i!=0)
			{
				long endTime = System.currentTimeMillis();
				System.out.println(i+"*w 运行时间："+(endTime-startTime)+"ms");
			}
				
		}
		
		bw.flush();
    	bw.close();  
    	

		System.out.println("替换完成");
		pst.close();
		conn.close();
		
	}
	//去除内容为空的用户行为
	public void map5() throws IOException, SQLException {
		// TODO Auto-generated method stub
		Connection conn = ConnectionSource.getConnection();
																						//title contents iid
		PreparedStatement pst1 = conn.prepareStatement("delete from trace4caixin where iid = ? ");
		PreparedStatement pst3 = conn.prepareStatement("delete from news4caixin where id = ? ");
		PreparedStatement pst2 = conn.prepareStatement("select iid,id from news4caixin where contents is null");
		ResultSet set = pst2.executeQuery();
		int num=0;
		while(set.next()){
			pst1.setInt(1, set.getInt(1));
			pst3.setInt(1, set.getInt(2));
			pst1.execute();
			pst3.execute();
			num++;
		}
		System.out.println( num);
		set.close();
		pst1.close();
		pst2.close();
		pst3.close();
		conn.close();
		
	}
	
	//剔除掉iid不在news4caixin中的用户记录
	public void map6() throws SQLException, IOException {
		// TODO Auto-generated method stub
		BufferedWriter bw  = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("F:/data/caixin/sample/trace4caixin_original_delete.txt"), "UTF-8"));    
		
		Connection conn = ConnectionSource.getConnection();
		conn.setAutoCommit(false);
		PreparedStatement pst = conn.prepareStatement("select * from trace4caixin where id > ? limit 10000");//选择用户浏览的所有新闻
		Statement st = conn.createStatement();
		st.execute("create temporary table tmp (id int(11) not null AUTO_INCREMENT,uid int(11) not null,iid int(11) not null,time int(11) not null,primary key (id))");
		PreparedStatement pst1 = conn.prepareStatement("insert into tmp values (null,?,?,?)");//选择用户浏览的所有新闻
		
		ResultSet set = st.executeQuery("select iid from news4caixin");
		HashMap<Integer, Integer> hm = new HashMap<Integer, Integer>();
		while(set.next())
		{
			hm.put(set.getInt(1), 0);
		}
		set.close();
		
		set = st.executeQuery("select count(id) from trace4caixin");
		set.next();
		int total = set.getInt(1);//新闻总数
		set.close();
		
		System.out.println("开始去处没有新闻的记录");
		int iid,uid,time;
		for(int i=0;i*10000<total;i++)
		{
			long startTime = System.currentTimeMillis();//获取当前时间
			pst.setInt(1, i*10000);
			set = pst.executeQuery();
			while(set.next())
			{
				uid = set.getInt(2);
				iid = set.getInt(3);
				time = set.getInt(4);
				if(hm.containsKey(iid))
				{
					pst1.setInt(1, uid);
					pst1.setInt(2, iid);
					pst1.setInt(3, time);
					pst1.addBatch();
				}	
				
			}
			pst1.executeBatch();   
            conn.commit();   
            pst1.clearBatch();
			set.close();
				        
			if(i%10 == 0 && i!=0)
			{
				long endTime = System.currentTimeMillis();
				System.out.println(i+"*w 运行时间："+(endTime-startTime)+"ms");
			}
				
		}
		
		System.out.println("开始删掉度小于2的记录");
		pst = conn.prepareStatement("select * from tmp where id > ? limit 10000");//选择用户浏览的所有新闻
		set = st.executeQuery("select uid from tmp group by uid having count(id) > 1");
		hm = new HashMap<Integer, Integer>();
		while(set.next())
		{
			hm.put(set.getInt(1), 0);
		}
		set.close();
    	
		set = st.executeQuery("select count(id) from tmp");
		set.next();	
		total = set.getInt(1);//新闻总数
		set.close();
		
		for(int i=0;i*10000<total;i++)
		{
			long startTime = System.currentTimeMillis();//获取当前时间
			pst.setInt(1, i*10000);
			set = pst.executeQuery();
			while(set.next())
			{
				uid = set.getInt(2);
				iid = set.getInt(3);
				time = set.getInt(4);
				if(hm.containsKey(uid))
				{
					bw.write(uid+"\t"+iid+"\t"+time);
					bw.newLine();//换行
				}	
				
			}
			set.close();
			
	        
			if(i%10 == 0 && i!=0)
			{
				long endTime = System.currentTimeMillis();
				System.out.println(i+"*w 运行时间："+(endTime-startTime)+"ms");
			}
				
		}
		
		bw.flush();
    	bw.close();
		
		System.out.println("替换完成");
		pst.close();
		pst1.close();
		st.close();
		conn.close();
		
	}
	
		//剔除掉iid不在news4caixin中的用户记录
		public void map7() throws SQLException, IOException {
			// TODO Auto-generated method stub
			long endTime,startTime = System.currentTimeMillis();
			Connection conn = ConnectionSource.getConnection();
			conn.setAutoCommit(false);
			PreparedStatement pst = conn.prepareStatement("delete from news4xisihutong where id = ?");//选择用户浏览的所有新闻
			Statement st = conn.createStatement();
			st.execute("create temporary table iids (iid int(11) not null) select iid from news4xisihutong group by iid having count(iid)>1");
			st.execute("create temporary table ids (id int(11) not null,iid int(11) not null) select id,iid from news4xisihutong where iid in (select iid from iids)");
			HashMap<Integer, Integer> hm = new HashMap<Integer, Integer>();
			ResultSet set = st.executeQuery("select id,iid from ids");
			System.out.println("准备完毕");
			int num=0,iid,id;
			while(set.next())
			{
				id = set.getInt(1);
				iid = set.getInt(2);
				if(!hm.containsKey(iid))
				{
					hm.put(iid, 0);
				}else{
					pst.setInt(1, id);
					pst.addBatch();
					num++;
					if(num%1000==0)
					{
						pst.executeBatch();   
			            conn.commit();   
			            pst.clearBatch();
			            endTime = System.currentTimeMillis();
			            System.out.println(num+"*w 运行时间："+(endTime-startTime)+"ms");
			            startTime = endTime;
					}
				}				
			}
			set.close();
			pst.executeBatch();   
            conn.commit();   
            pst.clearBatch();
			
            st.execute("drop table iids");
            st.execute("drop table ids");
			pst.close();
			st.close();
			conn.close();
			
		}	
	public static void main(String[] args){
		// TODO Auto-generated method stub
		
		try {
			new map().map7();
			System.out.println("ok");
		} catch (SQLException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
