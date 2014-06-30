package fengNiao;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;



public class calTfidf {
	private HashMap<String, Integer> hm = new HashMap<String, Integer>();//�洢����token�ĳ��ָôʵ���������
	
	public void analysis(ResultSet set,PreparedStatement pst1,PreparedStatement pst2,int total) throws SQLException{
		
		
		while (set.next()) 
		{
			int id = set.getInt(1);
			String token =  set.getString(2);
			float tf = set.getFloat(3);
			float count = 0;
			if(hm.containsKey(token))
			{
				count = hm.get(token);//���������������
			}else
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
			int result = pst1.executeUpdate();
       }
		
	}
	
	public void cal(String tablename) throws SQLException{
		Connection conn = ConnectionSource.getConnection();
		PreparedStatement pst1 = conn.prepareStatement("update "+tablename+" set tfidf=? where id=?");
		PreparedStatement pst2 = conn.prepareStatement("select count(id) from "+tablename+" where token=?");
		PreparedStatement pst3 = conn.prepareStatement("select * from "+tablename+" where id > ? limit 10000");
		Statement st = conn.createStatement();
		int num = 1;
		ResultSet set = st.executeQuery("select count(id) from news");
		set.next();
		int total = set.getInt(1);//��������
		set = st.executeQuery("select count(id) from "+tablename+"");
		set.next();
		int sum = set.getInt(1);//�ִ�����
		set.close();
		System.out.println(sum);
		
		for(int i=0;i*10000<sum;i++)
		{
			pst3.setInt(1, i*10000);
			//System.out.println(i*10000); 
			set = pst3.executeQuery();
			analysis(set,pst1,pst2,total);
			set.close();
		}
		
		ConnectionSource.close(pst1);
		ConnectionSource.close(pst2);
		ConnectionSource.close(pst3);
		ConnectionSource.close(st);
		ConnectionSource.close(conn);
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		long startTime = System.currentTimeMillis();//��ȡ��ǰʱ��
		calTfidf clf = new calTfidf();
		try {
			clf.cal("token_all");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		long endTime = System.currentTimeMillis();
		System.out.println("��������ʱ�䣺"+(endTime-startTime)/60000+"min");
	}

}
