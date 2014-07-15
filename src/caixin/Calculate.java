package caixin;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Calculate {
	
	
	public void calTfidf()
	{
		Common.Calculate cal = new Common.Calculate();
		try {
			cal.calTfidf("news4caixin", "token4caixin");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public float calSimilar(String tableName,String iid1,String iid2)
	{
		float similar = 0,tmp1,tmp2,tmp3;
		try {
			//String sql = "SELECT DISTINCT token,tfidf FROM token WHERE iid = ? ORDER BY `tfidf` LIMIT 30";
			//选出文章的30个特征词
			Connection conn = ConnectionSource.getConnection();
			PreparedStatement pst = conn.prepareStatement("SELECT DISTINCT token,tfidf FROM "+tableName+" WHERE iid = ? ORDER BY tfidf LIMIT 30");
			
			
			ResultSet set;
			HashMap<String, Float> hm1 = new HashMap<String, Float>(); 
			HashMap<String, Float> hm2 = new HashMap<String, Float>(); 
			Iterator iter;
			Map.Entry<String, Float> entry;
			
			pst.setString(1, iid1);
			set = pst.executeQuery();
			while (set.next()) 
			{
				hm1.put(set.getString(1).trim(), set.getFloat(2));
			}
			set.close();
			pst.setString(1, iid2);
			set = pst.executeQuery();
			pst.close();
			while (set.next()) 
			{
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
			ConnectionSource.closeAll(conn,pst);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	//	System.out.println(similar);
		return similar;
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		new Calculate().calTfidf();
	}
	
}
