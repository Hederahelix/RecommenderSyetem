package fengNiao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class calSimilar {

	private int total;
	

	
	public float calSimilar(String iid1,String iid2)
	{
		float similar = 0,tmp1,tmp2,tmp3;
		try {
			//String sql = "SELECT DISTINCT token,tfidf FROM token WHERE iid = ? ORDER BY `tfidf` LIMIT 30";
			Connection conn = ConnectionSource.getConnection();
			PreparedStatement pst = conn.prepareStatement("SELECT DISTINCT token,tfidf FROM token WHERE iid = ? ORDER BY tfidf LIMIT 30");
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
	
	public void calRandom()
	{
		try {
			String sql = "SELECT DISTINCT token,tfidf,t1.iid FROM token AS t1 JOIN (SELECT iid FROM caixin WHERE id >= (SELECT CEILING(1+RAND()*(SELECT MAX(id)FROM caixin)))ORDER BY id LIMIT 1) AS t2 WHERE t1.iid = t2.iid ORDER BY tfidf LIMIT 30";
			Connection conn = ConnectionSource.getConnection(); 
			Statement st = conn.createStatement();
			ResultSet set;
			HashMap<String, Float> hm1 = new HashMap<String, Float>(); 
			HashMap<String, Float> hm2 = new HashMap<String, Float>(); 
			Iterator iter;
			Map.Entry<String, Float> entry;
			String iid1,iid2;
			float similar = 0,tmp1,tmp2,tmp3;
			for(int i=0;i<10000;i++)
			{
				hm1.clear();
				hm2.clear();
				iid1=iid2=null;
				//查找两篇不同新闻
				while(true)
				{
					set = st.executeQuery(sql);
					while (set.next()) 
					{
						iid1 = set.getString(3);
						hm1.put(set.getString(1).trim(), -1*(set.getFloat(2)));
					}
					
					set = st.executeQuery(sql);
					while (set.next()) 
					{
						iid2 = set.getString(3);
						hm2.put(set.getString(1).trim(), -1*(set.getFloat(2)));
					}
					if(hm2.isEmpty() || hm1.isEmpty())
						continue;
					
					if(!iid1.equals(iid2))
						break;
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
				
				similar += tmp1/(tmp2*tmp3);
				System.out.println(i);
			}
			similar = similar/1000;
			System.out.println(similar);
				
			ConnectionSource.closeAll(conn,st);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}
	
	public static void main(String[] args) throws FileNotFoundException {
		// TODO Auto-generated method stub
		
		graph g = new graph();
		Scanner forthLine=new Scanner(new File("forthLineCaixin1hour.txt"));
		PrintWriter simOfforthLinePrintWriter=new PrintWriter("simOfforthLine1hour_after.txt");
		
		
//		Scanner secondLine=new Scanner(new File("secondLine_Caixin"));
		
//		int iid1,iid2;
//		double sumSimOf2line=0;
//		for (int i = 0; i < 10000; i++) {
//			iid1=secondLine.nextInt();
//			iid2=secondLine.nextInt();
//			sumSimOf2line+=g.calSimilar(((Integer)iid1).toString(), ((Integer)iid2).toString());
//		}
//		System.out.println(sumSimOf2line/10000);
		
	//第三条线	
		int j,j1;
		for (j = 0; j < 200; j++) {
			int readFlag=0;
			int iid1,iid2;
			double sumSim=0.0;
		while(readFlag<10000){
			readFlag++;
			iid1=forthLine.nextInt();
			iid2=forthLine.nextInt();
			j1=forthLine.nextInt();
			if(j!=j1){
				System.out.println("not enough lines");
				break;
			}
			double tempd=g.calSimilar(((Integer)iid1).toString(), ((Integer)iid2).toString());
			if(tempd<=1){
				sumSim+=tempd;
			}
		}
		System.out.println("the sim of second line is:"+sumSim/readFlag+"\tj="+j);
		
		simOfforthLinePrintWriter.println(sumSim/readFlag+"\t"+readFlag);
		readFlag=0;
		simOfforthLinePrintWriter.flush();
	}
		forthLine.close();
		simOfforthLinePrintWriter.close();
		
		
		//g.calRandom();
	//	g.calSimilar("100561810", "100563663");
		//g.test();
	//	g.calRandom();
	}

}
