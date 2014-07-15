package Common;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

public class Indicator {
	
	public float callPrecision(String fileName,int itemNum,String spliteChar,String traceTable,String newsTable) throws SQLException, IOException
	{
		Connection conn = ConnectionSource.getConnection();
		PreparedStatement pst = conn.prepareStatement("select iid from "+traceTable+" where uid=? and type=1");//选择用户浏览的所有新闻
		Statement smt = conn.createStatement();
		ResultSet set = null; 
		
		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(fileName),"UTF-8"));
		int line=0; String tempString; String[] item;
		ArrayList<Integer> iid = new ArrayList<Integer>();int uid=-1;
		int Ntp = 0,M,N; float precision = 0,P = 0;
		
		set = smt.executeQuery("select count(distinct uid) from "+traceTable);
		set.next();
		M = set.getInt(1);
		set.close();
		
		set = smt.executeQuery("select count(distinct iid) from "+newsTable+" where type=1");
		set.next();
		N = set.getInt(1);
		set.close();
		
		while ((tempString = reader.readLine()) != null) 
		{		
			line++;
			item = tempString.split(spliteChar);
			if(line%itemNum == 0)
			{
				if(set!=null)
				{
					while(set.next())
					{
						if(iid.contains(set.getInt(1)))
							Ntp++;
					}
					P += (float)Ntp/itemNum;	
					iid.clear();
					set.close(); 
				}	
				
				uid = Integer.parseInt(item[0]);	
				pst.setInt(1, uid);
				set = pst.executeQuery();
				
				
				Ntp = 0;
			}
			
			iid.add(Integer.parseInt(item[1]));
			
		}
		P = P/M;
		precision = P/(itemNum/N);
		return precision;
	}
	
	public void callAUC(String fileName,String traceTable) throws SQLException
	{
		Connection conn = ConnectionSource.getConnection();
		PreparedStatement pst1 = conn.prepareStatement("select iid from "+traceTable+" where uid=? and type=1");//选择用户浏览的所有新闻
		ResultSet set; 
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
