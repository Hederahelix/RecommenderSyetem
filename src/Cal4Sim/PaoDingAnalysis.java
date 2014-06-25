package Cal4Sim;

import java.io.IOException;
import java.io.StringReader; 
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;

import Dat2Db.ConnectionSource;
import net.paoding.analysis.analyzer.PaodingAnalyzer;




public class PaoDingAnalysis {
	
	private Connection conn = null;
	private PreparedStatement pst = null;
	private Analyzer analyzer = new PaodingAnalyzer(); //定义一个解析器  
	
	
	public void analysis(ResultSet set) throws SQLException{
		String text = set.getString(2);
		int iid =  set.getInt(1);
		
		long startTime = System.currentTimeMillis();//获取当前时间
		TokenStream tokenStream = analyzer.tokenStream(text, new StringReader(text)); //得到token序列的输出流  
		long endTime = System.currentTimeMillis();
		System.out.println("程序运行时间："+(endTime-startTime)+"ms");
		
		
		HashMap<String, Integer> hm  = new HashMap<String, Integer>(); 
		float sum = 0;//分词个数
		try {  
		    Token t; 
		   
		    while ((t = tokenStream.next()) != null)  
		    {  
		    	String temp = t.termText().trim();
		    	if(hm.containsKey(temp))
		    		hm.put(temp, hm.get(temp)+1);	
		    	else
		    		hm.put(temp, 1);
		    	sum++;
		    	
		    }  
		} catch (IOException e) {  
		    e.printStackTrace();  
		} 
		
		Iterator iter = hm.entrySet().iterator();
		int line = 1;
		while (iter.hasNext()) 
		{
			Map.Entry<String, Integer> entry = (Map.Entry<String, Integer>) iter.next();
			String key = entry.getKey();
			float val = entry.getValue()/sum;
			pst.setString(1, key); 
		    pst.setFloat(2, val);
		    pst.setFloat(3, 0); 
		    pst.setInt(4, iid);
		    
		    //pst.executeUpdate();
		    pst.addBatch();//用PreparedStatement的批量处理 
		    //System.out.println(key+"  "+val);   
		    if(line%1000==0){//当增加了500个批处理的时候再提交 
            	pst.executeBatch();   
                conn.commit();   
                pst.clearBatch();  
            }
		    line++;
		}
		
		if(line%1000!=0){//当增加了500个批处理的时候再提交 
        	pst.executeBatch();   
            conn.commit();   
            pst.clearBatch();  
        }
		
			
	}
	
	public void buildTb()
	{
		try {
			conn = ConnectionSource.getConnection();
			conn.setAutoCommit(false); 
			Statement st = conn.createStatement();
			pst = conn.prepareStatement("insert into token values (null,?,?,?,?)");
	        ResultSet set = st.executeQuery("select iid,contents from news");
	        int num = 1;
	        while (set.next()) {

	        	analysis(set);
	        	System.out.println(num++);   
	        	//break;

	        }
	        ConnectionSource.close(pst);  
	        ConnectionSource.closeAll(st,conn);
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		long startTime = System.currentTimeMillis();//获取当前时间
		PaoDingAnalysis pd = new PaoDingAnalysis();
		pd.buildTb();
		  
		long endTime = System.currentTimeMillis();
		System.out.println("程序运行时间："+(endTime-startTime)/60000+"min");
		
	}

}

