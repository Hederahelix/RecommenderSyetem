package fengNiao;

import java.io.IOException;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import net.paoding.analysis.analyzer.PaodingAnalyzer;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;

public class test {
	
	public static void staticCount4iid() throws SQLException{
		int spliteNum = 30000;
		Connection conn = ConnectionSource.getConnection();
		PreparedStatement pst = conn.prepareStatement("select count(id) from news where iid between ? and ?");
		
		for(int i=1;i*spliteNum<3256485+spliteNum;i++){
			pst.setInt(1, (i-1)*spliteNum);
			pst.setInt(2, i*spliteNum);
			ResultSet set = pst.executeQuery();
			set.next();
			System.out.println("line: "+i+" iid between "+(i-1)*spliteNum+" and "+ i*spliteNum+" count:"+set.getInt(1));
			set.close();
		}
		
		ConnectionSource.closeAll(conn,pst);
	}
	
	public static void test4Paoding() throws SQLException, IOException{
		Analyzer analyzer = new PaodingAnalyzer(); 
        String  indexStr = "ÎÒµÄQQºÅÂëÊÇ58472399"; 
        StringReader reader = new StringReader(indexStr); 
        TokenStream ts = analyzer.tokenStream(indexStr, reader); 
        Token t = ts.next(); 
        while (t != null) { 
            System.out.print(t.termText()+"  "); 
            t = ts.next(); 
        } 
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.print(Math.ceil(0.5));
		
	}

}
