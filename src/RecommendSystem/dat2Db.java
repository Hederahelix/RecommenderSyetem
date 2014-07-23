package RecommendSystem;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import Dat2Db.ConnectionSource;

public class dat2Db {
	
	public void insert(String insertSql,int filedNum,String filename,String Regex) throws IOException, SQLException{
		int sum = 0,line = 0;
		String tempString = null; 		
		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(filename),"UTF-8"));
        
		Pattern p=Pattern.compile(Regex);
		Matcher matcher;

		Connection conn = ConnectionSource.getConnection();
		conn.setAutoCommit(false);
																						//title contents iid
		PreparedStatement pst = conn.prepareStatement(insertSql);
		
		while ((tempString = reader.readLine()) != null) {
			
			line++;
			matcher = p.matcher(tempString);
            if (matcher.find()) 
            {
            	for(int i=1;i<=filedNum;i++)
            		pst.setObject(i, matcher.group(i).trim()); //title
            	
            	pst.addBatch();
            	sum++;

            }else
            	System.out.println("error :"+line);
             
			
			
			
			if(sum % 5000 == 0)
			{
				pst.executeBatch();   
                conn.commit();   
                pst.clearBatch();
                
			}
			
			if(sum % 10000 == 0){
				System.out.println("complete :"+sum);
			}

		}
		
		pst.executeBatch();   
        conn.commit();   
        pst.clearBatch();
        System.out.println(sum+" "+line);
        
        pst.close();
        conn.close();
		reader.close();
		
		
	}
	
	
	public void insert4news(String tableName,String filename,String Regex) throws IOException, SQLException{
		int sum = 0,line = 0;
		String tempString = null; 		
		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(filename),"UTF-8"));
        
		Pattern p=Pattern.compile(Regex);
		Matcher matcher;

		Connection conn = ConnectionSource.getConnection();
		conn.setAutoCommit(false);
																						//title contents iid
		PreparedStatement pst = conn.prepareStatement("insert into "+tableName+" values (null,?,?,?); ");
		
		while ((tempString = reader.readLine()) != null) {
			
			line++;
			matcher = p.matcher(tempString);
            if (matcher.find()) 
            {
            	pst.setString(1, matcher.group(1).trim()); //title
            	pst.setString(2, matcher.group(2).trim()); //contents
            	pst.setInt(3, Integer.parseInt(0+matcher.group(3).trim())); //iid
            	pst.addBatch();
            	sum++;

            }else
            	System.out.println("error :"+line);
             
			
			
			
			if(sum % 5000 == 0)
			{
				pst.executeBatch();   
                conn.commit();   
                pst.clearBatch();
                
			}
			
			if(sum % 10000 == 0){
				System.out.println("complete :"+sum);
			}

		}
		
		pst.executeBatch();   
        conn.commit();   
        pst.clearBatch();
        System.out.println(sum+" "+line);
        
        pst.close();
        conn.close();
		reader.close();
		
		
	}

	public void loadwithoutindex(String[] dropIndex,String[] createIndex,String[] loadsql) throws SQLException{
		long startTime,endTime;
		
		Connection conn = ConnectionSource.getConnection();
		Statement stmt = conn.createStatement();
		
		//"DROP INDEX `index1` ON "+tableName+";"
		System.out.println("开始删除索引");
		if(dropIndex!=null){
			for(int i=0;i<dropIndex.length;i++)
			{
				stmt.execute(dropIndex[i]);
				System.out.println("已经删除第"+(i+1)+"个索引");
			}
		}
		

		//"LOAD DATA LOCAL INFILE '"+fileName+"' INTO TABLE "+tableName+" FIELDS TERMINATED BY ' ' (token,tf,tfidf,iid,type);"
		System.out.println("开始载入文件");
		startTime = System.currentTimeMillis();//获取当前时间
		for(int i=0;i<loadsql.length;i++)
		{
			stmt.execute(loadsql[i]);
			System.out.println("已经载入第"+(i+1)+"个文件");
		}
		endTime = System.currentTimeMillis();//获取当前时间
		System.out.println("没有索引插入 耗时："+(endTime-startTime)/60000+"min");
		
		//"CREATE INDEX `index1` ON "+tableName+" (iid,type);"
		System.out.println("开始创建索引");
		startTime = System.currentTimeMillis();//获取当前时间
		if(createIndex!=null)
		{
			for(int i=0;i<createIndex.length;i++)
			{
				stmt.execute(createIndex[i]);
				System.out.println("已经创建第"+(i+1)+"个索引");
			}
		}
		endTime = System.currentTimeMillis();//获取当前时间
		System.out.println(" 创建索引 耗时："+(endTime-startTime)/60000+"min");
		
		stmt.close();
		conn.close();		
	}
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			new dat2Db().insert("insert into iid4xisihutong values(null,?,?)",2,"F:/data/tmp/hashiid.txt","(\\d+).*?(\\d+)");
		} catch (IOException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
