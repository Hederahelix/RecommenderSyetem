package caixing;

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
	
	public static void insert4news() throws IOException, SQLException{
		int sum = 0,line = 0;
		String tempString = null; 		
		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("D:/data/Ccaixin.txt"),"UTF-8"));
        
        //.*?"title":"([^"]*)".*?"contents":"([^"]*)".*?"iid":".*?(\d*)"
		
        String Regex=".*?\"title\":\"([^\"]*)\".*?\"contents\":\"([^\"]*)\".*?\"iid\":\".*?(\\d*)\"";
		Pattern p=Pattern.compile(Regex);
		Matcher matcher;

		
		Connection conn = ConnectionSource.getConnection();
		conn.setAutoCommit(false);
		Statement stmt = conn.createStatement();
		stmt.execute("DROP INDEX `index` ON news4caixin;");
		
		PreparedStatement pst = conn.prepareStatement("insert into news4caixin values (null,?,?,?); ");
		
		while ((tempString = reader.readLine()) != null) {
			
			line++;
			matcher = p.matcher(tempString);
            if (matcher.find()) 
            {
        		//titile contents iid
            	if(Integer.parseInt(0+matcher.group(3).trim())!=0)    		
            	{
            		pst.setString(1, matcher.group(1).trim()); 
                	pst.setString(2, matcher.group(2).trim()); 
                	pst.setInt(3, Integer.parseInt(0+matcher.group(3).trim())); 
                	pst.addBatch();
                	sum++;
            	}else{
            		System.out.println("error :"+tempString);
            	}
            	
            	
    			
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
        
        stmt.execute("CREATE INDEX `index` ON news4caixin (iid);");
        pst.close();
        stmt.close();
        conn.close();
		reader.close();
		
		System.out.println(sum+" "+line);
	}

	public static void loadwithoutindex(String fileName,String tableName) throws SQLException{
		long startTime,endTime;
		
		Connection conn = ConnectionSource.getConnection();
		Statement stmt = conn.createStatement();
		stmt.execute("DROP INDEX `index1` ON "+tableName+";");
		stmt.execute("DROP INDEX `index2` ON "+tableName+";");
		stmt.execute("DROP INDEX `index3` ON "+tableName+";");
		startTime = System.currentTimeMillis();//获取当前时间
		stmt.execute("LOAD DATA LOCAL INFILE '"+fileName+"' INTO TABLE "+tableName+" FIELDS TERMINATED BY '|' (token,tf,tfidf,iid,type);");
		endTime = System.currentTimeMillis();//获取当前时间
		System.out.println("没有索引插入 耗时："+(endTime-startTime)/60000+"min");
		
		startTime = System.currentTimeMillis();//获取当前时间
		stmt.execute("CREATE INDEX `index1` ON "+tableName+" (iid,type);");
		stmt.execute("CREATE INDEX `index2` ON "+tableName+" (token);");
		stmt.execute("CREATE INDEX `index3` ON "+tableName+" (tfidf,iid);");
		endTime = System.currentTimeMillis();//获取当前时间
		System.out.println(" 创建索引 耗时："+(endTime-startTime)/60000+"min");
		
		stmt.close();
		conn.close();		
	}
	
	public static void load4token_all() throws SQLException{
		long startTime,endTime;
		
		Connection conn = ConnectionSource.getConnection();
		Statement stmt = conn.createStatement();
		stmt.execute("DROP INDEX `index1` ON token_all;");
		stmt.execute("DROP INDEX `index2` ON token_all;");
		stmt.execute("DROP INDEX `index3` ON token_all;");
		startTime = System.currentTimeMillis();//获取当前时间
		for(int i=1;i<110;i++)
			stmt.execute("LOAD DATA LOCAL INFILE 'D:/data/fengniao/splitefilesbyiid/file"+i+".txt' INTO TABLE token_all FIELDS TERMINATED BY ' ' (token,tf,tfidf,iid,type);");
		endTime = System.currentTimeMillis();//获取当前时间
		System.out.println("没有索引插入 耗时："+(endTime-startTime)/60000+"min");
		
		startTime = System.currentTimeMillis();//获取当前时间
		stmt.execute("CREATE INDEX `index1` ON token_all (iid,type);");
		stmt.execute("CREATE INDEX `index2` ON token_all (token);");
		stmt.execute("CREATE INDEX `index3` ON token_all (tfidf,iid);");
		endTime = System.currentTimeMillis();//获取当前时间
		System.out.println(" 创建索引 耗时："+(endTime-startTime)/60000+"min");
		
		stmt.close();
		conn.close();		
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		long startTime = System.currentTimeMillis();//获取当前时间
		try {
			insert4news();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		long endTime = System.currentTimeMillis();
		System.out.println("insert4news 程序运行时间："+(endTime-startTime)/60000+"min");
		
		
	}

}
