package fengNiao;

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
		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("D:/data/Cfengniao.txt"),"UTF-8"));
        
        //.*?"title":"([^"]*)".*?"contents":"([^"]*)".*?"iid":".*?(\d*)"
        String Regex=".*?\"title\":\"([^\"]*)\".*?\"contents\":\"([^\"]*)\".*?\"iid\":\".*?(\\d*)\"";
		Pattern p=Pattern.compile(Regex);
		Matcher matcher;

		
		Connection conn = ConnectionSource.getConnection();
		conn.setAutoCommit(false);
		PreparedStatement pst = conn.prepareStatement("insert into news values (null,?,?,?); ");
		
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
		stmt.execute("LOAD DATA LOCAL INFILE '"+fileName+"' INTO TABLE "+tableName+" FIELDS TERMINATED BY ' ' (token,tf,tfidf,iid,type);");
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
	
	public static void load4trace_all() throws SQLException{
		long startTime,endTime;
		
		Connection conn = ConnectionSource.getConnection();
		Statement stmt = conn.createStatement();
		stmt.execute("DROP INDEX `index` ON trace_all;");
		startTime = System.currentTimeMillis();//获取当前时间
		stmt.execute("LOAD DATA LOCAL INFILE 'F:/data/splitefiles/file_all.txt' INTO TABLE trace_all FIELDS TERMINATED BY ' ' (uid,iid,time);");
		
		endTime = System.currentTimeMillis();//获取当前时间
		System.out.println("没有索引插入 耗时："+(endTime-startTime)/60000+"min");
		
		startTime = System.currentTimeMillis();//获取当前时间
		stmt.execute("CREATE INDEX `index` ON trace_all (uid,iid,time);");
		endTime = System.currentTimeMillis();//获取当前时间
		System.out.println(" 创建索引 耗时："+(endTime-startTime)/60000+"min");
		
		stmt.close();
		conn.close();		
	}
	
	public static void load4trace_merge() throws SQLException{
		long startTime,endTime;
		String tablename,sql;
		Connection conn = ConnectionSource.getConnection();
		Statement stmt = conn.createStatement();
		sql = "CREATE TABLE IF NOT EXISTS trace_merge("
	    		+"`id` int(11) NOT NULL AUTO_INCREMENT,"
	    		+"`uid` int(11) NOT NULL,"
	    		+"`iid` int(11) NOT NULL,"
	    		+"`time` double NOT NULL,"
	    		+"PRIMARY KEY (`id`),"
	    		+"KEY `index` (`uid`,`iid`,`time`)"
	    		+") ENGINE=MERGE UNION=(";
		
		for(int i=1;i<32;i++){
			if(i<10)
				tablename = "trace_00"+i;
			else
				tablename = "trace_0"+i;
			
			if(i==1)
				sql += tablename;
			else
				sql += ","+tablename;
			
			stmt.execute("DROP INDEX `index` ON "+tablename+";");
			stmt.execute("LOAD DATA LOCAL INFILE 'F:/data/splitefiles/file"+i+".txt' INTO TABLE "+tablename+" FIELDS TERMINATED BY ' ' (uid,iid,time);");
			stmt.execute("CREATE INDEX `index` ON "+tablename+" (uid,iid,time);");
			System.out.println(i);
		}
		sql+= ") INSERT_METHOD=LAST AUTO_INCREMENT=1;";
		stmt.execute(sql);	
		
		stmt.close();
		conn.close();		
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		long startTime = System.currentTimeMillis();//获取当前时间
		try {
			load4trace_merge();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		long endTime = System.currentTimeMillis();
		System.out.println("insert4news 程序运行时间："+(endTime-startTime)/60000+"min");
		
		
	}

}
