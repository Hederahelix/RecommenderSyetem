package Common;

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
	
	public static void insert4news(String tableName,String filename,String Regex) throws IOException, SQLException{
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
        
        pst.close();
        conn.close();
		reader.close();
		
		System.out.println(sum+" "+line);
	}

	public static void loadwithoutindex(String[] dropIndex,String[] createIndex,String[] loadsql) throws SQLException{
		long startTime,endTime;
		
		Connection conn = ConnectionSource.getConnection();
		Statement stmt = conn.createStatement();
		
		//"DROP INDEX `index1` ON "+tableName+";"
		System.out.println("开始删除索引");
		for(int i=0;i<dropIndex.length;i++)
		{
			stmt.execute(dropIndex[i]);
			System.out.println("已经删除第"+i+"个索引");
		}

		//"LOAD DATA LOCAL INFILE '"+fileName+"' INTO TABLE "+tableName+" FIELDS TERMINATED BY ' ' (token,tf,tfidf,iid,type);"
		System.out.println("开始载入文件");
		startTime = System.currentTimeMillis();//获取当前时间
		for(int i=0;i<dropIndex.length;i++)
		{
			stmt.execute(loadsql[i]);
			System.out.println("已经载入第"+i+"个文件");
		}
		endTime = System.currentTimeMillis();//获取当前时间
		System.out.println("没有索引插入 耗时："+(endTime-startTime)/60000+"min");
		
		//"CREATE INDEX `index1` ON "+tableName+" (iid,type);"
		System.out.println("开始创建索引");
		startTime = System.currentTimeMillis();//获取当前时间
		for(int i=0;i<dropIndex.length;i++)
		{
			stmt.execute(createIndex[i]);
			System.out.println("已经创建第"+i+"个索引");
		}
		endTime = System.currentTimeMillis();//获取当前时间
		System.out.println(" 创建索引 耗时："+(endTime-startTime)/60000+"min");
		
		stmt.close();
		conn.close();		
	}
	
	public static void load4token_all(String[] fileName,String spliteChar) throws SQLException{
		String[] dropIndex={"DROP INDEX `index1` ON token_all;","DROP INDEX `index2` ON token_all;","DROP INDEX `index3` ON token_all;"};
		String[] createIndex={"CREATE INDEX `index1` ON token_all (iid,type);","CREATE INDEX `index2` ON token_all (token);","CREATE INDEX `index3` ON token_all (tfidf,iid);"};
		String[] loadsql = new String[fileName.length]; 
		
		for(int i=1;i<fileName.length;i++)
			loadsql[i] = "LOAD DATA LOCAL INFILE '"+fileName+"' INTO TABLE token_all FIELDS TERMINATED BY '"+spliteChar+"' (token,tf,tfidf,iid,type);";
		
		loadwithoutindex(dropIndex,createIndex,loadsql);
		
	}
	
	public static void load4trace_all(String[] fileName,String spliteChar) throws SQLException{
		String[] dropIndex={"DROP INDEX `index` ON trace_all;"};
		String[] createIndex={"CREATE INDEX `index` ON trace_all (uid,iid,time);"};
		String[] loadsql = new String[fileName.length]; 
		
		for(int i=1;i<fileName.length;i++)
			loadsql[i] = "LOAD DATA LOCAL INFILE '"+fileName+"' INTO TABLE trace_all FIELDS TERMINATED BY '"+spliteChar+"' (uid,iid,time);";
		
		
		loadwithoutindex(dropIndex,createIndex,loadsql);		
	}
	
	public static void load4trace_merge(String[] fileName,String spliteChar) throws SQLException{
		String[] dropIndex = new String[fileName.length]; 
		String[] createIndex = new String[fileName.length]; 
		String[] loadsql = new String[fileName.length]; 
		String tablename;
		for(int i=1;i<fileName.length;i++)
		{
			if(i<10)
				tablename = "trace_00"+i;
			if(i<100)
				tablename = "trace_0"+i;
			else
				tablename = "trace_"+i;
			
			dropIndex[i] = "DROP INDEX `index` ON "+tablename+";";
			loadsql[i] = "LOAD DATA LOCAL INFILE '"+fileName+"' INTO TABLE "+tablename+" FIELDS TERMINATED BY '"+spliteChar+"' (uid,iid,time);";
			createIndex[i] = "CREATE INDEX `index` ON "+tablename+" (uid,iid,time);";
		}
			
		
		loadwithoutindex(dropIndex,createIndex,loadsql);
		
		//创建MERGE表
		String tableStruct = "`id` int(11) NOT NULL AUTO_INCREMENT,"
	    		+"`uid` int(11) NOT NULL,"
	    		+"`iid` int(11) NOT NULL,"
	    		+"`time` double NOT NULL,"
	    		+"PRIMARY KEY (`id`),"
	    		+"KEY `index` (`uid`,`iid`,`time`)";
		
		TablesManger.createmergeTable("trace_merge", tableStruct, "trace", fileName.length);
		
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		
	}

}
