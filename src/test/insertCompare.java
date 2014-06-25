package test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import Dat2Db.ConnectionSource;

public class insertCompare {
	
	public static void insertwithindex(String fileName,String tableName) throws SQLException{
		long startTime,endTime;
		
		Connection conn = ConnectionSource.getConnection();
		Statement stmt = conn.createStatement();
		
		startTime = System.currentTimeMillis();//获取当前时间
		stmt.execute("LOAD DATA LOCAL INFILE 'D:/data/splitefiles/"+fileName+".txt' INTO TABLE "+tableName+" FIELDS TERMINATED BY ' ' OPTIONALLY ENCLOSED BY '' LINES TERMINATED BY '\r\n' (uid,iid,time);");
		endTime = System.currentTimeMillis();//获取当前时间
		System.out.println("有索引插入 耗时："+(endTime-startTime)/60000+"min");
		
		stmt.close();
		conn.close();
	}
	
	public static void insertwithoutindex(String fileName,String tableName) throws SQLException{
		long startTime,endTime;
		
		Connection conn = ConnectionSource.getConnection();
		Statement stmt = conn.createStatement();
		stmt.execute("DROP INDEX `index` ON "+tableName+";");
		
		startTime = System.currentTimeMillis();//获取当前时间
		stmt.execute("LOAD DATA LOCAL INFILE 'D:/data/splitefiles/"+fileName+".txt' INTO TABLE "+tableName+" FIELDS TERMINATED BY ' ' OPTIONALLY ENCLOSED BY '' LINES TERMINATED BY '\r\n' (uid,iid,time);");
		endTime = System.currentTimeMillis();//获取当前时间
		System.out.println("没有索引插入 耗时："+(endTime-startTime)/60000+"min");
		
		startTime = System.currentTimeMillis();//获取当前时间
		stmt.execute("CREATE INDEX `index` ON "+tableName+" (uid,iid,time);");
		endTime = System.currentTimeMillis();//获取当前时间
		System.out.println(" 创建索引 耗时："+(endTime-startTime)/60000+"min");
		
		stmt.close();
		conn.close();		
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		long startTime = System.currentTimeMillis();//获取当前时间
		try {
			//insertwithoutindex("file3","trace_003");
			insertwithindex("file3","trace_003");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		long endTime = System.currentTimeMillis();//获取当前时间
		System.out.println("程序运行时间："+(endTime-startTime)/60000+"min");
	}

}
