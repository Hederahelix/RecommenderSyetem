package test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import Dat2Db.ConnectionSource;

public class loadFile {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Connection conn;
		try {
			long startTime = System.currentTimeMillis();//获取当前时间
			conn = ConnectionSource.getConnection();
			Statement stmt = conn.createStatement();
			 System.out.println("LOAD DATA LOCAL INFILE 'D:/data/test/10000000w.txt' INTO TABLE trace_001 FIELDS TERMINATED BY ' ' OPTIONALLY ENCLOSED BY '' LINES TERMINATED BY '\r\n' (uid,iid,time);");
			 stmt.execute("LOAD DATA LOCAL INFILE 'D:/data/splitefiles/file4.txt' INTO TABLE trace_004 FIELDS TERMINATED BY ' ' OPTIONALLY ENCLOSED BY '' LINES TERMINATED BY '\r\n' (uid,iid,time);");
			 long endTime = System.currentTimeMillis();//获取当前时间
			 System.out.println("LOAD FILE 100w 程序运行时间："+(endTime-startTime)/60000+"min");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
