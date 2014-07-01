package fengNiao;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public class TablesManger {
	
	public static void createTables4Token() throws SQLException{
		String tablename = "Token",tmp;
		Connection conn = ConnectionSource.getConnection();
    	Statement stmt = conn.createStatement();
    	for(int i=1;i<110;i++)
    	{
    		
    		System.out.println("line "+i);
    		//stmt.execute("DROP TABLE IF EXISTS "+tmp);
    		//stmt.execute("CREATE TABLE Token_"+i+" LIKE Token");
    		stmt.execute("alter table Token_"+i+" engine=myisam;");		
    	
    	}
    	stmt.close();
    	conn.close();
	}
	
	public static void createmerge4Token() throws SQLException{
		String sql;
		Connection conn = ConnectionSource.getConnection();
    	Statement stmt = conn.createStatement();
    	sql = "CREATE TABLE IF NOT EXISTS token(`id` int(11) NOT NULL AUTO_INCREMENT,"
    		+"`token` varchar(255) NOT NULL,"
    		+"`tf` float NOT NULL,"
    		+"`tfidf` float NOT NULL,"
    		+"`iid` varchar(255) NOT NULL,"
    		+"`type` int(2) NOT NULL,"
    		+"PRIMARY KEY (`id`),"
    		+"KEY `index1` (`iid`,`type`),"
    		+"KEY `index2` (`token`),"
    		+"KEY `index3` (`tfidf`,`iid`)"
    		+") ENGINE=MERGE UNION=(token_1";
    	for(int i=2;i<110;i++)
    	{
    		
    		sql+= ",token_"+i;

    	}
    	sql+= ") INSERT_METHOD=LAST AUTO_INCREMENT=1;";
    	System.out.println(sql);
    	stmt.execute(sql);		
    	stmt.close();
    	conn.close();
	}
	
	public static void createmerge4Trace() throws SQLException{
		String sql;
		Connection conn = ConnectionSource.getConnection();
    	Statement stmt = conn.createStatement();
    	sql = "CREATE TABLE IF NOT EXISTS trace("
    		+"`id` int(11) NOT NULL AUTO_INCREMENT,"
    		+"`uid` int(11) NOT NULL,"
    		+"`iid` int(11) NOT NULL,"
    		+"`tfidf` float NOT NULL,"
    		+"`iid` varchar(255) NOT NULL,"
    		+"`time` double NOT NULL,"
    		+"PRIMARY KEY (`id`),"
    		+"KEY `index` (`uid`,`iid`,`time`)"
    		+") ENGINE=MERGE UNION=(trace_001";
    	for(int i=2;i<110;i++)
    	{
    		if(i<10)
    			sql+= ",trace_00"+i;
			 else
				sql+= ",trace_0"+i;
    	}
    	sql+= ") INSERT_METHOD=LAST AUTO_INCREMENT=1;";
    	System.out.println(sql);
    	stmt.execute(sql);		
    	stmt.close();
    	conn.close();
	}
	
	public static void changeEngine4Trace() throws SQLException{
		String sql;
		Connection conn = ConnectionSource.getConnection();
    	Statement stmt = conn.createStatement();
    	sql = "alter table tt7 engine=myisam;";
    	for(int i=1;i<101;i++)
    	{
    		if(i<10)
    			sql = "alter table trace_00"+i+" engine=myisam;";
			 else
				 sql = "alter table trace_0"+i+" engine=myisam;";
    		
    		System.out.println(sql);
        	stmt.execute(sql);
    	}

    			
    	stmt.close();
    	conn.close();
	}
	
	public static void truncate4Token() throws SQLException{
		String tablename = "Token",tmp;
		Connection conn = ConnectionSource.getConnection();
    	Statement stmt = conn.createStatement();
    	for(int i=1;i<110;i++)
    	{
    		
    		System.out.println("line "+i);
    		stmt.execute("truncate Token_"+i+";");
    		 		
    	
    	}
    	stmt.close();
    	conn.close();
	}
	
	public static void calSum4Token() throws SQLException{
		String tablename = "Token",tmp;
		Connection conn = ConnectionSource.getConnection();
    	Statement stmt = conn.createStatement();
    	int num,sum = 0; 
    	for(int i=1;i<32;i++)
    	{
    		if(i<10)
				tablename = "trace_00"+i;
			else
				tablename = "trace_0"+i;
    		System.out.println("line "+i);
    		ResultSet set = stmt.executeQuery("select count(id) from "+tablename);
    		set.next();
    		num = set.getInt(1);
    		System.out.println(num); 
    		sum += num;
 	
    	}
    	System.out.println(sum); 
    	stmt.close();
    	conn.close();
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			calSum4Token();
			//changeEngine4Trace();
			//createmerge4Token();
			//createTables4Token();
			//truncate4Token();
			//calSum4Token();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
