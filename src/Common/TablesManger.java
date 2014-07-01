package Common;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public class TablesManger {
	
	public static void queryTables(String[] sql) throws SQLException{
		Connection conn = ConnectionSource.getConnection();
    	Statement stmt = conn.createStatement();
    	for(int i=1;i<sql.length;i++)
    	{
    		
    		System.out.println("line "+i);
    		stmt.execute(sql[i]);		
    	
    	}
    	stmt.close();
    	conn.close();
	}
	
	public static void createmergeTable(String tableName,String tableStruct,String subTableName,int subTableCount) throws SQLException{
		Connection conn = ConnectionSource.getConnection();
		Statement stmt = conn.createStatement();
		/*"CREATE TABLE IF NOT EXISTS trace_merge("
	    		+"`id` int(11) NOT NULL AUTO_INCREMENT,"
	    		+"`uid` int(11) NOT NULL,"
	    		+"`iid` int(11) NOT NULL,"
	    		+"`time` double NOT NULL,"
	    		+"PRIMARY KEY (`id`),"
	    		+"KEY `index` (`uid`,`iid`,`time`)"
	    		+") ENGINE=MERGE UNION=(";
		 */
		String tablename,sql = "CREATE TABLE IF NOT EXISTS "+tableName+"("+tableStruct+") ENGINE=MERGE UNION=(";
		
		for(int i=1;i<subTableCount;i++)
		{
			if(i<10)
				tablename = subTableName+"_00"+i;
			if(i<100)
				tablename = subTableName+"_0"+i;
			else
				tablename = subTableName+"_"+i;
			
			if(i==1)
				sql += tablename;
			else
				sql += ","+tablename;
			
		}
		sql+= ") INSERT_METHOD=LAST AUTO_INCREMENT=1;";
		stmt.execute(sql);	
		
		stmt.close();
		conn.close();
	}
	
	
	public static void changeEngine4Trace(int subTableCount) throws SQLException{
		String[] sql = new String[subTableCount];

    	for(int i=1;i<subTableCount;i++)
    	{
    		if(i<10)
    			sql[i] = "alter table trace_00"+i+" engine=myisam;";
    		if(i<100)
				sql[i] = "alter table trace_0"+i+" engine=myisam;";
    		else
    			sql[i] = "alter table trace_"+i+" engine=myisam;";
    	}

    			
    	queryTables(sql);
	}
	
	public static void truncate4Token(int subTableCount) throws SQLException{
		String[] sql = new String[subTableCount];

    	for(int i=1;i<subTableCount;i++)
    	{
    		if(i<10)
    			sql[i] = "truncate token_00"+i+";";
    		if(i<100)
				sql[i] = "truncate token_0"+i+";";
    		else
    			sql[i] = "truncate token_"+i+";";
    	}

    			
    	queryTables(sql);
	}
	
	public static void calSum(String subTableName,int subTableCount) throws SQLException{
		Connection conn = ConnectionSource.getConnection();
    	Statement stmt = conn.createStatement();
    	int num,sum = 0; 
    	String tableName;
    	
    	for(int i=1;i<subTableCount;i++)
    	{
    		if(i<10)
    			tableName = subTableName+"_00"+i;
    		if(i<100)
    			tableName = subTableName+"_0"+i;
    		else
    			tableName = subTableName+"_"+i;
    		
    		ResultSet set = stmt.executeQuery("select count(id) from "+tableName);
    		set.next();
    		num = set.getInt(1);
    		sum += num;
    		System.out.println(num); 
    	}
    	System.out.println(sum); 
    	stmt.close();
    	conn.close();
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
	}

}
