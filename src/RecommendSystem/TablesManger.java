package RecommendSystem;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public class TablesManger {
	
	public void queryTables(String[] sql) throws SQLException{
		Connection conn = ConnectionSource.getConnection();
    	Statement stmt = conn.createStatement();
    	for(int i=0;i<sql.length;i++)
    	{
    		
    		System.out.println("line "+i);
    		stmt.execute(sql[i]);		
    	
    	}
    	stmt.close();
    	conn.close();
	}
	
	public void createmergeTable(String tableName,String tableStruct,String subTableName,int subTableCount) throws SQLException{
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
		Common.dat2Db db = new Common.dat2Db();
		
		for(int i=0;i<subTableCount;i++)
		{
			tablename = db.getTablesName(subTableName, i); 
			
			if(i==0)
				sql += tablename;
			else
				sql += ","+tablename;
			
		}
		sql+= ") INSERT_METHOD=LAST AUTO_INCREMENT=1;";
		stmt.execute(sql);	
		
		stmt.close();
		conn.close();
	}
	
	
	public void changeEngine4Trace(int subTableCount) throws SQLException{
		String[] sql = new String[subTableCount];
		String tableName;
		Common.dat2Db db = new Common.dat2Db();
		
    	for(int i=0;i<subTableCount;i++)
    	{
    		tableName = db.getTablesName("trace", i);
    		sql[i] = "alter table "+tableName+" engine=myisam;";
    	}

    			
    	queryTables(sql);
	}
	
	public void truncate4Token(int subTableCount) throws SQLException{
		String[] sql = new String[subTableCount];
		String tableName;
		Common.dat2Db db = new Common.dat2Db();
		
    	for(int i=0;i<subTableCount;i++)
    	{
    		tableName = db.getTablesName("token", i);
    		
    		sql[i] = "truncate "+tableName;
    	}

    			
    	queryTables(sql);
	}
	
	public void calSum(String[] subTableName) throws SQLException{
		Connection conn = ConnectionSource.getConnection();
    	Statement stmt = conn.createStatement();
    	int num,sum = 0; 
    	
    	for(int i=0;i<subTableName.length;i++)
    	{

    		ResultSet set = stmt.executeQuery("select count(id) from "+subTableName[i]);
    		set.next();
    		num = set.getInt(1);
    		set.close();
    		sum += num;
    		System.out.println(num); 
    	}
    	System.out.println(sum); 
    	stmt.close();
    	conn.close();
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		/*String[] sql = new String[31];
		String tableName;
    	Common.dat2Db db = new Common.dat2Db();
    	TablesManger tm = new TablesManger();
		for(int i=0;i<31;i++)
		{
			tableName = db.getTablesName("trace", i);
			sql[i] = "create table "+tableName+" like trace";
			System.out.println(sql[i]); 
		}
		try {
			tm.queryTables(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		String[] dbname = new String[100];
		for(int i=0;i<100;i++)
		{
			dbname[i] = "token4xisihutong_"+i;
		}
		try {
			new TablesManger().calSum(dbname);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
