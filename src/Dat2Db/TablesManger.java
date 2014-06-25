package Dat2Db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public class TablesManger {
	
	public static void createTables() throws SQLException{
		String tablename = "trace_",tmp;
		Connection conn = ConnectionSource.getConnection();
    	Statement stmt = conn.createStatement();
    	for(int i=1;i<101;i++)
    	{
    		if(i<10){
    			tmp = tablename+"00"+i; 
    		}else if(i<100){
    			tmp = tablename+"0"+i; 
    		}else{
    			tmp = tablename+i;
    		}
    		System.out.println("line "+i);
    		stmt.execute("DROP TABLE IF EXISTS "+tmp);
    		stmt.execute("CREATE TABLE "+tmp+" LIKE trace_001");
    		 		
    	
    	}
    	stmt.close();
    	conn.close();
	}
	
	public static void truncate() throws SQLException{
		String tablename = "trace_",tmp;
		Connection conn = ConnectionSource.getConnection();
    	Statement stmt = conn.createStatement();
    	for(int i=1;i<101;i++)
    	{
    		if(i<10){
    			tmp = tablename+"00"+i; 
    		}else if(i<100){
    			tmp = tablename+"0"+i; 
    		}else{
    			tmp = tablename+i;
    		}
    		System.out.println("line "+i);
    		stmt.execute("truncate "+tmp+";");
    		 		
    	
    	}
    	stmt.close();
    	conn.close();
	}
	
	public static void calSum() throws SQLException{
		String tablename = "trace_",tmp;
		Connection conn = ConnectionSource.getConnection();
    	Statement stmt = conn.createStatement();
    	int num,sum = 0; 
    	for(int i=1;i<101;i++)
    	{
    		if(i<10){
    			tmp = tablename+"00"+i; 
    		}else if(i<100){
    			tmp = tablename+"0"+i; 
    		}else{
    			tmp = tablename+i;
    		}
    		System.out.println("line "+i);
    		ResultSet set = stmt.executeQuery("select count(id) from "+tmp);
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
			truncate();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
