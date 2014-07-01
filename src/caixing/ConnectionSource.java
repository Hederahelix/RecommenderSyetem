package caixing;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.dbcp.BasicDataSourceFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Properties;

public class ConnectionSource {
	
    private static BasicDataSource dataSource = null;

    public ConnectionSource() {
    }

    public static void init() {

        if (dataSource != null) {
            try {
                dataSource.close();
            } catch (Exception e) {
                //
        }
        dataSource = null;
    }

    try {
    	
        Properties p = new Properties();
        p.setProperty("driverClassName", "com.mysql.jdbc.Driver");
        p.setProperty("url", "jdbc:mysql://localhost:3306/recommender");
        p.setProperty("password", "root");
        p.setProperty("username", "root");
        p.setProperty("maxActive", "200");
        p.setProperty("maxIdle", "10");
        p.setProperty("maxWait", "1000");
        p.setProperty("removeAbandoned", "false");
        p.setProperty("removeAbandonedTimeout", "120");
        p.setProperty("testOnBorrow", "true");
        p.setProperty("logAbandoned", "true");

        dataSource = (BasicDataSource) BasicDataSourceFactory.createDataSource(p);

    } catch (Exception e) {
        //
    }
}


	public static synchronized Connection getConnection() throws  SQLException {
	    if (dataSource == null) {
	        init();
	    }
	    Connection conn = null;
	    if (dataSource != null) {
	        conn = dataSource.getConnection();
	    }
	    return conn;
	}
	
	public static void close(Connection conn) { 
        if (conn != null) 
            try { 
            	conn.close(); 
            } catch (SQLException e) { 
                e.printStackTrace(); 
            } 
    } 

    public static void close(ResultSet rs) { 
        if (rs != null) 
            try { 
                rs.close(); 
            } catch (SQLException e) { 
                e.printStackTrace(); 
            } 
    } 

    public static void close(Statement stmt) { 
        if (stmt != null) 
            try { 
                stmt.close(); 
            } catch (SQLException e) { 
                e.printStackTrace(); 
            } 
    } 
    
    public static void close(PreparedStatement stmt) { 
        if (stmt != null) 
            try { 
                stmt.close(); 
            } catch (SQLException e) { 
                e.printStackTrace(); 
            } 
    } 

   
    public static void closeAll(Object... objs) { 
        for (Object obj : objs) { 
            if (obj instanceof Connection) close((Connection) obj); 
            if (obj instanceof Statement) close((Statement) obj); 
            if (obj instanceof PreparedStatement) close((PreparedStatement) obj); 
            if (obj instanceof ResultSet) close((ResultSet) obj); 
        } 
    }
    
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		ConnectionSource.init();
		try {
			Connection conn = ConnectionSource.getConnection();
			Statement stmt = null;
	        ResultSet rset = null;
	        stmt = conn.createStatement();
            System.out.println("Executing statement.");
            rset = stmt.executeQuery("select * from news where id = 1");
            System.out.println("Results:");
            int numcols = rset.getMetaData().getColumnCount();
            while(rset.next()) {
                for(int i=1;i<=numcols;i++) {
                    System.out.print("\t" + rset.getString(i));
                }
                System.out.println("");
            }
	        
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
