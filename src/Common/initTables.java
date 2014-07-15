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

public class initTables {
	
	private void insert4news(String tableName,String filename,String Regex) throws IOException, SQLException{
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
            	pst.setInt(3, Integer.parseInt(matcher.group(3).trim())); //iid
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
        System.out.println(sum+" "+line);
        
        pst.close();
        conn.close();
		reader.close();
		
		
	}
	
	public void initTrace(String tableName) throws SQLException
	{
		Connection conn = ConnectionSource.getConnection();
		String sql = "CREATE TABLE `"+tableName+"` ("
						+  "`id` int(11) NOT NULL AUTO_INCREMENT,"
						+  "`uid` int(11) DEFAULT NULL,"
						+  "`iid` int(11) DEFAULT NULL,"
						+  "`time` int(11) DEFAULT NULL,"
						+  "`type` int(11) DEFAULT '0',"
						+  "PRIMARY KEY (`id`),"
						+  "KEY `index1` (`uid`,`type`),"
						+  "KEY `index2` (`iid`)"
						+  "ENGINE=InnoDB DEFAULT CHARSET=utf8";
		
		Statement stm = conn.createStatement();
		stm.execute(sql);
		
		
		
		conn.close();
	}
	
	public void initToken(String tableName) throws SQLException
	{
		Connection conn = ConnectionSource.getConnection();
		String sql = "CREATE TABLE `"+tableName+"` ("
						+  "`id` int(11) NOT NULL AUTO_INCREMENT,"
						+  "`token` varchar(255) NOT NULL,"
						+  "`tfidf` float NOT NULL,"
						+  "`iid` int(11) DEFAULT NULL,"
						+  "PRIMARY KEY (`id`),"
						+  "ENGINE=InnoDB DEFAULT CHARSET=utf8";
		
		Statement stm = conn.createStatement();
		stm.execute(sql);
		
		String[] files = {"F:/data/"+tableName+"/token/all/token4"+tableName+".txt"};
		Common.wordSegment ws = new Common.wordSegment();
		Common.dat2Db db = new Common.dat2Db();
		try {
			ws.Segment4Content("news4caixin",files,"\t");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String[] dropIndex={"DROP INDEX `index1` ON token4caixin;","DROP INDEX `index2` ON token4caixin;","DROP INDEX `index3` ON token4caixin;"};
		String[] createIndex={"CREATE INDEX `index1` ON token4caixin (iid,type);","CREATE INDEX `index2` ON token4caixin (token);","CREATE INDEX `index3` ON token4caixin (tfidf,iid);"};
		String[] loadsql = {"LOAD DATA LOCAL INFILE 'F:/data/caixin/token/all/token4caixin.txt' INTO TABLE token4caixin FIELDS TERMINATED BY '\t' (token,tf,tfidf,iid,type);"};
		try {
			db.loadwithoutindex(dropIndex, createIndex, loadsql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		stm.execute("CREATE INDEX `index1` ON "+tableName+" (iid);");
		stm.execute("CREATE INDEX `index2` ON "+tableName+" (token);");
		stm.execute("CREATE INDEX `index3` ON "+tableName+" (tfidf,iid);");
		stm.close();
		conn.close();
	}
	
	public void initNews(String tableName,String filename,String Regex) throws SQLException
	{
		Connection conn = ConnectionSource.getConnection();
		String sql = "CREATE TABLE `"+tableName+"` ("
						+  "`id` int(11) NOT NULL AUTO_INCREMENT,"
						+  "`title` varchar(100) DEFAULT NULL,"
						+  "`contents` longtext,"
						+  "`iid` int(11) DEFAULT NULL,"
						+  "PRIMARY KEY (`id`),"
						+  "ENGINE=InnoDB DEFAULT CHARSET=utf8";
		
		Statement stm = conn.createStatement();
		stm.execute(sql);
		
		try {
			insert4news(tableName,filename,Regex);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		stm.execute("CREATE INDEX `index1` ON "+tableName+" (iid);");
		stm.close();
		conn.close();
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
