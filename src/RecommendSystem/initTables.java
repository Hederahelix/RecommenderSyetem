package RecommendSystem;

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
		String tempString = null,title; 		
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
            	/*title = matcher.group(1);
            	if(title.length()>255)
            		title = title.substring(0,254).trim();*/
            	
            	pst.setString(1, matcher.group(1).trim()); //title
            	pst.setString(2, matcher.group(2).trim()); //contents
            	pst.setInt(3, Integer.parseInt(matcher.group(3).trim())); //iid
            	pst.addBatch();
            	sum++;

            }else
            	System.out.println("error :"+tempString);
             
			
			
			
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
	
	public void initTrace(String tableName,String trainFile,String testFile) throws SQLException
	{
		Connection conn = ConnectionSource.getConnection();
		String sql = "CREATE TABLE `"+tableName+"` ("
						+  "`id` int(11) NOT NULL AUTO_INCREMENT,"
						+  "`uid` int(11) DEFAULT NULL,"
						+  "`iid` int(11) DEFAULT NULL,"
						+  "`time` int(11) DEFAULT NULL,"
						+  "`type` int(11) DEFAULT '0',"
						+  "PRIMARY KEY (`id`)"
						+  ")ENGINE=InnoDB DEFAULT CHARSET=utf8";
		
		Statement stm = conn.createStatement();
		stm.execute(sql);
				
		//加载训练集
		String[] loadsql = {"LOAD DATA LOCAL INFILE '"+trainFile+"' INTO TABLE "+tableName+" FIELDS TERMINATED BY '\t' (uid,iid,time);"};
		try {
			new dat2Db().loadwithoutindex(null, null, loadsql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		//加载测试集
		stm.execute("ALTER TABLE "+tableName+" MODIFY COLUMN `type`  int(11) NULL DEFAULT 1 AFTER `time`;");
		String[] createIndex={"CREATE INDEX `index1` ON "+tableName+" (uid,type);","CREATE INDEX `index2` ON "+tableName+" (iid);"};
		loadsql[0] = "LOAD DATA LOCAL INFILE '"+testFile+"' INTO TABLE "+tableName+" FIELDS TERMINATED BY '\t' (uid,iid,time);";
		try {
			new dat2Db().loadwithoutindex(null, createIndex, loadsql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		stm.execute("ALTER TABLE "+tableName+" MODIFY COLUMN `type`  int(11) NULL DEFAULT 0 AFTER `time`;");
		stm.close();
		conn.close();
	}
	
	public void initToken(String newsName,String tokenName) throws SQLException
	{
		Connection conn = ConnectionSource.getConnection();
		String sql = "CREATE TABLE `"+tokenName+"` ("
						+  "`id` int(11) NOT NULL AUTO_INCREMENT,"
						+  "`token` varchar(255) NOT NULL,"
						+  "`tf` float NOT NULL,"
						+  "`tfidf` float NOT NULL,"
						+  "`iid` int(11) DEFAULT NULL,"
						+  "PRIMARY KEY (`id`)"
						+  ")ENGINE=InnoDB DEFAULT CHARSET=utf8";
		
		Statement stm = conn.createStatement();
		stm.execute(sql);
		
		String tmpfile = "F:/data/tmp.txt";
		try {
			new wordSegment().Segment4Content(newsName,tmpfile,"\t");
		} catch (InterruptedException | IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		String[] createIndex={"CREATE INDEX `index1` ON "+tokenName+" (iid);","CREATE INDEX `index2` ON "+tokenName+" (token);","CREATE INDEX `index3` ON "+tokenName+" (tfidf,iid);"};
		String[] loadsql = {"LOAD DATA LOCAL INFILE '"+tmpfile+"' INTO TABLE "+tokenName+" FIELDS TERMINATED BY '\t' (token,tf,tfidf,iid);"};
		try {
			new dat2Db().loadwithoutindex(null, createIndex, loadsql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		new wordSegment().calTfidf(newsName,tokenName);
		
		stm.close();
		conn.close();
	}
	
	public void initNews(String tableName,String filename,String Regex) throws SQLException
	{
		Connection conn = ConnectionSource.getConnection();
		String sql = "CREATE TABLE `"+tableName+"` ("
						+  "`id` int(11) NOT NULL AUTO_INCREMENT,"
						+  "`title` varchar(255) DEFAULT NULL,"
						+  "`contents` longtext,"
						+  "`iid` int(11) DEFAULT NULL,"
						+  "PRIMARY KEY (`id`)"
						+  ")ENGINE=InnoDB DEFAULT CHARSET=utf8";
		
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
