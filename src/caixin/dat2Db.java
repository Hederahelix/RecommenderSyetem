package caixin;

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
	
	public static void insert4news() throws IOException, SQLException{
		Common.dat2Db db = new Common.dat2Db();
		
		db.insert4news("news4caixin", "F:/data/Ccaixin.txt", ".*?\"title\":\"([^\"]*)\".*?\"contents\":\"([^\"]*)\".*?\"iid\":\".*?(\\d*)\"");
		
        
        //.*?"title":"([^"]*)".*?"contents":"([^"]*)".*?"iid":".*?(\d*)"
		
        
	}

	public static void loadwithoutindex(String fileName,String tableName) throws SQLException{
		
	}
	
	public static void load4token_all() throws SQLException{

	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		long startTime = System.currentTimeMillis();//获取当前时间
		try {
			insert4news();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		long endTime = System.currentTimeMillis();
		System.out.println("insert4news 程序运行时间："+(endTime-startTime)/60000+"min");
		
		
	}

}
