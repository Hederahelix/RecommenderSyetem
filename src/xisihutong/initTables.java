package xisihutong;

import java.io.IOException;
import java.sql.SQLException;

import RecommendSystem.dat2Db;

public class initTables {
	
	public void initNews()
	{
		String filename = "f:/data/Cxicihutong.txt";
		String Regex = ".*?\"title\":\"([^\"]*)\".*?\"contents\":\"([^\"]*)\".*?\"iid\":\"[^\"]*?(\\d+)\"";
		try {
			new RecommendSystem.initTables().initNews("news4xisihutong", "iid4xisihutong", filename, Regex);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void initToken()
	{
		try {
			new RecommendSystem.initTables().initToken("news4xisihutong", "token4xisihutong",100,"F:/data/xisihutong/tmp/");
		} catch (SQLException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}


	public void initTrace()
	{
		try {
			//String traceName,String newsName,String trainFile,String testFile,String Regex
			new RecommendSystem.initTables().initTrace("trace4xisihutong", "news4xisihutong","F:/data/xisihutong/trace/10000samples.txt", "F:/data/xisihutong/trace/test10000samples.txt");
		} catch (SQLException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		new initTables().initToken();
		System.out.println("-------------- ok");
		/*
		String[] sql = new String[3178];
		for(int i=0;i<3178;i++)
		{
			sql[i] = "LOAD DATA LOCAL INFILE 'F:/data/xisihutong/tmp/token"+i+".txt' INTO TABLE token4test FIELDS TERMINATED BY '\t'"
					+ " (token,tf,tfidf,iid);";
		}
		
		try {
			new dat2Db().loadwithoutindex(null, null, sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	       
		
		/*new initTables().initTokenEx();*/
		
	}
}
