package xisihutong;

import java.sql.SQLException;

public class initTables {
	
	public void initNews(String filename,String Regex)
	{
		try {
			new Common.initTables().initNews("news4xisihutong", filename, Regex);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String filename, Regex;
		filename = "f:/data/Cxicihutong.txt";
		Regex = ".*?\"title\":\"([^\"]*)\".*?\"contents\":\"([^\"]*)\".*?\"iid\":\"[^\"]*(\\d+)\"";
		new initTables().initNews(filename,Regex);
	}

}
