package xisihutong;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ItemCF {
	

//String traceTable,String[] tokenTable,String recommendFile,String weightFile,int itemNum
	public void calMultiEffectByText(String recommendFile,String weightFile,int itemNum) throws SQLException, InterruptedException{
		String[] dbname = new String[100];
		for(int i=0;i<100;i++)
		{
			dbname[i] = "token4xisihutong_"+i;
		}
		new RecommendSystem.ItemCF().startIcf("trace4xisihutong", dbname, recommendFile, weightFile,itemNum);
	}
	
	public void calMultiEffectByTrace(String recommendFile,String weightFile,int itemNum) throws SQLException, InterruptedException{
		
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		long startTime,endTime;
		
		String FilePath = "F:/data/xisihutong/res/text/";
		
		try {
			startTime = System.currentTimeMillis();//获取当前时间
			new ItemCF().calMultiEffectByText(FilePath+"recommendlist50.txt",FilePath+"weightlist.txt",50);
			endTime = System.currentTimeMillis();//获取当前时间
			System.out.println(" 耗时："+(endTime-startTime)/60000+"min");
		} catch (SQLException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}

}

