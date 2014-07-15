package caixin;

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
	
	//划分训练集
	public void init() throws SQLException{
		new Common.ItemCF().init("trace4caixin");
	}
	
	
	public void calMultiEffectByText(String recommendFile,String weightFile,int itemNum) throws SQLException, InterruptedException{
		new Common.ItemCF().calMultiEffect("trace4caixin", "token4caixin", recommendFile, weightFile,itemNum);
	}
	
	public void calMultiEffectByTrace(String recommendFile,String weightFile,int itemNum) throws SQLException, InterruptedException{
		new Common.ItemCF().calMultiEffectByTrace("trace4caixin", recommendFile, weightFile,itemNum);
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		long startTime,endTime;
		
		String FilePath = "F:/data/caixin/res/Large20samples/trace/";
		
		try {
			for(int i=2;i<11;i+=4)
			{
				startTime = System.currentTimeMillis();//获取当前时间
				new ItemCF().calMultiEffectByTrace(FilePath+"recommendlist"+i+".txt",FilePath+"weightlist"+i+".txt",i);
				endTime = System.currentTimeMillis();//获取当前时间
				System.out.println(" 耗时："+(endTime-startTime)/60000+"min");
			}
		} catch (SQLException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}

}

