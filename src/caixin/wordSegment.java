package caixin;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import net.paoding.analysis.analyzer.PaodingAnalyzer;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;






public class wordSegment {	
	
	public static void Segment4Content(){
		String[] files = {"F:/data/caixin/token/all/token4caixin.txt"};
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
	}
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
	}
	
	

}
