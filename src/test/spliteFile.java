package test;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;

public class spliteFile {
	
	public void spliteByTime() throws IOException{
		int sum=0,datenum = 0;	
		String[] res;
		Double timestamp;
		String tempString = null; 		
		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("D:/data/task_res_caixingwang.txt"),"UTF-8"));
        BufferedWriter[] bw = new BufferedWriter[32];
        
        for(int i=1;i<32;i++)
        {
        	bw[i] = new BufferedWriter(new FileWriter("D:/data/splitefilesbytime/file"+i+".txt"));       	
        }
        
		while ((tempString = reader.readLine()) != null) {
			sum++;
			res = tempString.split(" ");
			timestamp = Double.parseDouble(res[2].trim());
            datenum = ((int)(timestamp - 1393603200)/86400+1);
			bw[datenum].write(tempString);
			bw[datenum].newLine();//换行
			
			if(sum % 1000000 == 0)
			{
				System.out.println("100w");
			}

		}
		
		for(int i=1;i<32;i++)
        {
			bw[i].flush();
        	bw[i].close();  
        }
		reader.close();
		
		System.out.println(sum);
	}
	
	public void spliteByUid() throws IOException{
		
	}
	
	public void spliteByLine() throws IOException{
		int sum=0,num=1;
		String tempString = null;
		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("D:/data/task_res_caixingwang.txt"),"UTF-8"));
		FileWriter writer = null;
        BufferedWriter bw = null;
        
		while ((tempString = reader.readLine()) != null) {
			
			if(sum % 1000000 == 0)
			{
				if(bw != null)
				{
					bw.flush();
					bw.close();
					System.out.println("file"+num+" complete");
				}
				
				writer = new FileWriter("D:/data/splitefilesbyuid/file"+num+".txt");
				bw = new BufferedWriter(writer);
				num++;
			}
			
			
			bw.write(tempString);
			bw.newLine();//换行
			
			sum++;

		}
		bw.flush();
		bw.close();
		reader.close();
	}
	
	public static void main(String[] args){
		
		long startTime = System.currentTimeMillis();//获取当前时间
		
		long endTime = System.currentTimeMillis();
		System.out.println("程序运行时间："+(endTime-startTime)/60000+"min");
	}
	
}