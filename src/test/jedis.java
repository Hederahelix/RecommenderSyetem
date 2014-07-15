package test;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import redis.clients.jedis.Jedis;

public class jedis {

	public static void main(String[] args) throws InterruptedException {

		Semaphore semp = new Semaphore(10);
		ExecutorService threadPool = Executors.newFixedThreadPool(10);
		
		for(int i=0;i<1000;i++){
			semp.acquire();//等待有线程完成任务，再新建新的任务
			System.out.println("--------------------------------"+i+"th thread start");
			threadPool.submit(new simTask(i,semp));
		}
		System.out.println("ok");//执行结果：xinxin 
		
		redisUtil.destory();
	}

}


class simTask implements Callable<Integer>{
	private int id;
	private Semaphore semp;
	private Jedis jedis;
	
	public simTask(int id,Semaphore semp){
		this.id = id;
		this.semp = semp;
		jedis = redisUtil.getJedis();

	}
	
	
	public Integer call(){	
		long startTime,endTime,midTime;		
		startTime = System.currentTimeMillis();//获取当前时间
		for(int i=0;i<10000000;i++)
		{
			jedis.set("id"+id+" "+i, ""+i);
			System.out.println(i);
		}
					
		endTime = System.currentTimeMillis();
		System.out.println(id+"th thread cost time = "+(endTime-startTime)+"ms");
		semp.release();
		redisUtil.returnResource(jedis);
		return 1;    
	 }
}



