package RecommendSystem;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisShardInfo;

public class jedisProxy{
	private Jedis jedis;
	private float Maxmemory = 5;//GB

	public jedisProxy(Jedis jedis) {
		super();
		this.jedis = jedis;    
	}

	public Jedis getJedis() {
		return jedis;
	}

	public float getMemory()
	{
		String usedBytes = jedis.info().split("\n")[18].split(":")[1];
		int size = usedBytes.length();
		if(size>8)
		{
			String usedGBytes = usedBytes.substring(0,size - 9);
			usedGBytes = usedGBytes+"."+usedBytes.charAt(size- 9);
			return Float.parseFloat(usedGBytes);
		}else{
			return 0;
		}
	}
	
	public void lpushex(String key,String value)
	{
		if(getMemory()>Maxmemory)//Ğ´ÈëÎÄ¼ş
		{
			
		}else{
			jedis.lpush(key, value);
		}
	}
}
