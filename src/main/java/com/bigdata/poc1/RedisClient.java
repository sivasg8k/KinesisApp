package com.bigdata.poc1;



import java.util.StringTokenizer;

import redis.clients.jedis.Jedis;

public class RedisClient {
	
	private static final String endpoint = "moviereviewsdb.sqjbuo.0001.use1.cache.amazonaws.com";
	
	private static RedisClient redisClient = new RedisClient();
	
	private Jedis jedis;
	
	private RedisClient() {
		
		jedis = new Jedis(endpoint,6379,15000);
		jedis.connect();
		
	}
	
	public static RedisClient getInstance() {
		return redisClient;
	}
	
	public void updateWordCountToRedis(String input) {
		StringTokenizer st = new StringTokenizer(input," ");
		Integer count = null;
		while(st.hasMoreTokens()) {
			String word = st.nextToken();
			if(null != jedis.get(word)) {
				count = Integer.parseInt(jedis.get(word));
				count++;
			} else {
				count = new Integer(1);
			}
			jedis.set(word, String.valueOf(count));
		}
	}
	
	public static void main(String args[]) {
		RedisClient rd = new RedisClient();
	}

}
