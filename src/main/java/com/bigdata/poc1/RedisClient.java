package com.bigdata.poc1;



import redis.clients.jedis.Jedis;

public class RedisClient {
	
	private static final String endpoint = "moviereviewsdb.sqjbuo.0001.use1.cache.amazonaws.com";
	
	private Jedis jedis;
	
	public RedisClient() {
		
		jedis = new Jedis(endpoint,6379,15000);
		jedis.connect();
		
		System.out.println("Name ----->" + jedis.get("name"));
		jedis.set("name","Sathish");
		System.out.println("name modified");
		
		System.out.println("Score ----->" + jedis.get("score"));
		jedis.set("score","7741");
		System.out.println("score modified");
	}
	
	public static void main(String args[]) {
		RedisClient rd = new RedisClient();
		
		
	}

}
