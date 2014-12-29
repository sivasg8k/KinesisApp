package com.bigdata.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;

public class BigDataUtil {
	
	private BigDataUtil() {
		
	}
	
	private static final BigDataUtil bigDataUtil = new BigDataUtil();
	private static AWSCredentials awsCreds = null;
	private static Map<String,String> stopWords = new HashMap<String,String>();
	
	static {
		
		String resourceName = "awsCredentials.properties"; // could also be a constant
		ClassLoader loader = Thread.currentThread().getContextClassLoader();
		Properties props = new Properties();
		
		InputStream resourceStream = loader.getResourceAsStream(resourceName);
	    try {
			props.load(resourceStream);
		} catch (IOException e) {
			e.printStackTrace();
		}
	    
	    String accessKey = props.getProperty("accessKey");
	    String secretKey = props.getProperty("secretKey");
		
		awsCreds = new BasicAWSCredentials(accessKey, secretKey);
		
		resourceName = "stop_words_english.txt";
		loader = Thread.currentThread().getContextClassLoader();
		
		resourceStream = loader.getResourceAsStream(resourceName);
	    BufferedReader in = new BufferedReader(new InputStreamReader(resourceStream));
	    String line = null;
	    
	    try {
			while((line = in.readLine()) != null) {
				stopWords.put(line.trim(), "1");
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static BigDataUtil getInstance() {
		return bigDataUtil;
	}
	
	public AWSCredentials getCreds() {
		return awsCreds;
	}
	
	public String removeStopWords(String input) {
		
		StringBuffer output = new StringBuffer();
		
		StringTokenizer st = new StringTokenizer(input," ");
		
		while(st.hasMoreTokens()) {
			String token = st.nextToken().trim();
			if(null == stopWords.get(token)) {
				token = token.replace("/", "");
				token = token.replace(":", "");
				token = token.replace(",", "");
				token = token.replace(".", "");
				output.append(token);
				output.append(" ");
			}
		}
		return output.toString();
	}
	
	

}
