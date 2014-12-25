package com.bigdata.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;

public class BigDataUtil {
	
	private BigDataUtil() {
		
	}
	
	private static final BigDataUtil bigDataUtil = new BigDataUtil();
	private static AWSCredentials awsCreds = null;
	
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
	}
	
	public static BigDataUtil getInstance() {
		return bigDataUtil;
	}
	
	public AWSCredentials getCreds() {
		return awsCreds;
	}

}
