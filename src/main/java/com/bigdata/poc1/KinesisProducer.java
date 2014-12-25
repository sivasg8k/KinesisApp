package com.bigdata.poc1;

import java.io.IOException;
import java.nio.ByteBuffer;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.bigdata.utils.BigDataUtil;

public class KinesisProducer {

	public static void main(String[] args) throws IOException {
		
		String streamName = "KinesisStreamingApp";
		
		AmazonKinesis kinesisClient = new AmazonKinesisClient(BigDataUtil.getInstance().getCreds());
		String sequenceNumberOfPreviousRecord = "1234";
		
		for (int j = 0; j < 10000000; j++) {  
			
			PutRecordRequest putRecordRequest = new PutRecordRequest(); 
			putRecordRequest.setStreamName(streamName);
			putRecordRequest.setData(ByteBuffer.wrap(String.format("testData-%d",j).getBytes()));
			putRecordRequest.setPartitionKey(String.format( "partitionKey-%d", j%5 )); 
		    putRecordRequest.setSequenceNumberForOrdering(String.valueOf(sequenceNumberOfPreviousRecord));  
		    PutRecordResult putRecordResult = kinesisClient.putRecord(putRecordRequest);  
		    sequenceNumberOfPreviousRecord = putRecordResult.getSequenceNumber(); 
		    System.out.println("record number " + j  + " inserted into kinesis");
		}


	}

}
