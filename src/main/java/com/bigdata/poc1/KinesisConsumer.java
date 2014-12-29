package com.bigdata.poc1;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.Shard;
import com.bigdata.utils.BigDataUtil;

public class KinesisConsumer {

	public static void main(String[] args) throws IOException {
		
		
		String streamName = "KinesisStreamingApp";
		
		AmazonKinesis kinesisClient = new AmazonKinesisClient(BigDataUtil.getInstance().getCreds());
		
		List<Record> records = null;
		
		DescribeStreamRequest dsr = new DescribeStreamRequest();
		dsr.setStreamName(streamName);
		List<Shard> shards = null;
		String startingSequenceNumber = "49546348174621634574882490030821379688905806070966910978";
		
		
		while(true) {
			
				DescribeStreamResult dsRes = kinesisClient.describeStream(dsr);
				shards = dsRes.getStreamDescription().getShards();
		
				for (Shard shard : shards) {
				  //Create new GetRecordsRequest with existing shardIterator.   //Set maximum records to return to 1000.
					
					String shardIterator = null;
					
					
					GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest();
					getShardIteratorRequest.setStreamName(streamName);
					getShardIteratorRequest.setShardId(shard.getShardId()); 
					getShardIteratorRequest.setShardIteratorType("AFTER_SEQUENCE_NUMBER");
					getShardIteratorRequest.setStartingSequenceNumber(startingSequenceNumber);
					
					GetShardIteratorResult getShardIteratorResult = kinesisClient.getShardIterator(getShardIteratorRequest);
					shardIterator = getShardIteratorResult.getShardIterator();
		
				
					GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
					
				    getRecordsRequest.setShardIterator(shardIterator);
				    //getRecordsRequest.setLimit(3000); 
				    GetRecordsResult result = kinesisClient.getRecords(getRecordsRequest);
				  //Put result into record list. Result may be empty.  records = result.getRecords();
				    records = result.getRecords();
				  
				  for(Record record : records) {
					  String recordData = new String(record.getData().array());
					  recordData = recordData.replace("review/summary", "");
					  recordData = recordData.replace("review/text", "");
					  System.out.println("record seq " + record.getSequenceNumber() + " :record data " + recordData);
					  recordData = BigDataUtil.getInstance().removeStopWords(recordData);
					  System.out.println("record seq " + record.getSequenceNumber() + " :record data without stop words" + recordData);
				  }
				  if(!records.isEmpty()) {
					  startingSequenceNumber = records.get(records.size()-1).getSequenceNumber();
				  }
				  try {    
					    Thread.sleep(1000);
					  } catch (InterruptedException exception) { 
						  throw new RuntimeException(exception);
				      }
				     // shardIterator = result.getNextShardIterator();
				 }
		}
	}

}
