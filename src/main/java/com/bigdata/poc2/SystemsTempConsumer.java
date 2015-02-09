package com.bigdata.poc2;

import java.util.List;

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
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
import com.amazonaws.services.kinesis.model.ShardIteratorType;

public class SystemsTempConsumer {

	public static void main(String[] args) {
		
		String streamName = "SysTempStream";
		
		AmazonKinesis kinesisClient = new AmazonKinesisClient(new EnvironmentVariableCredentialsProvider());
		
		List<Record> records = null;
		
		DescribeStreamRequest dsr = new DescribeStreamRequest();
		dsr.setStreamName(streamName);
		List<Shard> shards = null;
		String startingSequenceNumber = "49547752755836920748238186178896741036426297854077698050";
		
		
		
			
				DescribeStreamResult dsRes = kinesisClient.describeStream(dsr);
				
				shards = dsRes.getStreamDescription().getShards();
				int recCount = 0;
				
		
				for (Shard shard : shards) {
				  //Create new GetRecordsRequest with existing shardIterator.   //Set maximum records to return to 1000.
					
					String shardIterator = null;
					GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest();
					getShardIteratorRequest.setStreamName(streamName);
					getShardIteratorRequest.setShardId(shard.getShardId()); 
					getShardIteratorRequest.setShardIteratorType(ShardIteratorType.AT_SEQUENCE_NUMBER);
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
					  System.out.println("record seq " + record.getSequenceNumber() + " :record data " + recordData);
					  recCount++;
				  }
				 
				  System.out.println("The total records in stream " + recCount);
				  
				  try {    
					    Thread.sleep(1000);
				  } catch (InterruptedException exception) { 
					  throw new RuntimeException(exception);
				  }
				     // shardIterator = result.getNextShardIterator();
				 }
		

	}

}
