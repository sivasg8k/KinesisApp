package com.bigdata.poc2;

import java.nio.ByteBuffer;

import org.eclipse.paho.client.mqttv3.*;

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.bigdata.utils.BigDataUtil;


/**
 * @author Dominik Obermaier
 * @author Christian Götz
 */
public class SubscribeCallback implements MqttCallback {
	
	AmazonKinesis kinesisClient = null;
	String streamName = "SysTempStream";
	private static int recCount = 0;
	
	public SubscribeCallback() {
		kinesisClient = new AmazonKinesisClient(new EnvironmentVariableCredentialsProvider());
	}

	public void connectionLost(Throwable cause) {
		// TODO Auto-generated method stub
		
	}

	public void messageArrived(String topic, MqttMessage message)
			throws Exception {
		
		String inMessage = message.toString();
		
		String[] inMsgSpl = inMessage.split(",");
		
		String timestamp = BigDataUtil.getInstance().convertDateToHiveFormat(inMsgSpl[0], inMsgSpl[1]);
		
		StringBuffer outMsg = new StringBuffer(timestamp + ",");
		int msgLen = inMsgSpl.length;
		
		for(int i=2;i<msgLen;i++) {
			outMsg.append(inMsgSpl[i]);
			outMsg.append(",");
		}
		outMsg.deleteCharAt(outMsg.length()-1);
		
		System.out.println("Message arrived. Topic: " + topic + "  Message: " + outMsg.toString());
		
		PutRecordRequest putRecordRequest = new PutRecordRequest();
		putRecordRequest.setStreamName(streamName);
		putRecordRequest.setData(ByteBuffer.wrap(outMsg.toString().getBytes()));
		putRecordRequest.setPartitionKey(String.format("partitionKey1"));
		PutRecordResult putRecordResult = kinesisClient.putRecord(putRecordRequest);
		System.out.println("record published to kinesis" + putRecordResult.getSequenceNumber());
		System.out.println("records inserted " + recCount);
		
		recCount++;
		
		
		if ("home/LWT".equals(topic)) {
            System.err.println("Sensor gone!");
            System.out.println("records inserted into kinesis" + recCount);
        }
	}

	public void deliveryComplete(IMqttDeliveryToken token) {
		// TODO Auto-generated method stub
		
	}
}
