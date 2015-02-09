package com.bigdata.poc2;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.eclipse.paho.client.mqttv3.*;

import com.bigdata.utils.BigDataUtil;

/**
 * @author Dominik Obermaier
 * @author Christian Götz
 */
public class SystemsTempPublisher {

    public static final String BROKER_URL = "tcp://localhost:1883";

    
    public static final String TOPIC_TEMPERATURE = "home/systemperature";

    private MqttClient client;


    public SystemsTempPublisher() {

        //We have to generate a unique Client id.
        String clientId = BigDataUtil.getMacAddress() + "-pub";


        try {

            client = new MqttClient(BROKER_URL, clientId);

        } catch (MqttException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private void start() {

        try {
            MqttConnectOptions options = new MqttConnectOptions();
            options.setCleanSession(false);
            options.setWill(client.getTopic("home/LWT"), "I'm gone :(".getBytes(), 0, false);

            client.connect(options);

            publishTemperature();

            Thread.sleep(500);
            
        } catch (MqttException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void publishTemperature() throws MqttException {
        final MqttTopic temperatureTopic = client.getTopic(TOPIC_TEMPERATURE);

        
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        
        String resourceName = "HVAC.csv";
		loader = Thread.currentThread().getContextClassLoader();
		
		InputStream resourceStream = loader.getResourceAsStream(resourceName);
	    BufferedReader in = new BufferedReader(new InputStreamReader(resourceStream));
	    String line = null;
	    int recCount = 0;
	    
	    try {
			while((line = in.readLine()) != null) {
				System.out.println("Published data. Topic: " + temperatureTopic.getName() + "  Message: " + line);
				temperatureTopic.publish(new MqttMessage(line.getBytes()));
				try {    
						Thread.sleep(1000);
				} catch (InterruptedException exception) { 
					throw new RuntimeException(exception);
				}
				recCount++;
			}
		} catch (IOException e) {
			
		}
	    System.out.println("Total records in file " + recCount);
	    
    }

    public static void main(String... args) {
        final SystemsTempPublisher publisher = new SystemsTempPublisher();
        publisher.start();
    }
}
