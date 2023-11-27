package de.htwdd.im;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

public class SimpleMQTTSub implements MqttCallback {

private List<String> activeMembersList = new ArrayList<String>();
private MqttClient myClient;
private MqttConnectOptions connOpt;

static final String BROKER_URL = "tcp://141.56.180.177:1883";

// connectionLost This callback is invoked upon losing the MQTT connection.
public void connectionLost(Throwable t) {
	System.out.println("Connection lost!");
	// code to reconnect to the broker would go here if desired
}

// deliveryComplete This callback is invoked when a message published by this client 
// is successfully received by the broker.
public void deliveryComplete(MqttDeliveryToken token) {
	try {
		System.out.println("Pub complete" + new String(token.getMessage().getPayload()));
	} catch (MqttException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
}

// MAIN
public static void main(String[] args) {
	
	//List<String> activeMembersList = new ArrayList<String>();
	
	SimpleMQTTSub smc = new SimpleMQTTSub();
	smc.runClient();
	//printListActiveMembers(topic);
}

// runClient The main functionality of this simple example. 
// Create a MQTT client, connect to broker, pub/sub, disconnect.
public void runClient() {
	// setup MQTT Client
	String clientID = MqttClient.generateClientId();
	connOpt = new MqttConnectOptions();
	connOpt.setCleanSession(false); // PERSISTENT SESSION
	connOpt.setKeepAliveInterval(30); 

	// Connect to Broker
	try {
		myClient = new MqttClient(BROKER_URL, clientID);
		myClient.setCallback(this);
		myClient.connect(connOpt);
	} catch (MqttException e) {
		e.printStackTrace();
		System.exit(-1);
	}

	System.out.println("Connected to " + BROKER_URL);

	// setup topic filter
	// String myTopic = "htwdd/informatik/wi/wise2324/im/s81983/mySampleValues";
	String myTopic = "htwdd/informatik/wi/wise2324/im/#"; // WILDCARD TOPIC
	String lwtTopic = "htwdd/informatik/wi/wise2324/im/s80287/crash"; // LAST WILL AND TESTAMENT

	// subscribe to topic if subscriber
	try {
		int subQoS = 1;
		myClient.subscribe(myTopic, subQoS);
		myClient.subscribe(lwtTopic, subQoS);
	} catch (Exception e) {
		e.printStackTrace();
	}
	
	// disconnect
	try {
		// wait to ensure subscribed messages are delivered
		Thread.sleep(100000);
		myClient.disconnect();
	} catch (Exception e) {
		e.printStackTrace();
	}
}

public void messageArrived(String topic, MqttMessage message) throws Exception {
	
	// list string declaration
	//List<String> activeMembersList = new ArrayList<String>();
	
	
	if (topic.equals("htwdd/informatik/wi/wise2324/im/s80287/crash")) {
		System.exit(-1);
	}
	
	
	System.out.println("-------------------------------------------------");
	System.out.println("| Topic:" + topic);
	
	System.out.println("| sNummer: " + topic.substring(32,38));
	activeMembersList.add(topic.substring(32,38));
	
	System.out.println("| Message: " + new String(message.getPayload()));
	System.out.println("-------------------------------------------------");

	// print all elements of list
	System.out.println("| List of active members:");
	System.out.println(Arrays.toString(activeMembersList.toArray()));
	
}

/*
public void printListActiveMembers(String topic, MqttMessage message) throws Exception {
	System.out.println("-------------------------------------------------");
	System.out.println("| List of active members:");
	System.out.println("| sNummer: " + topic.substring(32,38));
}
*/

@Override
public void deliveryComplete(IMqttDeliveryToken arg0) {
	// TODO Auto-generated method stub
	
}
}
