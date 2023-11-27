package de.htwdd.im;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

public class PahoPublisher {
	public static void main(String[] args) {
		// ZUFALLSZAHLEN
		int min = 0;
	    int max = 100;
		int random_int = (int) Math.floor(Math.random()*(max-min+1) + min);
		String content = "Randomzahl: " + random_int;
		
		String topic = "htwdd/informatik/wi/wise2324/im/s80287/mySampleValues";
		int qos = 0;
		String broker = "tcp://141.56.180.177:1883";
		String clientId = MqttClient.generateClientId();
		MemoryPersistence persistence = new MemoryPersistence();

		try {
		
		// build a new MQTT client and bind it to the broker
		MqttClient sampleClient = new MqttClient(broker, clientId, persistence);
		MqttConnectOptions connOpts = new MqttConnectOptions();
		
		// check doc for clean session usage
		connOpts.setCleanSession(true);
		System.out.println("Connecting to broker: "+broker);
		
		//connecting to the broker
		sampleClient.connect(connOpts);
		System.out.println("Connected");
		System.out.println("Publishing message: "+content);
		
		//prepare the message
		MqttMessage message = new MqttMessage(content.getBytes());
		message.setRetained(true); // RETAINED MESSAGE
		message.setQos(qos);
		
		//publish the message
		sampleClient.publish(topic, message);
		System.out.println("Message published");
		// say good by to the broker
		sampleClient.disconnect();
		System.out.println("Disconnected");
		} catch(MqttException me) {
		System.out.println("reason "+me.getReasonCode());
		System.out.println("msg "+me.getMessage());
		System.out.println("loc "+me.getLocalizedMessage());
		System.out.println("cause "+me.getCause());
		System.out.println("excep "+me);
		me.printStackTrace();
		}
		}
}
