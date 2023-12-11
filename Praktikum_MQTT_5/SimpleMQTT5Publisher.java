package de.htwdd;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAck;
import com.hivemq.client.mqtt.mqtt5.message.disconnect.Mqtt5DisconnectReasonCode;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5PayloadFormatIndicator;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5PublishResult;

public class SimpleMQTT5Publisher {

	public static void main(String[] args) {
		
		// build blocking client
		Mqtt5BlockingClient  client = Mqtt5Client.builder().identifier(UUID.randomUUID().toString()).serverHost("broker.hivemq.com").buildBlocking();
		
		// connect blocking client
		Mqtt5ConnAck connAckMessage = client.connectWith().keepAlive(10).cleanStart(true).willPublish()
	            .topic("htwdd/informatik/wise2223/im/sXXXXX/dead")
	            .qos(MqttQos.AT_LEAST_ONCE)
	            .payload("I passed away.. :-(".getBytes())
	            .retain(true)
	            .messageExpiryInterval(100)
	            .delayInterval(10)
	            .payloadFormatIndicator(Mqtt5PayloadFormatIndicator.UTF_8)
	            .contentType("text/plain")
	            .userProperties()
	                .add("sNummer", "sXXXXX")
	                .applyUserProperties()
	            .applyWillPublish().
	
				send();
		
		System.out.println("Connected: " + connAckMessage);
		
		
		// publish a message
		Mqtt5PublishResult publishResult = client.publishWith().topic("htwdd/informatik/wise2223/im/sXXXXX")
				.qos(MqttQos.AT_LEAST_ONCE)
				.retain(false)
				.payload("Hello MQTT5".getBytes())
				.userProperties()
				.add("name", "sXXXXX")
				.applyUserProperties()
				.correlationData("myCorrDataID".getBytes())
				.responseTopic("htwdd/informatik/wise2223/im/sXXXXX/returnTopic")
				.send();
		
		System.out.println("Message sent: " + publishResult);
		
		
		// client dies and triggers LWT on broker
		client.disconnectWith().reasonCode(Mqtt5DisconnectReasonCode.DISCONNECT_WITH_WILL_MESSAGE).send();
		
		System.out.println("Discconnted w/ LWT trigger: ");
	}

}
