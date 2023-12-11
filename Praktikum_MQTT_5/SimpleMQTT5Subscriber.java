package de.htwwdd;

import java.util.UUID;

import com.hivemq.client.internal.mqtt.message.subscribe.suback.MqttSubAck;
import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;

import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAck;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5PayloadFormatIndicator;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;

public class SimpleMQTT5Subscriber {

	public static void main(String[] args) {

		Mqtt5BlockingClient  client = Mqtt5Client.builder().identifier(UUID.randomUUID().toString()).serverHost("broker.hivemq.com").buildBlocking();

		Mqtt5ConnAck connAckMessage = client.connectWith().keepAlive(10).cleanStart(true).willPublish()
				.topic("htwdd/informatik/wise2223/im/sXXXXX/dead")
				.qos(MqttQos.AT_LEAST_ONCE)
				.payload("ups... I crashed somehow...".getBytes())
				.retain(true)
				.messageExpiryInterval(100)
				.delayInterval(10)
				.payloadFormatIndicator(Mqtt5PayloadFormatIndicator.UTF_8)
				.userProperties()
				.add("sNummer", "sXXXXX")
				.applyUserProperties()
				.applyWillPublish().
				send();

		System.out.println("Connected: " + connAckMessage);

		{
			try (Mqtt5BlockingClient.Mqtt5Publishes publishes = client.publishes(MqttGlobalPublishFilter.ALL)) {

				MqttSubAck subAck = (MqttSubAck) client.subscribeWith().topicFilter("htwdd/informatik/wise2223/im/sXXXXX").qos(MqttQos.AT_LEAST_ONCE).send();

				System.out.println("subscribed... "+ subAck);

				Mqtt5Publish publish = publishes.receive();
				System.out.print("Payload: " + new String(publish.getPayloadAsBytes()));

				System.out.println(" Received message:" + publish);

				// get the correlationID from the message
				if (publish.getCorrelationData().isPresent())
				{
					final byte[] b = new byte[publish.getCorrelationData().get().remaining()];
					publish.getCorrelationData().get().duplicate().get(b);
					System.out.println("CoorId:" + new String(b));
				}


			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		client.disconnect();
		System.out.println("Disonnected... ");
	}

}
