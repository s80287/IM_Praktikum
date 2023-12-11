
import java.util.UUID;

import com.hivemq.client.internal.mqtt.message.subscribe.suback.MqttSubAck;
import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;

import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAck;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5PayloadFormatIndicator;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5PublishResult;

public class SimpleMQTT5Subscriber {

	public static void main(String[] args) {

		Mqtt5BlockingClient  client = Mqtt5Client.builder().identifier(UUID.randomUUID().toString()).serverHost("141.56.180.177").serverPort(1883).buildBlocking();

		Mqtt5ConnAck connAckMessage = client.connectWith().keepAlive(10).cleanStart(true).willPublish()
				.topic("htwdd/informatik/wi/wise2324/im/s80287/dead")
				.qos(MqttQos.AT_LEAST_ONCE)
				.payload("ups... I crashed somehow...".getBytes())
				.retain(true)
				.messageExpiryInterval(100)
				.delayInterval(10)
				.payloadFormatIndicator(Mqtt5PayloadFormatIndicator.UTF_8)
				.userProperties()
				.add("sNummer", "s80287")
				.applyUserProperties()
				.applyWillPublish().
				send();

		System.out.println("Connected: " + connAckMessage);

		{
			try (Mqtt5BlockingClient.Mqtt5Publishes publishes = client.publishes(MqttGlobalPublishFilter.ALL)) {
				
				
				// Task 10: concurrent subscribers aka shared subscription
				// https://docs.hivemq.com/hivemq/latest/user-guide/shared-subscriptions.html
				MqttSubAck subAck = (MqttSubAck) client.subscribeWith().topicFilter("$share/group1/htwdd/informatik/wi/wise2324/im/s80287").qos(MqttQos.AT_LEAST_ONCE).send();

				System.out.println("subscribed... "+ subAck);
				
				boolean stopCondition = false;
				
				
				while (stopCondition == false) {
					Mqtt5Publish publish = publishes.receive();
					
					// Task 5: User properties
					System.out.println("User properties: " + publish.getUserProperties());
					
					String newString = new String(publish.getPayloadAsBytes());
					
					//System.out.println("Vor if: " + stopCondition);
					
					if (newString.equals("STOP")) {
						stopCondition = true;
					}
				
					//System.out.println("Nach if: " + stopCondition);
				
					System.out.print("Payload: " + new String(publish.getPayloadAsBytes()));

					System.out.println(" Received message:" + publish);

					// get the correlationID from the message
					if (publish.getCorrelationData().isPresent())
					{
						final byte[] b = new byte[publish.getCorrelationData().get().remaining()];
						publish.getCorrelationData().get().duplicate().get(b);
						System.out.println("CoorId:" + new String(b));
					}
					
					// Task 6: if publish.returnTopic.isPresent()
					// publish a message: copy this part from publisher
					
					if (publish.getResponseTopic().isPresent()) {
						// response topic
						Mqtt5PublishResult publishResult = client.publishWith().topic(publish.getResponseTopic().get())
								.qos(MqttQos.AT_LEAST_ONCE)
								.retain(false)
								.payload("Response came".getBytes())
								// correlation ID
								.correlationData(publish.getCorrelationData().get())
								.send();
						
						System.out.println("Response sent: " + publishResult);
					}
					
					
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
