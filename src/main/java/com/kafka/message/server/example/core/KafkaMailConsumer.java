package com.kafka.message.server.example.core;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

/**
 * Created by david on 2/27/16.
 */
public class KafkaMailConsumer extends Thread {
	private final KafkaConsumer<Integer, String> consumer;
    private boolean running = true;

	public KafkaMailConsumer(String topic, String offset, String groupId) {
		this(topic, offset, groupId, KafkaMailProperties.kafkaServerPort);
	}

	public KafkaMailConsumer(String topic, String offset, String groupId, int port) {
		consumer =  new KafkaConsumer<Integer, String>(createConsumerConfig(offset, groupId, port));
		consumer.subscribe(Collections.singletonList(topic));
	}

	/**
	 * Creates the consumer config.
	 *
	 * @return the consumer config
	 */
	private static Properties createConsumerConfig(String offset, String groupId, int port) {
		Properties props = new Properties();
		props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
				KafkaMailProperties.kafkaServerURL+":"+port);
		props.put("group.id", groupId);
		props.put("auto.commit.interval.ms", "1000");
		props.put("auto.offset.reset", offset);
		props.put("key.deserializer", "com.kafka.message.server.example.adapt.IntegerDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		return props;
	}

	public void setRunning(boolean running) {
		this.running = running;
	}

	public void notifyMessage(Integer key, String value){
		System.out.println("Consumer: Received mail (" + key + ") " + value);
	}

	public void run() {
		while(running) {
			ConsumerRecords<Integer, String> records = consumer.poll(1000);
			for (ConsumerRecord<Integer, String> record : records) {
				notifyMessage(record.key(), record.value());
			}
		}
	}
}
