package com.kafka.message.server.example.core;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.kafka.message.server.example.adapt.IntegerEncoderDecoder;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

/**
 * Created by david on 2/27/16.
 */
public class KafkaMailConsumer extends Thread {
	private final ConsumerConnector consumer;
	private final String topic;
    private IntegerEncoderDecoder keyDecoder;

	public KafkaMailConsumer(String topic, String offset, String groupId) {
		consumer = Consumer.createJavaConsumerConnector(createConsumerConfig(offset, groupId));
		this.topic = topic;
		keyDecoder = new IntegerEncoderDecoder();
	}

	/**
	 * Creates the consumer config.
	 *
	 * @return the consumer config
	 */
	private static ConsumerConfig createConsumerConfig(String offset, String groupId) {
		Properties props = new Properties();
		props.put("zookeeper.connect", KafkaMailProperties.zkConnect);
		props.put("group.id", groupId);
		props.put("zookeeper.session.timeout.ms", "400");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");
		props.put("auto.offset.reset", offset);

		return new ConsumerConfig(props);
	}

	public void run() {
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, new Integer(1));
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
		KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
		ConsumerIterator<byte[], byte[]> it = stream.iterator();
		while (it.hasNext()) {
			MessageAndMetadata<byte[], byte[]> message = it.next();
			System.out.println("Consumer: Received mail (" + keyDecoder.fromBytes(message.key()) + ") " + new String(message.message()));
		}
	}
}
