package com.kafka.message.server.example.core;

/**
 * Created by david on 2/27/16.
 */
public interface KafkaMailProperties{
	final static String topic = "topic1";
	final static String groupId = "group1";
	final static String zkConnect = "localhost:2181";
    final static String kafkaServerURL = "localhost";
    final static int kafkaServerPort = 6667;

    final static int kafkaProducerBufferSize = 64*1024;
    final static int connectionTimeOut = 100000;
    final static int reconnectInterval = 10000;
}
