package com.kafka.message.server.example.adapt;

/**
 * Created by david on 2/27/16.
 */

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

public class CustomPartitioner implements Partitioner {

    public CustomPartitioner() {
    }

    @Override
    public void configure(Map<String, ?> map) {
        // nothing to do
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        Integer ikey = (Integer) key;
        return ikey % cluster.partitionCountForTopic(topic);
    }

    @Override
    public void close() {
        // nothing to do
    }
}