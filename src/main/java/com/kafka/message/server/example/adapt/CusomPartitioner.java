package com.kafka.message.server.example.adapt;

/**
 * Created by david on 2/27/16.
 */

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class CusomPartitioner implements Partitioner {

    public CusomPartitioner (VerifiableProperties props) {

    }

    public int partition(Object key, int a_numPartitions) {
        int partition = 0;
        Integer ikey = (Integer) key;
        return ikey % a_numPartitions;
    }

}