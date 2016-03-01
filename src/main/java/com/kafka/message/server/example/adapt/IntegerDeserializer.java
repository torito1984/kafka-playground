package com.kafka.message.server.example.adapt;

import kafka.serializer.Decoder;
import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * Created by david on 2/27/16.
 */
public class IntegerDeserializer implements Deserializer<Integer> {

    public IntegerDeserializer(){
    }

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public Integer deserialize(String s, byte[] bytes) {
        if (bytes == null)
            return null;
        if (bytes.length != 4) {
            throw new SerializationException("Size of data received by IntegerDeserializer is not 4: " + bytes.length);
        }

        int value = 0;
        for (byte b : bytes) {
            value <<= 8;
            value |= b & 0xFF;
        }
        return value;
    }

    public void close() {
        // nothing to do
    }
}
