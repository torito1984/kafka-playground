package com.kafka.message.server.example.adapt;

import kafka.serializer.Decoder;
import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.Map;

/**
 * Created by david on 2/27/16.
 */
public class IntegerSerializer implements Serializer<Integer> {

    public IntegerSerializer(){
    }

    public void configure(Map<String, ?> configs, boolean isKey) {
        // nothing to do
    }

    @Override
    public byte[] serialize(String topic, Integer integer) {
        if (integer == null)
            return null;

        return new byte[] {
                (byte) (integer >>> 24),
                (byte) (integer >>> 16),
                (byte) (integer >>> 8),
                integer.byteValue()
        };
    }

    public void close() {
        // nothing to do
    }
}
