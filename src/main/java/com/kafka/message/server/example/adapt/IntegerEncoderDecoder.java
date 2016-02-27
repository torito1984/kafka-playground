package com.kafka.message.server.example.adapt;

import kafka.serializer.Decoder;
import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;

/**
 * Created by david on 2/27/16.
 */
public class IntegerEncoderDecoder implements Encoder<Integer>, Decoder<Integer> {

    private String encoding = "UTF8";

    public IntegerEncoderDecoder(){
    }

    public IntegerEncoderDecoder(VerifiableProperties props){
    }

    @Override
    public Integer fromBytes(byte[] bytes) {
        try {
            return Integer.valueOf(new String(bytes, encoding));
        } catch (Exception e) {
           return 0;
        }
    }

    @Override
    public byte[] toBytes(Integer integer) {
        return Charset.forName("UTF-8").encode(integer.toString()).array();
    }
}
