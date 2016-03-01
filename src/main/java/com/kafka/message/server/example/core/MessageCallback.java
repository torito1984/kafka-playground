package com.kafka.message.server.example.core;

/**
 * Created by osboxes on 3/1/16.
 */
public interface MessageCallback<K,V> {
    void notify(K key, V value);
}
