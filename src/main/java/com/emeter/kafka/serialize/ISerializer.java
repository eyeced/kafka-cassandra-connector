package com.emeter.kafka.serialize;

/**
 * Created by abhiso on 1/6/16.
 */
public interface ISerializer<T> {

    byte[] serialize(T t, Class<T> clazz);

    T deserialize(byte[] bytes, Class<T> clazz);
}
