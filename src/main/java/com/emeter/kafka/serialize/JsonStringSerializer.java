package com.emeter.kafka.serialize;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;

import java.io.IOException;

/**
 * Created by abhiso on 1/20/16.
 */
public class JsonStringSerializer {

    private static ObjectMapper objectMapper = new ObjectMapper();

    public <T> String serialize(T t, Class<T> clazz) {
        ObjectWriter objectWriter = objectMapper.writerWithView(clazz);
        try {
            return objectWriter.writeValueAsString(t);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public <T> T deserialize(String json, Class<T> clazz) {
        ObjectReader reader = objectMapper.reader(clazz);
        try {
            return reader.readValue(json);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
