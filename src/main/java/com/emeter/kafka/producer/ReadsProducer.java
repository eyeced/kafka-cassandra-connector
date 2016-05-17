package com.emeter.kafka.producer;

import com.emeter.kafka.data.Read;
import com.emeter.kafka.serialize.JsonSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.sql.Date;
import java.time.Instant;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ThreadFactory;
import java.util.stream.IntStream;

/**
 * Created by abhiso on 4/4/16.
 */
public class ReadsProducer {

    private Producer<String, String> producer;

    private static final ReadsProducer INSTANCE = new ReadsProducer();

    /**
     * @return the created instance
     */
    public static ReadsProducer getInstance() {
        return INSTANCE;
    }

    private ReadsProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);
    }

    /**
     * send message
     * @param json
     * @param key
     */
    public void send(String json, String key) {
        System.out.println(json);
        producer.send(new ProducerRecord<>("new-reads", key, json));
    }

    public static void main(String[] args) {

        String str = "{\"type\":\"com.emeter.kafka.data.Read\",\"meterId\":6,\"value\":0.5982115626434001,\"readTime\":1461641997924,\"flag\":1}";

        ReadsProducer producer = ReadsProducer.getInstance();
        JsonSerializer serializer = new JsonSerializer();
//        System.out.println(serializer.fromJson(str, Read.class));
        Random random = new Random();
        while (true) {
            IntStream.range(1, 10)
                    .mapToObj(i -> new Read(Long.valueOf(i), 1l, random.nextDouble(), Date.from(Instant.now()), 1l))
                    .map(read -> serializer.toJson(read))
                    .forEach(s -> producer.send(s, String.valueOf(random.nextInt(10))));
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
