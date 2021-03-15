package com.jyenes.kafka.callbacks;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class DataDomainCallbackProducer {

    public static final Logger log = LoggerFactory.getLogger(DataDomainCallbackProducer.class);

    public static void main(String[] args) {
        long starTime = System.currentTimeMillis();
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("linger.ms", "10");

        try (Producer<String, String> producer = new KafkaProducer<String, String>(props)) {
            for (int i = 0; i < 100; i++) {
                // async way of sending messages
                // key is for warranty the order, same key goes to the same partition
                producer.send(new ProducerRecord<>("jyenes-topic", (i % 2 == 0)? "key-1" : "key-2",String.valueOf(i)), (metadata, exception) -> {
                    if (exception != null) {
                        log.info("there was an error {}", exception.getMessage());
                    }
                    log.info("Callback: Offset = {}, Partition = {}, Topic = {}", metadata.offset(), metadata.partition(), metadata.topic());
                });
            }
            producer.flush();
        }
        log.info("Processing time = {} ms", (System.currentTimeMillis() - starTime));
    }

}
