package com.jyenes.kafka.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class DataDomainConsumer {

    private static final Logger log = LoggerFactory.getLogger(DataDomainConsumer.class);

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "jyenes-group");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.intervals.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        try (Consumer<String, String> consumer = new KafkaConsumer<String, String>(props);) {
            consumer.subscribe(Arrays.asList("jyenes-topic"));
            while (true) {
                ConsumerRecords<String, String> consumerRecords= consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> cr: consumerRecords) {

                log.info("Offset = {}, Partition = {}, Key = {}, Value ={}", cr.offset(), cr.partition(), cr.key(), cr.value());
                }
            }
        }
    }
}
