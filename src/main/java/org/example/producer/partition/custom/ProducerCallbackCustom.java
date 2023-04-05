package org.example.producer.partition.custom;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerCallbackCustom {
    public static void main(String[] args) {

        // 0
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.96.131:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, MyPartition.class);

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 3; i++) {
            ProducerRecord record = new ProducerRecord<>("first", i + "--test");
            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    System.out.println("topic :" + metadata.topic() + " partition: " + metadata.partition());
                }
            });
        }

        for (int i = 0; i < 3; i++) {
            ProducerRecord record = new ProducerRecord<>("first", i + "--abc");
            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    System.out.println("topic :" + metadata.topic() + " partition: " + metadata.partition());
                }
            });
        }

        for (int i = 0; i < 3; i++) {
            ProducerRecord record = new ProducerRecord<>("first", i + "--123");
            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    System.out.println("topic :" + metadata.topic() + " partition: " + metadata.partition());
                }
            });
        }

        producer.close();
    }
}