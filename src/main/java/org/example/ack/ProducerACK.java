package org.example.ack;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerACK {
    public static void main(String[] args) {

        // 0
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.96.131:9092");

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //parameters
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384); // 批次大小16K
        // 压缩 gzip\snappy\lz4\zstd
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, CompressionType.SNAPPY.name);
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);// 缓冲区大小64M

        //ack
        properties.put(ProducerConfig.ACKS_CONFIG, "1");
        //ACK-RETRY
        properties.put(ProducerConfig.RETRIES_CONFIG, 3);

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 3; i++) {
            ProducerRecord record = new ProducerRecord<>("first", i + "--test");
            producer.send(record);
        }

        producer.close();
    }
}