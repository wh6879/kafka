package org.example.transaction;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerTransaction {
    public static void main(String[] args) {

        // 0
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.96.137:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "tranaction_id_01");// 1
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        producer.initTransactions();// 2
        producer.beginTransaction();// 3

        try {
            for (int i = 0; i < 3; i++) {
                ProducerRecord record = new ProducerRecord<>("first", i + "--tran");
                producer.send(record);
            }
            producer.commitTransaction();// 4
        } catch (Exception e) {
            producer.abortTransaction();// 5
        } finally {
            producer.close();
        }
    }
}