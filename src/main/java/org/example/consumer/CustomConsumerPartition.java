package org.example.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

public class CustomConsumerPartition {

    public static void main(String[] args) {
        // 0 配置
        Properties properties = new Properties();

        // 连接
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.96.137:9092");

        // 反序列化
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // groupid
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "GroupId");

        // 1 创建消费者
        KafkaConsumer consumer = new KafkaConsumer(properties);

        // 2 订阅
        ArrayList<TopicPartition> topicPartitions = new ArrayList<>();
        TopicPartition topicPartition = new TopicPartition("first", 0);
        topicPartitions.add(topicPartition);
        consumer.assign(topicPartitions);

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord record : records) {
                System.out.println(record);
            }
        }

    }
}
