package org.example.consumer.seek;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

public class CustomConsumerSeekTime {

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

        // autooffset
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        //
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);

        // 1 创建消费者
        KafkaConsumer consumer = new KafkaConsumer(properties);

        // 2 订阅
        ArrayList<String> topics = new ArrayList<>();
        topics.add("first");
        consumer.subscribe(topics);

        // 指定位置消费
        Set<TopicPartition> assignment = consumer.assignment();
        // 保证分区分配完成?? 消费者异步分配方案流程
        while (assignment.size() == 0 ) {
            consumer.poll(Duration.ofSeconds(1));
            assignment = consumer.assignment();
        }

        HashMap<TopicPartition, Long> topicPartitionLongHashMap = new HashMap<>();
        for (TopicPartition topicPartition : assignment ) {
            // 1天谴
            topicPartitionLongHashMap.put(topicPartition,
                    System.currentTimeMillis() - 1 * 24 * 3600 * 1000);
        }

        Map<TopicPartition, OffsetAndTimestamp> topicPartitionOffsetAndTimestampMap =
                consumer.offsetsForTimes(topicPartitionLongHashMap);

        for (TopicPartition topicPartition : assignment ) {
            OffsetAndTimestamp offsetAndTimestamp =
                    topicPartitionOffsetAndTimestampMap.get(topicPartition);

            consumer.seek(topicPartition, offsetAndTimestamp.offset());
        }

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord record : records) {
                System.out.println(record);
            }

            consumer.commitSync();
        }

    }
}
