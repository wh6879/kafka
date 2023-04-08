package org.example.topic;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class CreateTopic {

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.96.137:9092");

        AdminClient adminClient = AdminClient.create(properties);

        ArrayList<NewTopic> newTopics = new ArrayList<>();

        NewTopic newTopic = new NewTopic("newTopic", 12, (short) 2);

        newTopics.add(newTopic);

        CreateTopicsResult createTopicsResult = adminClient.createTopics(newTopics);

        try {
            createTopicsResult.all().get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }

    }
}
