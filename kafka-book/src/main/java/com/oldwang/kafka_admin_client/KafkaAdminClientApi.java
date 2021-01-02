package com.oldwang.kafka_admin_client;


import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartitionInfo;
import org.omg.CORBA.TIMEOUT;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * @author oldwang
 * 使用KafkaAdminClient操作topic
 */
public class KafkaAdminClientApi {

    static String brokerList = "localhost:9092";
    static String topic = "kafka-admin";

    public static void main(String[] args) {
//        createTopicDemo();
        describeTopicDemo();
//        listTopicDemo();
//        deleteTopicDemo();
    }

    private static void deleteTopicDemo() {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        AdminClient client = KafkaAdminClient.create(properties);
        DeleteTopicsOptions options = new DeleteTopicsOptions();
        options.timeoutMs(10000);
        DeleteTopicsResult deleteTopics = client.deleteTopics(Collections.singleton(topic), options);
        try {
            Set<Map.Entry<String, KafkaFuture<Void>>> entrySet = deleteTopics.values().entrySet();
            for (Map.Entry<String, KafkaFuture<Void>> entry : entrySet) {
                String key = entry.getKey();
                Void aVoid = entry.getValue().get();
                System.out.println(key+"_"+aVoid);
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

    }

    /**
     * 列出可用的topic
     */
    private static void listTopicDemo() {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        AdminClient client = KafkaAdminClient.create(properties);
        ListTopicsResult listTopicsResult = client.listTopics();
        try {
            Set<String> strings = listTopicsResult.names().get();
            strings.forEach(System.out::println);
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        client.close();
    }

    /**
     * 展示topic详细信息
     */
    private static void describeTopicDemo() {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        AdminClient client = KafkaAdminClient.create(properties);
        DescribeTopicsResult result = client.describeTopics(Collections.singleton("kafka-admin"));
        try {
            KafkaFuture<Map<String, TopicDescription>> all = result.all();
            Map<String, TopicDescription> maps = result.all().get();
            Set<Map.Entry<String, TopicDescription>> entries = maps.entrySet();
            entries.forEach(map -> {
                String key = map.getKey();
                TopicDescription value = map.getValue();
                String name = value.name();
                List<TopicPartitionInfo> partitions = value.partitions();
                partitions.forEach(partition -> {
                    System.out.println(key+"_"+name+"_"+partition);
                });
            });

        } catch (InterruptedException | ExecutionException e ) {
            e.printStackTrace();
        }
        client.close();
    }

    /**
     * 创建topic
     */
    private static void createTopicDemo() {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        AdminClient client = KafkaAdminClient.create(properties);
        //创建topic 该topic 3个分区1个副本
        NewTopic newTopic = new NewTopic(topic,3,(short) 1);
        //设置配置信息
        Map<String, String> configs = new HashMap<>();
        configs.put("cleanup.policy","compact");
        newTopic.configs(configs);
        CreateTopicsResult result = client.createTopics(Collections.singleton(newTopic));
        try {
            result.all().get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        client.close();
    }
}
