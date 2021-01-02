package com.oldwang.kafka_admin_client;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.config.ConfigResource;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * @author oldwang
 * 使用api操作topic配置信息
 */
public class KafkaAdminClientConfigApi {
    static String brokerList = "localhost:9092";
    static String topic = "kafka-admin";

    public static void main(String[] args) {
//        describeTopicConfig();
//        alterTopicConfig();
        createPartitions();
    }

    /**
     * topic增加分区
     */
    private static void createPartitions() {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,brokerList);
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG,30000);
        AdminClient client = KafkaAdminClient.create(properties);
        Map<String, NewPartitions> partitions = new HashMap<>();
        partitions.put(topic,NewPartitions.increaseTo(5));
        CreatePartitionsResult result = client.createPartitions(partitions);
        try {
            result.all().get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        client.close();
    }

    /**
     * 修改topic配置
     */
    private static void alterTopicConfig() {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,brokerList);
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG,30000);
        AdminClient client = KafkaAdminClient.create(properties);
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC,topic);
        ConfigEntry configEntry = new ConfigEntry("cleanup.policy","compact");
        Config config = new Config(Collections.singleton(configEntry));
        Map<ConfigResource, Config> configs = new HashMap<>();
        configs.put(resource,config);
        AlterConfigsResult result = client.alterConfigs(configs);
        try {
            result.all().get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        client.close();
    }

    /**
     * 查看topic详细配置信息
     */
    private static void describeTopicConfig() {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,brokerList);
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG,30000);
        AdminClient client = KafkaAdminClient.create(properties);
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC,topic);
        DescribeConfigsResult result = client.describeConfigs(Collections.singleton(resource));
        Map<ConfigResource, Config> map = null;
        try {
            map = result.all().get();
            Config config = map.get(resource);
            System.out.println(config);
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
      client.close();
    }
}
