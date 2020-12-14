package com.oldwang.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author oldwang
 * kafka消费者
 */
public class KafkaConsumerAnalysis {
    private static final String brokerList = "localhost:9092";
    private static final String topic = "kafka-demo";
    private static final String groupId = "group.demo";
    private static final AtomicBoolean isRunning = new AtomicBoolean(true);

    public static void main(String[] args) {
        Properties properties = initConfig();
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);
        //订阅主题
        consumer.subscribe(Collections.singletonList(topic));

        List<TopicPartition> partitions = new ArrayList<>();
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
        partitionInfos.forEach(partitionInfo -> {
            partitions.add(new TopicPartition(partitionInfo.topic(),partitionInfo.partition()));
        });
        //指定订阅那些分区
        consumer.assign(partitions);
        //指定订阅具体分区 如果订阅多个 最后一个生效
        consumer.assign(Arrays.asList(new TopicPartition(topic,0)));
        try{
            while (isRunning.get()){
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("topic="+record.topic() +"-"+record.partition());
                    System.out.println("offset="+record.offset()+"-"+record.key()+"-"+record.value());
                }
            }

        }catch (Exception e){
            e.printStackTrace();
        }finally {
            consumer.close();
        }

    }


    private static Properties initConfig(){
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,brokerList);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG,"consumer.client.id.demo");
        return properties;
    }

}
