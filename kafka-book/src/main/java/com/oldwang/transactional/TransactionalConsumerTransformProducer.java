package com.oldwang.transactional;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;
import java.util.List;

/**
 * @author oldwang
 * kafka 消费--转换--生产模式示例
 */
public class TransactionalConsumerTransformProducer {

    public static final String brokerList = "localhost:9092";

    public static Properties getConsumerProperties() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "groupId");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return properties;
    }

    public static Properties getProducerProperties() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transactionalId");
        return properties;
    }

    public static void main(String[] args) {
        //初始化消费者和生产者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getConsumerProperties());
        consumer.subscribe(Collections.singleton("kafka-demo"));

        KafkaProducer<String, String> producer = new KafkaProducer<>(getProducerProperties());

        //初始化事物
        producer.initTransactions();

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            if (!records.isEmpty()) {
                Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

                //开启事物
                producer.beginTransaction();
                try {
                    for (TopicPartition partition : records.partitions()) {

                        List<ConsumerRecord<String, String>> consumerRecords = records.records(partition);
                        for (ConsumerRecord<String, String> record : consumerRecords) {
                            //对消息处理
                            String value = record.value() + "log";

                            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(record.topic(), record.key(), value);
                            //消费-生产模型
                            producer.send(producerRecord);
                        }
                        long offset = consumerRecords.get(consumerRecords.size() - 1).offset();
                        offsets.put(partition, new OffsetAndMetadata(offset + 1));
                    }
                    //提交消息位移
                    producer.sendOffsetsToTransaction(offsets, "groupId");
                    //提交事物
                    producer.commitTransaction();
                } catch (Exception e) {
                    //终止事物
                    producer.abortTransaction();
                    e.printStackTrace();
                }
            }
        }
    }
}
