package com.oldwang.consumer;


import com.oldwang.custom_serializer.CustomUserSerializer;
import com.oldwang.custom_serializer.User;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author oldwang
 *
 */
public class CustomSerializerProducer {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //使用自定义序列化器
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,ProducerProtostuffSerializser.class.getName());
        KafkaProducer<String, User> producer = new KafkaProducer<>(properties);
        Future<RecordMetadata> future = producer.send(new ProducerRecord<>("user", new User(1, "oldwang")));
        String topic = future.get().topic();
        System.out.println(topic);
        producer.close();
    }
}
