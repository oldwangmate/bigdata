package com.oldwang.custom_serializer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;


/**
 * @author oldwang
 */
public class CustomSerializerConsumer {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,CustomUserDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"user");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        KafkaConsumer<String,User> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singleton("user"));
        while (true){
            ConsumerRecords<String, User> records = consumer.poll(Duration.ofMillis(1000));
            records.forEach(record -> {
                User user = record.value();
                System.out.println(user.toString());
            });
        }
    }
}
