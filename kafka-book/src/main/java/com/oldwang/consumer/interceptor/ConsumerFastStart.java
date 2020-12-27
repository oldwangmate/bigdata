package com.oldwang.consumer.interceptor;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * @author oldwang
 */
public class ConsumerFastStart {
    private static final String BROKER_LIST = "localhost:9092";
    private static final String TOPIC = "kafka-demo";
    private static final String GROUP_ID = "group.demo";

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("bootstrap.servers",BROKER_LIST);
        //指定拦截器
        properties.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,ConsumerInterceptorTTL.class.getName());
        //设置消费组的名称
        properties.put("group.id",GROUP_ID);
        //  设置消费者的客户端实例
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);
        //订阅主题
        consumer.subscribe(Collections.singleton(TOPIC));
        //循环消费消息
        while (true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            records.forEach(record -> {
                System.out.println(record.value());
            });

        }


    }
}
