package com.oldwang.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author oldwang
 */
public class ProducerFastStart {

    private static final String  BROKER_LIST= "localhost:9092";
    private static final String TOPIC = "kafka-demo";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("bootstrap.servers",BROKER_LIST);
        //配置生产者客户端参数并创建kafkaProducer实例
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        //构建所需要发送的消息
        ProducerRecord<String,String> record = new ProducerRecord<>(TOPIC,"hello kafka");
        //发送消息
        producer.send(record);
        //关闭生产者客户端
        producer.close();

    }
}
