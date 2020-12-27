package com.oldwang.consumer.interceptor;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @author oldwang
 */
public class ProducerFastStart {

    private static final String  BROKER_LIST= "localhost:9092";
    private static final String TOPIC = "kafka-demo";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        KafkaProducer<String, String> producer = new KafkaProducer<>(initConfig());
        ProducerRecord<String,String> record = new ProducerRecord<>(TOPIC,0,System.currentTimeMillis() - 10 * 1000,null,"first-expire-data");
        //发送消息
        producer.send(record).get();
        ProducerRecord<String,String> record1 = new ProducerRecord<>(TOPIC,0,System.currentTimeMillis(),null,"no-data");
        //发送消息
        producer.send(record1).get();
        //关闭生产者客户端
        producer.close();

    }

    //封装配置文件
    private static Properties initConfig(){
        Properties properties = new Properties();
        //序列化方式 全限定类名
        properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        //可通过类名获取 提高代码简洁度
        properties.put("value.serializer", StringSerializer.class.getName());
        properties.put("bootstrap.servers",BROKER_LIST);
        return properties;
    }
}
