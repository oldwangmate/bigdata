package com.oldwang.transactional;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author oldwang
 * kafka事物发送消息
 */
public class KafkaProducerTransactional {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,"transactionalId");

        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);

        //初始化事物
        producer.initTransactions();
        //开启事物
        producer.beginTransaction();

        try {
            //业务逻辑处理
            ProducerRecord<String,String> record1 = new ProducerRecord<>("kafka-demo","msg1");
            producer.send(record1);
            ProducerRecord<String,String> record2 = new ProducerRecord<>("kafka-demo","msg2");
            producer.send(record2);
            ProducerRecord<String,String> record3 = new ProducerRecord<>("kafka-demo","msg3");
            producer.send(record3);

            //提交事物
            producer.commitTransaction();
        }catch (Exception e){
            e.printStackTrace();
            //终止事物
            producer.abortTransaction();
        }
        producer.close();
    }
}
