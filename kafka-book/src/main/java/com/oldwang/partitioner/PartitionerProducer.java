package com.oldwang.partitioner;

import com.oldwang.utils.GetProducerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @author oldwang
 */
public class PartitionerProducer {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = GetProducerConfig.initConfig();
        //设置自定义分区器
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,CustomPartitioner.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        //构建所需要发送的消息
        ProducerRecord<String,String> record = new ProducerRecord<>("kafka-demo","hello kafka");
        //发送消息
        RecordMetadata recordMetadata = producer.send(record).get();
        System.out.println(recordMetadata.partition());
        //关闭生产者客户端
        producer.close();
    }
}
