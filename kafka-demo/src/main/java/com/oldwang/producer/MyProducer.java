package com.oldwang.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author oldwang
 */
public class MyProducer {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        //指定kafka集群链接  bootstrap.servers
        properties.put("bootstrap.servers","localhost:9092");
        //ack应答机制
        properties.put("acks","all");
        //重试次数
        properties.put("delivery.timeout.ms", 30000);
        //批次大小
        properties.put("batch.size", 16384);
        //缓冲区大小
        properties.put("buffer.memory", 33554432);
        //序列化
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        for (int i = 0; i < 10; i++) {
            Future<RecordMetadata> future = producer.send(new ProducerRecord<String, String>("first", "MyProducer--" + i));
            RecordMetadata metadata = future.get();
            long offset = metadata.offset();
            System.out.println(offset);
        }
        producer.close();
    }
}
