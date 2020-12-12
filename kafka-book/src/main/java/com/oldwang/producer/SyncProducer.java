package com.oldwang.producer;

import com.oldwang.utils.GetProducerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.protocol.types.Field;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author oldwang
 * kafka同步发送消息
 */
public class SyncProducer {

    public static void main(String[] args) {
//        syncSendOne();
          syncSendTwo();
    }

    /**
     * 利用 future 阻塞方法 也可获取topic offset 等详细信息
     */
    private static void syncSendTwo() {
        KafkaProducer<String, String> producer = new KafkaProducer<>(GetProducerConfig.initConfig());
        ProducerRecord<String, String> record = new ProducerRecord<>("kafka-demo", "hello kafka");
        Future<RecordMetadata> future = producer.send(record);
        try {
            RecordMetadata recordMetadata = future.get();
            System.out.println(recordMetadata.offset());
            producer.close();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }


    private static void syncSendOne() {
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(GetProducerConfig.initConfig());
        ProducerRecord<String,String> record = new ProducerRecord<>("kafka-demo","hello kafka sync");
        try {
            producer.send(record).get();
            producer.close();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }
}
