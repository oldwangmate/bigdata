package com.oldwang.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author oldwang
 */
public class PartitionerProducer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        //指定自定义分区类
        properties.put("partitioner.class","com.oldwang.partitioner.MyPartitioner");

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<>("first","oldwang","test--"+i),(matedata,exception) -> {
                if(exception == null){
                    String topic = matedata.topic();
                    int partition = matedata.partition();
                    long offset = matedata.offset();
                    System.out.println(topic+"--"+partition+"--"+offset);
                }else {
                    exception.printStackTrace();
                }
            });

        }
        producer.close();
    }
}
