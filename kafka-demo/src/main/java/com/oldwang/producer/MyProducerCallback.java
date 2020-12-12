package com.oldwang.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * @author oldwang
 */
public class MyProducerCallback {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<>("first", 2, "oldewang", "test--" + i), (recordMetadata, exception) -> {
                if(exception == null){
                    String topic = recordMetadata.topic();
                    int partition = recordMetadata.partition();
                    long offset = recordMetadata.offset();
                    System.out.println(topic+"--"+partition+"--"+offset);
                }else {
                    exception.printStackTrace();
                }
            });
        }
        producer.close();
    }
}
