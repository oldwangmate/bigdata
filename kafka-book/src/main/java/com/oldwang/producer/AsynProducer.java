package com.oldwang.producer;

import com.oldwang.utils.GetProducerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * @author oldwang
 * 异步发送多条小心
 */
public class AsynProducer {

    /**
     * 使用Callback方式 简单明了
     * kafka有响应时就会回调， 要么发送成功，要么发送失败
     * @param args
     */
    public static void main(String[] args) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(GetProducerConfig.initConfig());
        int count = 0;
        while (count < 100){
            producer.send(new ProducerRecord<>("kafka-demo", "asyn-kafka-"+count), (metadata,exception) -> {
                if(exception != null){
                    System.out.println("消息发送异常");
                    exception.printStackTrace();
                }else {
                    String topic = metadata.topic();
                    System.out.println(topic);
                }
            });
            count++;
        }

        producer.close();
    }
}
