package com.oldwang.interceptor;

import com.oldwang.utils.GetProducerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @author oldwang
 * 严重自定义拦截器
 */
public class InterceptorProducer {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = GetProducerConfig.initConfig();
        //设置自定义拦截器(可指定多个拦截器)
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,CustomProducerInterceptor.class.getName()+","+CustomProducerInterceptorPlus.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        //构建所需要发送的消息
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String,String> record = new ProducerRecord<>("kafka-demo","hello kafka -"+i);
            //发送消息
            RecordMetadata recordMetadata = producer.send(record).get();
            System.out.println(recordMetadata.partition());
        }
        //关闭生产者客户端
        producer.close();
    }
}
