package com.oldwang.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author oldwang
 * 自定义生产者拦截器
 */
public class CustomProducerInterceptor implements ProducerInterceptor<String,String> {
    private volatile long success = 0;
    private volatile long error = 0;
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        String newValue = "CustomProducerInterceptor-" + record.value();
        return new ProducerRecord<>(record.topic(),record.partition(),record.key(),newValue,record.headers());
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if(exception == null){
            success ++;
        }else {
            error ++;
        }

    }

    @Override
    public void close() {
        double successRatio = success / (success+error);
        System.out.println("成功率="+ String.format("%f",successRatio * 100) + "%");
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
