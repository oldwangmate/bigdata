package com.oldwang.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author oldwang
 * 自定义生产者拦截器
 */
public class CustomProducerInterceptorPlus implements ProducerInterceptor<String,String> {
    private volatile long success = 0;
    private volatile long error = 0;
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        String newValue = "CustomProducerInterceptor-2" + record.value();
        return new ProducerRecord<>(record.topic(),record.partition(),record.key(),newValue,record.headers());
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
