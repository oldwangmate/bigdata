package com.oldwang.intercept;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author naver
 */
public class TimeInterceptor implements ProducerInterceptor<String,String> {


    @Override
    public void configure(Map<String, ?> configs) {

    }

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        //取出数据
        String value = record.value();
        value = System.currentTimeMillis()+","+value;
        //创建对象并返回
        return new ProducerRecord<String,String>(record.topic(),record.partition(),record.key(),value);
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

    @Override
    public void close() {

    }

}
