package com.oldwang.consumer.interceptor;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author oldwang
 * 消费者拦截器
 */
public class ConsumerInterceptorTTL implements ConsumerInterceptor<String,String> {

    private static final int expire_interval = 10 * 1000;
    @Override
    public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> records) {
        Map<TopicPartition, List<ConsumerRecord<String, String>>> newrecords = new HashMap<>();

        Long now = System.currentTimeMillis();
        for (TopicPartition partition : records.partitions()) {
            List<ConsumerRecord<String, String>> TpReCords = records.records(partition);

            List<ConsumerRecord<String,String>> list = new ArrayList<>();

            for (ConsumerRecord<String, String> reCord : TpReCords) {
                if(now - reCord.timestamp() < expire_interval ){
                    list.add(reCord);
                }
                if(!list.isEmpty()){
                   newrecords.put(partition,list);
                }
            }

        }
        return new ConsumerRecords<>(newrecords);
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        offsets.forEach((tp,offset) -> {
            System.out.println(tp+":"+offset);
        });
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
