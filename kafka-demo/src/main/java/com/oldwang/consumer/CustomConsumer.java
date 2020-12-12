package com.oldwang.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

/**
 * @author oldwang
 */
public class CustomConsumer {

    public static void main(String[] args) {
        Properties properties  = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        //消费者组
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"bigdata");
        //重制消费者的offset 消费者换组才会生效
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        //关闭自动提交
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);
        //创建消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList("first"), new ConsumerRebalanceListener() {
            //重新分配分区之前调用 提交当前负责的分区offset
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                System.out.println("==============回收的分区=============");
                for (TopicPartition partition : partitions) {
                    System.out.println(partition);
                }

            }
            //重新分配分区之后调用  定位新分配的offset
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                for (TopicPartition partition : partitions) {
                    long offset = getPartitionOffSet(partitions);
                    consumer.seek(partition,offset);
                }
            }


        });
        //获取数据
        while (true) {
            ConsumerRecords<String, String> poll = consumer.poll(Duration.ofMillis(100));
            //解析
            for (ConsumerRecord<String, String> consumerRecord : poll) {
                String key = consumerRecord.key();
                String value = consumerRecord.value();
                System.out.println(key + "-----" + value);
                TopicPartition topicPartition = new TopicPartition(consumerRecord.topic(), consumerRecord.partition());
                commitOffSet(topicPartition,consumerRecord.offset()+1);
            }

        }
    }

    private static void commitOffSet(TopicPartition topicPartition, long offset) {
        System.out.println(topicPartition.topic()+"---"+offset);
    }

    private static long getPartitionOffSet(Collection<TopicPartition> partitions) {
        return 0;
    }
}
