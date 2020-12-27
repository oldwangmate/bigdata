package com.oldwang.consumer.seek;

import com.oldwang.utils.GetConsumerConfig;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;

/**
 * 指定消费移位
 */
public class SeekConsumer {

    public static void main(String[] args) {
//        Properties properties = GetConsumerConfig.initConfig();
//        properties.put("group.id","bigdata");
//        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);
//        consumer.subscribe(Collections.singletonList("kafka-demo"));
//        Set<TopicPartition> partitionSet = new HashSet<>();
//        //如果不为0说明已经分配到分区了
//        while (partitionSet.size() == 0){
//             consumer.poll(Duration.ofMillis(100));
//            partitionSet = consumer.assignment();
//        }
//        for (TopicPartition partition : partitionSet) {
//            //设置消费移位
//            consumer.seek(partition,1);
//        }
//        while (true){
//            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
//            for (ConsumerRecord<String, String> record : records) {
//                System.out.println(record.value());
//            }
//        }
        kafkaSeekForTime();
//        kafkaSeekBeginning();
//        kafkaSeekEndOffset();
//         kafkaoffsetDB();
    }

    private static void kafkaoffsetDB() {
        Properties properties = GetConsumerConfig.initConfig();
        properties.put("group.id","bigdata");
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);
        Map<TopicPartition, OffsetAndMetadata> currentOffset = new HashMap<>();
        //在均衡发生前可以通过监听器的会调方法执行同步提交尽量避免消费重复
        consumer.subscribe(Collections.singletonList("kafka-demo"), new ConsumerRebalanceListener() {
            //在均衡之前和消费者停止消费之后被调用
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                consumer.commitSync(currentOffset);
                currentOffset.clear();
            }
            //重新分配分区和消费者开始读取消费之前被调用
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                for (TopicPartition partition : partitions) {
                    //从DB中读取消费移位
                    consumer.seek(partition,getoffsetDb(partition));
                }
            }
        });
        Set<TopicPartition> assignment = new HashSet<>();
        while (assignment.size() == 0){
            consumer.poll(Duration.ofMillis(100));
            assignment = consumer.assignment();
        }
        for (TopicPartition topicPartition : assignment) {
            //从DB中获取offset
            long offset = getoffsetDb(topicPartition);
            consumer.seek(topicPartition,offset);
        }
        while (true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            Set<TopicPartition> partitions = records.partitions();
            for (TopicPartition partition : partitions) {
                List<ConsumerRecord<String, String>> recordList = records.records(partition);
                for (ConsumerRecord<String, String> record : recordList) {
                    System.out.println(record.value());
                    currentOffset.put(new TopicPartition(record.topic(),record.partition()),new OffsetAndMetadata(record.offset() +1));
                }
                long offset = recordList.get(recordList.size() -1).offset();
                offsetToDB(partition,offset+1);
            }

        }
    }

    private static long getoffsetDb(TopicPartition topicPartition) {
        System.out.println(topicPartition);
        return 400;
    }

    private static void offsetToDB(TopicPartition partition, long l) {
        System.out.println(partition);
        System.out.println(l);
    }




    //使用seek()从分区末尾消费
    public static void kafkaSeekEndOffset() {
        Properties properties = GetConsumerConfig.initConfig();
        properties.put("group.id", "bigdata");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Collections.singletonList("kafka-demo"));
        Set<TopicPartition> partitionSet = new HashSet<>();
        while (partitionSet.size() == 0) {
            consumer.poll(Duration.ofMillis(100));
            partitionSet = consumer.assignment();
        }
        //获取指定分区末尾的消息位置
        Map<TopicPartition, Long> topicPartitionLongMap = consumer.endOffsets(partitionSet);
        for (TopicPartition topicPartition : partitionSet) {
            consumer.seek(topicPartition, topicPartitionLongMap.get(topicPartition));
        }
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.value());
            }
        }
    }

    //使用seek()从分区开始消费
    public static void kafkaSeekBeginning() {
        Properties properties = GetConsumerConfig.initConfig();
        properties.put("group.id", "bigdata");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Collections.singletonList("kafka-demo"));
        Set<TopicPartition> partitionSet = new HashSet<>();
        while (partitionSet.size() == 0) {
            consumer.poll(Duration.ofMillis(100));
            partitionSet = consumer.assignment();
        }
        //获取指定分区末尾的消息位置
        Map<TopicPartition, Long> topicPartitionLongMap = consumer.beginningOffsets(partitionSet);
        for (TopicPartition topicPartition : partitionSet) {
            consumer.seek(topicPartition, topicPartitionLongMap.get(topicPartition));
        }

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.value());
            }
        }
    }

    //使用seek()根据时间消费
    public static void kafkaSeekForTime() {
        Properties properties = GetConsumerConfig.initConfig();
        properties.put("group.id", "bigdata");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Collections.singletonList("kafka-demo"));
        Set<TopicPartition> partitionSet = new HashSet<>();
        while (partitionSet.size() == 0) {
            consumer.poll(Duration.ofMillis(100));
            partitionSet = consumer.assignment();
        }
        Map<TopicPartition, Long> timestampToSearch = new HashMap<>();
        for (TopicPartition topicPartition : partitionSet) {
            timestampToSearch.put(topicPartition, System.currentTimeMillis() - 1 * 24 * 3600 * 1000);
        }
        //设置消费
        Map<TopicPartition, OffsetAndTimestamp> offsets = consumer.offsetsForTimes(timestampToSearch);
        for (TopicPartition topicPartition : partitionSet) {
            OffsetAndTimestamp timestamp = offsets.get(topicPartition);
            consumer.seek(topicPartition, timestamp.offset());
        }
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.value());
            }
        }
    }
}
