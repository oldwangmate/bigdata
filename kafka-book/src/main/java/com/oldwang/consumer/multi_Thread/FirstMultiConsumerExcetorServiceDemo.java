package com.oldwang.consumer.multi_Thread;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class FirstMultiConsumerExcetorServiceDemo {

    public static final String brokerList = "localhost:9092";
    public static final String topic = "kafka-demo";
    public static final String groupId = "bigdata";
    static Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
    public static Properties initConfig() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        return properties;
    }

    public static void main(String[] args) {
        Properties properties =  initConfig();
        KafkaThreadConsumer kafkaThreadConsumer = new KafkaThreadConsumer(topic,properties,Runtime.getRuntime().availableProcessors());
        kafkaThreadConsumer.start();
    }

    static class KafkaThreadConsumer extends Thread{
        private KafkaConsumer<String,String> kafkaConsumer;
        private ExecutorService executorService;
        private int threadNumber;

        public KafkaThreadConsumer(String topic,Properties properties,int ThreadNumber){
            this.kafkaConsumer = new KafkaConsumer<>(properties);
            this.kafkaConsumer.subscribe(Collections.singleton(topic));
            this.threadNumber = ThreadNumber;
            executorService = new ThreadPoolExecutor(ThreadNumber,ThreadNumber,0L,
                    TimeUnit.MICROSECONDS,new ArrayBlockingQueue<>(1000),new ThreadPoolExecutor.CallerRunsPolicy());
        }
        @Override
        public void run() {
            while (true){
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));
                if(!records.isEmpty()){
                    executorService.submit(new RecordHander(records));
                    synchronized (offsets){
                        if(!offsets.isEmpty()){
                            kafkaConsumer.commitSync();
                        }
                    }
                }
            }
        }
    }

    static class RecordHander extends Thread{
        public final ConsumerRecords<String,String> consumerRecords;
        public RecordHander(ConsumerRecords<String,String> record){
            this.consumerRecords = record;
        }

        @Override
        public void run() {
            for (TopicPartition partition : consumerRecords.partitions()) {
                List<ConsumerRecord<String, String>> tprecords = consumerRecords.records(partition);
                //处理tpRecords
                long offset = tprecords.get(tprecords.size() -1 ).offset();
                synchronized (offsets){
                    if(!offsets.containsKey(tprecords)){
                        offsets.put(partition,new OffsetAndMetadata(offset+1));
                    }else {
                        long offset1 = offsets.get(partition).offset();
                        if(offset1 < offset +1){
                            offsets.put(partition,new OffsetAndMetadata(offset +1));
                        }
                    }
                }
                for (ConsumerRecord<String, String> tprecord : tprecords) {
                    System.out.println(tprecord.value());
                }
            }
        }
    }
}
