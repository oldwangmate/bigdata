package com.oldwang.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author oldwang
 * 自定义分区器逻辑
 */
public class CustomPartitioner implements Partitioner {

    private final AtomicInteger counter = new AtomicInteger(0);
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitioner = partitions.size();
        if(null == keyBytes){
            return counter.getAndIncrement() % numPartitioner;
        }else {
            return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitioner;
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
