package com.itheima.report.util;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class RoundRobinPartitioner implements Partitioner {

    // 并发包下的线程安全的整型类
    AtomicInteger counter = new AtomicInteger(0);

    // 返回值为分区号 0 1 2
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

        // 获取分区数量
        Integer partitions = cluster.partitionCountForTopic(topic);

        int curpartition = counter.incrementAndGet() % partitions;

        if(counter.get() > 65535){
            counter.set(0);
        }

        return curpartition;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
