package com.itheima.report.util;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration // 表示该类是一个配置类
public class KafkaProducerConfig {

    //    #kafka的服务器地址
    @Value("${kafka.bootstrap_servers_config}")
    private String bootstrap_servers_config;

    // #如果出现发送失败的情况，允许重试的次数
    @Value("${kafka.retries_config}")
    private String retries_config;
    // #每个批次发送多大的数据
    @Value("${kafka.batch_size_config}")
    private String batch_size_config;
    // #定时发送，达到1ms发送
    @Value("${kafka.linger_ms_config}")
    private String linger_ms_config;
    //  #缓存的大小
    @Value("${kafka.buffer_memory_config}")
    private String buffer_memory_config;
    //  #TOPIC的名字
    @Value("${kafka.topic}")
    private String topic;

    @Bean // 表示该对象是受Spring所管理的一个Bean
    public KafkaTemplate kafkaTemplate() {

        // 构建工厂需要的配置
        Map<String, Object> configs = new HashMap<>();

        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_servers_config);
        configs.put(ProducerConfig.RETRIES_CONFIG, retries_config);
        configs.put(ProducerConfig.BATCH_SIZE_CONFIG, batch_size_config);
        configs.put(ProducerConfig.LINGER_MS_CONFIG, linger_ms_config);
        configs.put(ProducerConfig.BUFFER_MEMORY_CONFIG, buffer_memory_config);

        // 设置key value的序列化器
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // 指定自定义分区
        configs.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, RoundRobinPartitioner.class);


        // 创建生产者工厂
        ProducerFactory<String, String> producerFactory = new DefaultKafkaProducerFactory(configs);

        // 要返回一个KafkTemplate的对象
        return new KafkaTemplate(producerFactory);
    }

}
