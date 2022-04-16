package com.example.kafka.comsumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.util.ObjectUtils;

import java.time.Duration;
import java.util.*;

public class KafkaConsumerSeekTimeStamp {
    public static void main(String[] args) {
        // 配置
        Properties properties = new Properties();

        // 设置kafka集群
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // key 反序列化器
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        //value 反序列化器
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // 消费者组id
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test1");

        // 创建消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        consumer.subscribe(List.of("first"));

        Set<TopicPartition> assignment = consumer.assignment();
        // 如果为空 那么没有拉取到分区消费策略
        while (ObjectUtils.isEmpty(assignment)) {
            consumer.poll(Duration.ofSeconds(1));
            assignment = consumer.assignment();
        }

        // 将时间转换为 offset
        Map<TopicPartition, Long> map = new HashMap<>();
        for (TopicPartition topicPartition : assignment) {
            // 获取一天前的数据
            // 指定时间戳对应的offset
            map.put(topicPartition, System.currentTimeMillis() - 1 * 24 * 3600 * 1000);
        }
        Map<TopicPartition, OffsetAndTimestamp> timestampMap = consumer.offsetsForTimes(map);

        // 所有分区都按照100以后进行消费
        // 已经消费过的数据是不能狗再次消费的
        for (TopicPartition partition : assignment) {
//            consumer.seek(partition, 140);
            // 对应时间的对应offset
            consumer.seek(partition, timestampMap.get(partition).offset());
        }

        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(10));
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord);
            }
        }
    }
}
