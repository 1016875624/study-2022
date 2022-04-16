package com.example.kafka.comsumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
// 消费者消费指定分区代码
public class KafkaComsumerPartitionDemo {
    public static void main(String[] args) {
        Properties properties = new Properties();
        // kafka集群地址
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // key反序列化器
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // value反序列化器
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 消费者组id  //必填
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test");

        // 创建消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        // 设置消费者订阅的分区
        List<TopicPartition> partitions = List.of(new TopicPartition("second", 0));
        consumer.assign(partitions);

        // 订阅主题
        // consumer.subscribe(List.of(""));

        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(10));
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord);
            }
        }
    }
}
