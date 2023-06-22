package com.dtxy.sync.dm2orcl;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.Future;

public class KafkaProducerService {
    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerService.class);
    private static KafkaProducer<String, String> producer;

    public KafkaProducerService(String bootstrapSer) {
        // 创建 Kafka 生产者
        createProducer(bootstrapSer);

        // 注册进程关闭钩子
        registerShutdownHook();
    }

    public void sendMessage(String topic, String message) {
        // 创建消息记录
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, message);

        // 发送消息并获取Future对象
        Future<RecordMetadata> future = producer.send(record);

        // 可选：如果需要等待消息发送完成并获取确认结果，可以使用下面的代码
        try {
            RecordMetadata metadata = future.get();
            logger.debug("消息发送成功，偏移量：{}", metadata.offset());
        } catch (Exception e) {
            logger.error("出错了：{}", e.getMessage());
        }
    }

    private static void createProducer(String bootstrapSer) {
        // 配置Kafka生产者的属性
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapSer); // Kafka broker的地址
        props.put("key.serializer", StringSerializer.class.getName()); // 键的序列化器
        props.put("value.serializer", StringSerializer.class.getName()); // 值的序列化器

        producer = new KafkaProducer<>(props);
    }

    private static void registerShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // 关闭 Kafka 生产者
            closeProducer();
        }));
    }

    private static void closeProducer() {
        if (producer != null) {
            producer.close();
            logger.info("{} ", "Kafka生产端已关闭！");
        }
    }
}


