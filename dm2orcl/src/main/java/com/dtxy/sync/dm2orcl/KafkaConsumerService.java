package com.dtxy.sync.dm2orcl;

import ch.qos.logback.classic.util.ContextInitializer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerService {
    private static Logger logger;
    private static Consumer<String, String> consumer;

    public static void main(String[] args) throws IOException {
        //加载配置文件
        ConfigUtil.loadConfigFile("config/config.properties");
        // 设置 Logback 配置文件的位置
        System.setProperty(ContextInitializer.CONFIG_FILE_PROPERTY, ConfigUtil.getFilePath("log.config.path"));

        logger = LoggerFactory.getLogger(KafkaConsumerService.class);
        // 初始化日志记录器
        logger.info("{} ", "Kafka消费端已启动！");

        // 创建 Kafka 消费者
        createConsumer();

        // 注册进程关闭钩子
        registerShutdownHook();

        // 执行消费逻辑
        consumeMessages();

    }

    private static void createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ConfigUtil.getProperty("kafka.bootstrap.servers"));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, ConfigUtil.getProperty("kafka.consumer.group"));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singleton(ConfigUtil.getProperty("kafka.logmnr.topic")));
    }

    private static void registerShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // 关闭 Kafka 消费者
            closeConsumer();
        }));
    }

    private static void closeConsumer() {
        if (consumer != null) {
            consumer.close();
            logger.info("{} ", "Kafka消费端已关闭！");
        }
    }

    private static void consumeMessages() {
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    // 处理JSON数据，这里只是简单地打印出来
                    logger.debug("消费消息:{} ", record.value());
                    //交给Oracle写入处理器
                    //OracleWriter.sync2Oracle(record.value());
                }
            }
        } finally {
            closeConsumer();
        }
    }
}

