package com.dtxy.sync.dm2orcl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicLong;

public class PositionRecorder {
    private static final Logger logger = LoggerFactory.getLogger(PositionRecorder.class);
    private static final String POSITION_FILE_PATH = ConfigUtil.getFilePath("monitor.position.file.path");
    private static final AtomicLong lastProcessedPosition = new AtomicLong(0);

    // 获取上次处理的位置
    public static long getLastProcessedPosition() {
        return lastProcessedPosition.get();
    }

    // 写入当前处理位置
    public static void recordPosition(long position) {
        lastProcessedPosition.set(position);
    }

    // 从文件中加载上次处理的位置
    public static void loadLastProcessedPosition() throws IOException {
        Path path = Path.of(POSITION_FILE_PATH);
        if (Files.exists(path)) {
            String content = Files.readString(path);
            long position = Long.parseLong(content);
            lastProcessedPosition.set(position);
            logger.info("从文件加载当前scn：{}", position);
        }
    }

    // 将当前处理位置写入文件
    public static synchronized void savePositionToFile() throws IOException {
        String content = String.valueOf(lastProcessedPosition.get());
        Path path = Path.of(POSITION_FILE_PATH);
        Files.writeString(path, content, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        logger.info("记录scn到文件：{}", content);
    }

    public static void main(String[] args) {
        try {
            // 加载上次处理的位置
            loadLastProcessedPosition();

            // 模拟多线程并发处理
            int numThreads = 10;
            Thread[] threads = new Thread[numThreads];

            for (int i = 0; i < numThreads; i++) {
                final int threadId = i;
                threads[i] = new Thread(() -> {
                    try {
                        // 模拟每个线程处理100条记录
                        for (int j = 0; j < 100; j++) {
                            // 处理数据，假设每次处理一个记录
                            processRecord(threadId * 100 + j);

                            // 定期刷新到文件
                            if (j % 10 == 0) {
                                savePositionToFile();
                            }
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
            }

            // 启动线程
            for (Thread thread : threads) {
                thread.start();
            }

            // 等待所有线程处理完成
            for (Thread thread : threads) {
                thread.join();
            }

            // 最后再记录一次位置
            savePositionToFile();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void processRecord(int recordId) {
        // 处理记录的逻辑
        System.out.println("Processing record: " + recordId);

        // 更新最后处理的位置
        recordPosition(recordId);
    }
}








