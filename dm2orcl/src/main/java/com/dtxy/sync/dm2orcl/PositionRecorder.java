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
    private static final AtomicLong lastProcessedTime = new AtomicLong(0);

    // 获取上次处理的位置
    public static long getLastProcessedPosition() {
        return lastProcessedPosition.get();
    }
    public static long getLastProcessedTime() {
        return lastProcessedTime.get();
    }

    // 写入当前处理位置
    public static void recordPosition(long position,long commt_time) {
        lastProcessedPosition.set(position);
        lastProcessedTime.set(commt_time);
    }

    // 从文件中加载上次处理的位置
    public static void loadLastProcessedPosition() throws IOException {
        Path path = Path.of(POSITION_FILE_PATH);
        if (Files.exists(path)) {
            String content = Files.readString(path);
            String[]tmp=content.split(",");
            long position = Long.parseLong(tmp[0]);
            long commit_time=Long.parseLong(tmp[1]);
            lastProcessedPosition.set(position);
            lastProcessedTime.set(commit_time);
            logger.info("从文件加载当前scn：{},{}", position,commit_time);
        }
    }

    // 将当前处理位置写入文件
    public static synchronized void savePositionToFile() throws IOException {
        StringBuilder content = new StringBuilder();
        content.append(String.valueOf(lastProcessedPosition.get())).append(",").append(String.valueOf(lastProcessedTime.get()));
        Path path = Path.of(POSITION_FILE_PATH);
        Files.writeString(path, content.toString(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        logger.info("记录scn到文件：{}", content.toString());
    }
}








