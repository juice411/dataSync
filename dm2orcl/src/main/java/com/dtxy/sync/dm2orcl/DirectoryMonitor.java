package com.dtxy.sync.dm2orcl;

import ch.qos.logback.classic.util.ContextInitializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.*;

public class DirectoryMonitor {

    private static Logger logger = null;

    public static void main(String[] args) {
        try {
            //加载配置文件
            ConfigUtil.loadConfigFile("config/config.properties");
            String DIRECTORY_PATH = ConfigUtil.getProperty("monitor.dir"); // 监控目录的路径
            // 设置 Logback 配置文件的位置
            System.setProperty(ContextInitializer.CONFIG_FILE_PROPERTY, ConfigUtil.getFilePath("log.config.path"));

            logger = LoggerFactory.getLogger(DirectoryMonitor.class);
            // 初始化日志记录器
            logger.info("logmnr started！");
            // 创建 WatchService 对象
            WatchService watchService = FileSystems.getDefault().newWatchService();

            // 注册监控目录
            Path directory = Paths.get(DIRECTORY_PATH);
            directory.register(watchService, StandardWatchEventKinds.ENTRY_MODIFY); // 监控文件修改事件

            //记录最后一次修改事件的时间戳
            long lastModifiedTime = 0;
            // 启动监控循环
            logger.info("Monitoring directory:{} ", DIRECTORY_PATH);
            while (true) {
                //WatchKey key = watchService.poll(30, TimeUnit.SECONDS); // 等待事件发生
                WatchKey key = watchService.take();
                if (key != null) {
                    for (WatchEvent<?> event : key.pollEvents()) {
                        WatchEvent.Kind<?> kind = event.kind();
                        if (kind == StandardWatchEventKinds.OVERFLOW || kind == StandardWatchEventKinds.ENTRY_DELETE) {
                            continue;
                        }

                        // 获取被修改的文件路径
                        Path filePath = directory.resolve((Path) event.context());
                        File modifiedFile = filePath.toFile();

                        // 获取文件的最后修改时间
                        long currentModifiedTime = Files.getLastModifiedTime(filePath).toMillis();

                        // 检查时间间隔，避免重复触发
                        if (currentModifiedTime - lastModifiedTime > 1000 * Integer.parseInt(ConfigUtil.getProperty("monitor.frequency.sec"))) {

                            // 处理文件修改事件
                            if (kind == StandardWatchEventKinds.ENTRY_MODIFY) {
                                logger.info("File modified:{} ", modifiedFile.getAbsolutePath());

                            } else if (kind == StandardWatchEventKinds.ENTRY_CREATE) {
                                logger.info("File created:{} ", modifiedFile.getAbsolutePath());

                            }
                            //readFileContents(modifiedFile); // 读取文件内容
                            //进行日志分析,根据目前分析，一个256M的日志文件，总大小1G的日志设定，一个日志最大记录数不超过6万，所以设定10万足够取完
                            Dbmslob.parseMinerLog(ConfigUtil.getProperty("dm.host"), Integer.parseInt(ConfigUtil.getProperty("dm.port")), ConfigUtil.getProperty("dm.user"), ConfigUtil.getProperty("dm.pwd"), modifiedFile.getAbsolutePath(), Integer.parseInt(ConfigUtil.getProperty("dm.logmnr.max.records")));

                            // 更新最后一次修改事件的时间戳
                            lastModifiedTime = currentModifiedTime;
                        }

                    }
                    key.reset();
                }
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            logger.error("出错了：{}", e.getMessage());
        }
    }

    private static void readFileContents(File file) {
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = reader.readLine()) != null) {
                // 处理文件内容
                System.out.println(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

