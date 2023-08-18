package com.dtxy.sync.dm2orcl;

import com.google.gson.JsonObject;
import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ConfigUtil {
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS");
    private static Properties properties;
    private static DataSource dataSource;

    private static ArrayBlockingQueue<JsonObject> dataQueue;

    private static ArrayBlockingQueue<String> fileQueue;

    private static ExecutorService consumerThreadPool;

    public static void init() {
        // 创建并配置连接池
        dataSource = createDataSource();

        dataQueue = new ArrayBlockingQueue<>(Integer.parseInt(getProperty("queue.capacity")));

        fileQueue = new ArrayBlockingQueue<>(50);

        consumerThreadPool = Executors.newCachedThreadPool();

        // 注册钩子函数
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // 关闭连接池
            closeDataSource(dataSource);
        }));
    }

    public static DataSource createDataSource() {
        BasicDataSource dataSource = new BasicDataSource();

// 设置数据库连接信息
        dataSource.setDriverClassName("dm.jdbc.driver.DmDriver");
        dataSource.setUrl(getProperty("dm.url"));
        dataSource.setUsername(getProperty("dm.user"));
        dataSource.setPassword(getProperty("dm.pwd"));

// 配置连接池属性
        dataSource.setInitialSize(2); // 初始连接数
        dataSource.setMaxTotal(5); // 最大活动连接数
        dataSource.setMinIdle(0); // 最小空闲连接数
        dataSource.setMaxIdle(2); // 最大空闲连接数
        dataSource.setMaxWaitMillis(5000); // 获取连接的最大等待时间（毫秒）

// 其他可选配置
// dataSource.setTestOnBorrow(true); // 验证连接是否可用
// dataSource.setValidationQuery("SELECT 1"); // 验证连接的SQL查询语句

// 连接池创建完成
        return dataSource;
    }

    public static DataSource getDMDs() {
        return dataSource;
    }

    public static DataSource getORCLDs() {
        return dataSource;
    }

    public static ArrayBlockingQueue<JsonObject> getDataQueue() {
        return dataQueue;
    }

    public static ArrayBlockingQueue<String> getFileQueue() {
        return fileQueue;
    }

    public static ExecutorService getConsumerThreadPool() {
        return consumerThreadPool;
    }

    private static void closeDataSource(DataSource dataSource) {
        if (dataSource instanceof BasicDataSource) {
            BasicDataSource basicDataSource = (BasicDataSource) dataSource;
            try {
                // 关闭连接池
                basicDataSource.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static long getCommitTime(String commit_time){
        Date date = null;
        try {
            date = sdf.parse(commit_time);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        return date.getTime();
    }

    public static void loadConfigFile(String relativePath) throws IOException {
        //String basePath = ConfigUtil.class.getResource("/").getPath();
        String basePath = System.getProperty("user.dir");
        String fullPath = basePath + File.separator + relativePath;

        properties = new Properties();
        FileInputStream fis = new FileInputStream(fullPath);
        properties.load(fis);
        fis.close();
    }

    public static String getProperty(String key) {
        return properties.getProperty(key);
    }

    public static String getFilePath(String key) {
        return System.getProperty("user.dir") + File.separator + getProperty(key);
    }

    protected static void releaseDM(Connection connection, Statement statement, PreparedStatement selectStatement, ResultSet resultSet,Logger logger) {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                // 处理连接关闭异常
                e.printStackTrace();
                logger.error("关闭达梦连接出错了：{}", e.getMessage());
            }
        }
        if (selectStatement != null) {
            try {
                selectStatement.close();
            } catch (SQLException e) {
                // 处理连接关闭异常
                e.printStackTrace();
                logger.error("关闭达梦连接出错了：{}", e.getMessage());
            }
        }
        if (statement != null) {
            try {
                //关闭日志分析
                statement = connection.createStatement();
                statement.addBatch("dbms_logmnr.end_logmnr()");
                statement.executeBatch();
                statement.close();
            } catch (SQLException e) {
                // 处理连接关闭异常
                e.printStackTrace();
                logger.error("关闭达梦连接出错了：{}", e.getMessage());
            }
        }

        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                // 处理连接关闭异常
                e.printStackTrace();
                logger.error("关闭达梦连接出错了：{}", e.getMessage());
            }
        }
    }

}
