package com.dtxy.sync.dm2orcl;

import org.apache.commons.dbcp2.BasicDataSource;

import javax.sql.DataSource;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;

public class ConfigUtil {
    private static Properties properties;
    private static DataSource dataSource;

    public static void init() {
        // 创建并配置连接池
        dataSource = createDataSource();

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
        dataSource.setInitialSize(1); // 初始连接数
        dataSource.setMaxTotal(5); // 最大活动连接数
        dataSource.setMinIdle(1); // 最小空闲连接数
        dataSource.setMaxIdle(5); // 最大空闲连接数
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

}
