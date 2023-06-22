package com.dtxy.sync.dm2orcl;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class ConfigUtil {
    private static Properties properties;

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
