package com.dtxy.sync.dm2orcl;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import oracle.jdbc.pool.OracleDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class OracleWriter {
    private static final Logger logger = LoggerFactory.getLogger(OracleWriter.class);
    private static final String pattern = "TIMESTAMP(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2})|DATE(\\d{4}-\\d{2}-\\d{2})";

    private static final OracleDataSource dataSource;

    static {
        // 创建连接池并获取连接
        dataSource = createDataSource();
        // 注册进程关闭钩子
        registerShutdownHook();


    }

    public static void main(String[] args) throws ClassNotFoundException {
        // 映射表，键为字段名，值为对应的 JSON key
        /*Map<String, String> FIELD_MAPPING = new HashMap<>();
        FIELD_MAPPING.put("id", "ID");
        FIELD_MAPPING.put("name", "NAME");
        // 获取即将操作的 Oracle 表数据的所有字段值的 JSON 对象
        JsonObject jsonData = getJsonDataFromAnotherSource();
        jsonData.addProperty("id","122");
        jsonData.addProperty("name","juice");

        sync2Oracle(jsonData,FIELD_MAPPING);*/
    }

    public static void sync2Oracle(String redo_sql) {

        try {
            Gson gson = new Gson();
            JsonObject jsonData = gson.fromJson(redo_sql, JsonObject.class);
            //先获取被操作的达梦表
            String dm_tab = jsonData.get("table").getAsString().trim();
            //获取操作类型
            String opr = jsonData.get("opr").getAsString();
            //获取数据对象
            JsonObject values = jsonData.getAsJsonObject("values");

            //从基础映射文件获取映射基础信息表
            JsonObject base_info = ExcelReader.getBaseInfo(dm_tab);
            //获取要被同步的Oracle表
            String oracle_tab = base_info.get("oracle_tab").getAsString().trim();
            //获取达梦与Oracle映射表
            Map<String, String> FIELD_MAPPING = FieldMapping.getFieldMapping(base_info.get("dm_field").getAsString(), base_info.get("oracle_field").getAsString());

            // 拼接操作语句
            String sql = null;
            if (opr.equalsIgnoreCase("insert")) {
                sql = buildInsertSql(FIELD_MAPPING, oracle_tab);
            } else if (opr.equalsIgnoreCase("update")) {
                //获取set json obj
                sql = buildUpdateSql(FIELD_MAPPING, oracle_tab, jsonData.getAsJsonObject("set"));
                sql = sql.replaceAll("\\byour_condition\\b", "ID='" + values.get("ID").getAsString() + "'");

            } else if (opr.equalsIgnoreCase("delete")) {
                sql = buildDeleteSql(FIELD_MAPPING, oracle_tab);
                sql = sql.replaceAll("\\byour_condition\\b", "ID='" + values.get("ID").getAsString() + "'");

            }

            //logger.debug("同步Oracle的sql：{}", sql);

            Connection connection = dataSource.getConnection();
            PreparedStatement statement = connection.prepareStatement(sql);

            // 设置字段值并执行同步
            if (opr.equalsIgnoreCase("insert")) {
                setParameterValues(statement, values, FIELD_MAPPING,sql);
            } else if (opr.equalsIgnoreCase("update")) {
                setUpdateParameterValues(statement, values, jsonData.getAsJsonObject("set"), FIELD_MAPPING,sql);
            }else {
                logger.debug("同步Oracle的sql：{}", sql);
            }

            statement.executeUpdate();
            statement.close();

            //判断是否存在procedure项，如果存在说明要执行存储过程同步
            /*if(base_info.has("procedure")){
                // 准备调用存储过程的语句
                String storedProc = String.format("{call %s}", base_info.get("procedure").getAsString().trim());
                // 创建 CallableStatement 对象
                CallableStatement cstmt = connection.prepareCall(storedProc);
                // 设置字段值
                int index = 1;
                for (String jsonKey : FIELD_MAPPING.keySet()) {
                    String field = FIELD_MAPPING.get(jsonKey);
                    if (isValidString(field)) {//排除为空字符串和数字字段的情况
                        String value = values.get(jsonKey.toUpperCase()).getAsString().trim();
                        cstmt.setString(index++, value);
                    }
                }

                // 执行存储过程
                cstmt.execute();
                cstmt.close();
            }*/
            //释放连接
            connection.close();

            logger.debug("{}", "数据同步 Oracle 成功！");
        } catch (Exception e) {
            logger.error("数据同步 Oracle 失败：{}", e.getMessage());
        }
    }

    // 拼接插入语句
    private static String buildInsertSql(Map<String, String> FIELD_MAPPING, String oracle_tab) {
        StringBuilder sqlBuilder = new StringBuilder("INSERT INTO ").append(oracle_tab).append(" (");
        StringBuilder placeholderBuilder = new StringBuilder("VALUES (");
        boolean isFirst = true;
        for (String dm_field : FIELD_MAPPING.keySet()) {
            String oracle_field = FIELD_MAPPING.get(dm_field);
            if (oracle_field.contains("=")) {//是否存在函数表达式
                if (!isFirst) {
                    sqlBuilder.append(", ");
                    placeholderBuilder.append(", ");
                }
                String []tmp=oracle_field.split("=",2);
                sqlBuilder.append(tmp[0].trim());
                placeholderBuilder.append(tmp[1].trim()).append("(").append("?").append(")");
                isFirst = false;

            }else {
                if (!isFirst) {
                    sqlBuilder.append(", ");
                    placeholderBuilder.append(", ");
                }
                sqlBuilder.append(oracle_field);
                placeholderBuilder.append("?");
                isFirst = false;
            }
        }
        sqlBuilder.append(") ");
        placeholderBuilder.append(")");

        return sqlBuilder.toString() + placeholderBuilder.toString();
    }

    // 拼接更新语句
    private static String buildUpdateSql(Map<String, String> FIELD_MAPPING, String oracle_tab, JsonObject set) {
        StringBuilder sqlBuilder = new StringBuilder("UPDATE ").append(oracle_tab).append(" SET ");
        boolean isFirst = true;
        for (String dm_field : FIELD_MAPPING.keySet()) {
            //判断达梦字段是否包含了#号
            if(dm_field.contains("#")){
                String []tmp=dm_field.split("#");
                if (set.has(tmp[0].toUpperCase())) {
                    String oracle_field = FIELD_MAPPING.get(dm_field);
                    if (oracle_field.contains("=")) {//是否存在函数表达式
                        if (!isFirst) {
                            sqlBuilder.append(", ");
                        }
                        tmp=oracle_field.split("=");
                        sqlBuilder.append(tmp[0].trim()).append(" = ").append(tmp[1].trim()).append("(").append("?").append(")");
                        isFirst = false;
                    }else {
                        if (!isFirst) {
                            sqlBuilder.append(", ");
                        }
                        sqlBuilder.append(oracle_field).append(" = ?");
                        isFirst = false;
                    }
                }

            }else {
                if (set.has(dm_field)) {
                    String oracle_field = FIELD_MAPPING.get(dm_field);
                    if (oracle_field.contains("=")) {//是否存在函数表达式
                        if (!isFirst) {
                            sqlBuilder.append(", ");
                        }
                        String []tmp=oracle_field.split("=");
                        sqlBuilder.append(tmp[0].trim()).append(" = ").append(tmp[1].trim()).append("(").append("?").append(")");
                        isFirst = false;
                    }else {
                        if (!isFirst) {
                            sqlBuilder.append(", ");
                        }
                        sqlBuilder.append(oracle_field).append(" = ?");
                        isFirst = false;
                    }
                }

            }

        }

        // 添加 WHERE 子句
        sqlBuilder.append(" WHERE your_condition");

        return sqlBuilder.toString();
    }

    // 拼接删除语句
    private static String buildDeleteSql(Map<String, String> FIELD_MAPPING, String oracle_tab) {
        StringBuilder sqlBuilder = new StringBuilder("DELETE FROM ").append(oracle_tab);
        sqlBuilder.append(" WHERE your_condition");

        return sqlBuilder.toString();
    }

    // 设置 PreparedStatement 对象的参数值
    private static void setParameterValues(PreparedStatement statement, JsonObject jsonData, Map<String, String> FIELD_MAPPING, String sql) throws SQLException {
        // 设置字段值并执行插入
        int index = 1;
        for (String dm_field : FIELD_MAPPING.keySet()) {
            String value;
            //判断达梦字段是否包含了#号
            if(dm_field.contains("#")){
                String []tmp=dm_field.split("#",2);
                value = jsonData.get(tmp[0].toUpperCase()).getAsString().trim();
            }else {
                value = jsonData.get(dm_field.toUpperCase()).getAsString().trim();
            }

            if(value.equals("NULL")){
                value=null;
            }else if(value.contains("TIMESTAMP")){
                Pattern regex = Pattern.compile(pattern);
                Matcher matcher = regex.matcher(value);
                if (matcher.find()) {
                    String timestamp = matcher.group(1);
                    value=timestamp;
                }

            }else if(value.contains("DATE")){
                Pattern regex = Pattern.compile(pattern);
                Matcher matcher = regex.matcher(value);
                if (matcher.find()) {
                    String timestamp = matcher.group(2);
                    value=timestamp += " 00:00:00";
                }

            }
            statement.setObject(index++, value);

            // 拼接完整的 SQL 语句,主要是方便定位问题
            if(value==null){
                sql = sql.replaceFirst("\\?", "''");
            }else
                sql = sql.replaceFirst("\\?", "'" + value + "'");


        }

        logger.debug("同步Oracle的sql：{}", sql);
    }

    // 设置 PreparedStatement 对象的参数值
    private static void setUpdateParameterValues(PreparedStatement statement, JsonObject jsonData, JsonObject set, Map<String, String> FIELD_MAPPING, String sql) throws SQLException {
        // 设置字段值并执行插入
        int index = 1;
        for (String dm_field : FIELD_MAPPING.keySet()) {
            String value;
            //判断达梦字段是否包含了#号
            if(dm_field.contains("#")){
                String []tmp=dm_field.split("#",2);
                if (set.has(tmp[0].toUpperCase())) {
                    value = jsonData.get(tmp[0].toUpperCase()).getAsString().trim();
                    if(value.equals("NULL")){
                        value=null;
                    }else if(value.contains("TIMESTAMP")){
                        Pattern regex = Pattern.compile(pattern);
                        Matcher matcher = regex.matcher(value);
                        if (matcher.find()) {
                            String timestamp = matcher.group(1);
                            value=timestamp;
                        }

                    }else if(value.contains("DATE")){
                        Pattern regex = Pattern.compile(pattern);
                        Matcher matcher = regex.matcher(value);
                        if (matcher.find()) {
                            String timestamp = matcher.group(2);
                            value=timestamp += " 00:00:00";
                        }

                    }

                    statement.setObject(index++, value);

                    // 拼接完整的 SQL 语句,主要是方便定位问题
                    if(value==null){
                        sql = sql.replaceFirst("\\?", "''");
                    }else
                        sql = sql.replaceFirst("\\?", "'" + value + "'");
                }
            }else {
                if (set.has(dm_field.toUpperCase())) {
                    value = jsonData.get(dm_field.toUpperCase()).getAsString().trim();
                    if(value.equals("NULL")){
                        value=null;
                    }else if(value.contains("TIMESTAMP")){
                        Pattern regex = Pattern.compile(pattern);
                        Matcher matcher = regex.matcher(value);
                        if (matcher.find()) {
                            String timestamp = matcher.group(1);
                            value=timestamp;
                        }

                    }else if(value.contains("DATE")){
                        Pattern regex = Pattern.compile(pattern);
                        Matcher matcher = regex.matcher(value);
                        if (matcher.find()) {
                            String timestamp = matcher.group(2);
                            value=timestamp += " 00:00:00";
                        }

                    }
                    statement.setObject(index++, value);

                    // 拼接完整的 SQL 语句,主要是方便定位问题
                    if(value==null){
                        sql = sql.replaceFirst("\\?", "''");
                    }else
                        sql = sql.replaceFirst("\\?", "'" + value + "'");
                }
            }

        }
        logger.debug("同步Oracle的sql：{}", sql);
    }

    // 判断是否为有效的字符串（非 null、非空、非纯数字字符串）
    private static boolean isValidString(String value) {
        return value != null && !value.isEmpty() && !isNumericString(value);
    }

    // 判断是否为纯数字字符串
    private static boolean isNumericString(String value) {
        return Pattern.matches("\\d+", value);
    }

    // 创建 Oracle 连接池
    private static OracleDataSource createDataSource() {
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            OracleDataSource dataSource = new OracleDataSource();
            dataSource.setURL(ConfigUtil.getProperty("oracle.url"));
            dataSource.setUser(ConfigUtil.getProperty("oracle.user"));
            dataSource.setPassword(ConfigUtil.getProperty("oracle.pwd"));
            return dataSource;
        } catch (SQLException | ClassNotFoundException e) {

            logger.error("创建 Oracle 连接池失败：{}", e.getMessage());

            return null;
        }
    }

    private static void registerShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // 关闭连接池
            if (dataSource != null) {
                try {
                    dataSource.close();
                } catch (SQLException e) {
                    logger.error("关闭 Oracle 连接池失败：{}", e.getMessage());

                }
            }
        }));
    }
}


