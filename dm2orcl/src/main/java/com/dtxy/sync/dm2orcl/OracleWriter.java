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
import java.util.regex.Pattern;

public class OracleWriter {
    private static final Logger logger = LoggerFactory.getLogger(OracleWriter.class);
    static ExcelReader excelReader;

    private static final OracleDataSource dataSource;

    static {
        excelReader = new ExcelReader();

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
            JsonObject base_info = excelReader.getBaseInfo(dm_tab);
            //获取要被同步的Oracle表
            String oracle_tab = base_info.get("oracle_tab").getAsString().trim();
            //获取达梦与Oracle映射表
            Map<String, String> FIELD_MAPPING = FieldMapping.getFieldMapping(base_info.get("dm_sql").getAsString(), base_info.get("oracle_sql").getAsString());

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

            logger.debug("同步Oracle的sql：{}", sql);

            Connection connection = dataSource.getConnection();
            PreparedStatement statement = connection.prepareStatement(sql);

            // 设置字段值并执行同步
            if (opr.equalsIgnoreCase("insert")) {
                setParameterValues(statement, values, FIELD_MAPPING);
            } else if (opr.equalsIgnoreCase("update")) {
                setUpdateParameterValues(statement, values, jsonData.getAsJsonObject("set"), FIELD_MAPPING);
            }

            statement.executeUpdate();

            logger.debug("{}", "数据同步 Oracle 成功！");
        } catch (SQLException e) {
            logger.error("数据同步 Oracle 失败：{}", e.getMessage());
        }
    }

    // 拼接插入语句
    private static String buildInsertSql(Map<String, String> FIELD_MAPPING, String oracle_tab) {
        StringBuilder sqlBuilder = new StringBuilder("INSERT INTO ").append(oracle_tab).append(" (");
        StringBuilder placeholderBuilder = new StringBuilder("VALUES (");
        boolean isFirst = true;
        for (String jsonKey : FIELD_MAPPING.keySet()) {
            String field = FIELD_MAPPING.get(jsonKey);
            if (isValidString(field)) {//排除为空字符串和数字字段的情况
                if (!isFirst) {
                    sqlBuilder.append(", ");
                    placeholderBuilder.append(", ");
                }
                sqlBuilder.append(field);
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
        for (String jsonKey : FIELD_MAPPING.keySet()) {
            if (set.has(jsonKey)) {
                String field = FIELD_MAPPING.get(jsonKey);
                if (isValidString(field)) {//排除为空字符串和数字字段的情况
                    if (!isFirst) {
                        sqlBuilder.append(", ");
                    }
                    sqlBuilder.append(field).append(" = ?");
                    isFirst = false;
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
    private static void setParameterValues(PreparedStatement statement, JsonObject jsonData, Map<String, String> FIELD_MAPPING) throws SQLException {
        // 设置字段值并执行插入
        int index = 1;
        for (String jsonKey : FIELD_MAPPING.keySet()) {
            String field = FIELD_MAPPING.get(jsonKey);
            if (isValidString(field)) {//排除为空字符串和数字字段的情况
                String value = jsonData.get(jsonKey.toUpperCase()).getAsString().trim();
                statement.setString(index++, value);
            }
        }
    }

    // 设置 PreparedStatement 对象的参数值
    private static void setUpdateParameterValues(PreparedStatement statement, JsonObject jsonData, JsonObject set, Map<String, String> FIELD_MAPPING) throws SQLException {
        // 设置字段值并执行插入
        int index = 1;
        for (String jsonKey : FIELD_MAPPING.keySet()) {
            if (set.has(jsonKey)) {
                String field = FIELD_MAPPING.get(jsonKey);
                if (isValidString(field)) {//排除为空字符串和数字字段的情况
                    String value = jsonData.get(jsonKey).getAsString();
                    statement.setString(index++, value);
                }
            }
        }
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


