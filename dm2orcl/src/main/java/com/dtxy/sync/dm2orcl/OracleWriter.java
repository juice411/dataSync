package com.dtxy.sync.dm2orcl;

import com.google.gson.*;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class OracleWriter {
    private static final Logger logger = LoggerFactory.getLogger(OracleWriter.class);
    private static final String pattern = "TIMESTAMP(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2})|DATE(\\d{4}-\\d{2}-\\d{2})";

    private static final HikariDataSource dataSource;

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

    public static void sync2Oracle(JsonObject jsonData) throws Exception {
        Connection connection = null;
        PreparedStatement statement = null;

        try {
            /*Gson gson = new Gson();
            JsonObject jsonData = gson.fromJson(redo_sql, JsonObject.class);*/
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
            //先判断是否有按状态同步的需求
            if(base_info.has("condition")){
                Connection dm_connection = null;
                //Statement dm_statement = connection.createStatement();
                PreparedStatement selectStatement = null;
                ResultSet resultSet = null;
                try {
                    //进一步判断是否满足同步的条件
                    String condition=base_info.get("condition").getAsString();
                    JsonObject condition_json = new Gson().fromJson(condition, JsonObject.class);
                    //首先判断是否主表
                    boolean isMaster=condition_json.get("master").getAsBoolean();

                    if(isMaster){
                        if(opr.equalsIgnoreCase("update")){
                            String dm_field=condition_json.get("field").getAsString().trim().toUpperCase();
                            String []tmp=condition_json.get("field_value").getAsString().trim().split("#",2);
                            if(tmp[0].equalsIgnoreCase(values.get(dm_field).getAsString())){//确认状态
                                //是否有逻辑删除
                                if(condition_json.has("del_field")&&condition_json.has("del_value")){
                                    String del_value=condition_json.get("del_value").getAsString().trim();
                                    if(values.get(condition_json.get("del_field").getAsString().trim().toUpperCase()).getAsString().trim().equalsIgnoreCase(del_value)){

                                        throw new Exception("set opr as delete");
                                    }
                                }
                                //首先对主表处理
                                String selectSql=buildSelectSql(dm_tab,base_info.get("dm_field").getAsString().split(","),"ID",values.get("ID").getAsString());

                                dm_connection = ConfigUtil.getDMDs().getConnection();
                                selectStatement = dm_connection.prepareStatement(selectSql);
                                resultSet = selectStatement.executeQuery();
                                // 将查询结果转换为JSON对象
                                JsonObject values_from_resultset=null;
                                if (resultSet.next()) {
                                    values_from_resultset = new JsonObject();
                                    ResultSetMetaData meta = resultSet.getMetaData();
                                    int columnCount = meta.getColumnCount();
                                    for (int i = 1; i <= columnCount; i++) {
                                        String fieldName = meta.getColumnName(i);
                                        String fieldValue = resultSet.getString(i);
                                        if (fieldValue == null) {
                                            fieldValue = "NULL";
                                        }
                                        values_from_resultset.addProperty(fieldName.toUpperCase(), fieldValue);
                                    }
                                }else {
                                    logger.info("没有找到主表数据：{}",condition_json);
                                    return;
                                }

                                connection = dataSource.getConnection();
                                connection.setAutoCommit(false);
                                // 拼接操作语句
                                String sql_insert = buildInsertSql(FIELD_MAPPING, oracle_tab);
                                statement = connection.prepareStatement(sql_insert);
                                // 设置字段值并执行同步
                                setParameterValues(statement, values, FIELD_MAPPING, sql_insert);
                                try {
                                    statement.executeUpdate();
                                    // 处理 children
                                    handleChildren(condition_json,values.get("ID").getAsString(),dm_connection,selectStatement,resultSet,connection);

                                    connection.commit();
                                    logger.info("{}，scn:{}", "事务提交 Oracle 成功！",jsonData.get("scn").getAsLong());
                                    return;

                                }catch (Exception e){
                                    connection.rollback();
                                    logger.error("{}，scn:{}", "事务提交 Oracle 失败！",jsonData.get("scn").getAsLong());
                                    throw e;
                                }

                            }else if(tmp[1].equalsIgnoreCase(values.get(dm_field).getAsString())){//退回状态
                                //TODO 该干嘛干嘛


                            }else {
                                logger.info("不满足同步状态：{}",condition_json);
                                return;
                            }

                        }else if(opr.equalsIgnoreCase("insert")){
                            logger.info("不满足同步状态：{}",condition_json);
                            return;
                        }else if(opr.equalsIgnoreCase("delete")){
                            //TODO 该干嘛干嘛
                        }

                    }else {

                        if(opr.equalsIgnoreCase("delete")){
                            //TODO 该干嘛干嘛
                        }else {
                            //递归获取顶级主表审核状态
                            dm_connection = ConfigUtil.getDMDs().getConnection();
                            String isSync[]=getMasterStatus(condition_json,values.get("ID").getAsString(),dm_connection,resultSet);
                            if(isSync[1].equals("0")){
                                return;
                            }
                            //是否有逻辑删除
                            if(condition_json.has("del_field")&&condition_json.has("del_value")){
                                String del_value=condition_json.get("del_value").getAsString().trim();
                                if(values.get(condition_json.get("del_field").getAsString().trim().toUpperCase()).getAsString().trim().equalsIgnoreCase(del_value)){

                                    throw new Exception("set opr as delete");
                                }
                            }
                        }

                    }
                }catch (Exception e){
                    if(e.getMessage().contains("ORA-00001: 违反唯一约束条件")){
                        //TODO 该干嘛干嘛
                        logger.info("{}","在次提交审核通过状态，只做更新");
                    }else if(e.getMessage().contains("set opr as delete")){
                        //TODO 该干嘛干嘛
                        opr="delete";
                        logger.info("{}","执行逻辑删除转物理删除");
                    }else
                        throw e;
                }finally {
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
                    if (dm_connection != null) {
                        try {
                            dm_connection.close();
                        } catch (SQLException e) {
                            // 处理连接关闭异常
                            e.printStackTrace();
                            logger.error("关闭达梦连接出错了：{}", e.getMessage());
                        }
                    }
                }

            }
            // 拼接操作语句
            String sql = null;
            if (opr.equalsIgnoreCase("insert")) {
                sql = buildInsertSql(FIELD_MAPPING, oracle_tab);
            } else if (opr.equalsIgnoreCase("update")) {
                //获取set json obj
                sql = buildUpdateSql(FIELD_MAPPING, oracle_tab, jsonData.getAsJsonObject("set"));
                if(sql==null){
                    logger.info("本次update没有涉及配置字段：{}",jsonData.getAsJsonObject("set"));
                    return;
                }
                sql = sql.replaceAll("\\byour_condition\\b", "ID='" + values.get("ID").getAsString() + "'");

            } else if (opr.equalsIgnoreCase("delete")) {
                sql = buildDeleteSql(FIELD_MAPPING, oracle_tab);
                sql = sql.replaceAll("\\byour_condition\\b", "ID='" + values.get("ID").getAsString() + "'");

            }

            //logger.debug("同步Oracle的sql：{}", sql);

            connection = dataSource.getConnection();
            statement = connection.prepareStatement(sql);

            // 设置字段值并执行同步
            if (opr.equalsIgnoreCase("insert")) {
                setParameterValues(statement, values, FIELD_MAPPING, sql);
            } else if (opr.equalsIgnoreCase("update")) {
                setUpdateParameterValues(statement, values, jsonData.getAsJsonObject("set"), FIELD_MAPPING, sql);
            } else {
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
            logger.info("{}，scn:{}", "数据同步 Oracle 成功！",jsonData.get("scn").getAsLong());
        } catch (SQLException e) {
            logger.error("数据同步 Oracle 失败：{}，scn:{}", e.getMessage(),jsonData.get("scn").getAsLong());
        } catch (Exception e) {
            throw e;
        }finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    // 处理连接关闭异常
                    e.printStackTrace();
                    logger.error("释放Oracle连接出错了：{}", e.getMessage());
                }
            }

            if (connection != null) {
                try {
                    //还原为自动提交
                    connection.setAutoCommit(true);
                    connection.close();
                } catch (SQLException e) {
                    // 处理连接关闭异常
                    e.printStackTrace();
                    logger.error("释放Oracle连接出错了：{}", e.getMessage());
                }
            }
        }
    }

    private static String []getMasterStatus(JsonObject jsonObject,String child_ID,Connection dm_connection, ResultSet resultSet) throws SQLException {
        String tmp[]={"-1","1"};
        if (jsonObject.has("children")) {
            JsonArray childrenArray = jsonObject.get("children").getAsJsonArray();
            for (JsonElement child : childrenArray) {
                tmp=getMasterStatus(child.getAsJsonObject(),child_ID,dm_connection,resultSet);
            }
        }

        // 输出当前节点信息或执行其他操作
        if (jsonObject.has("tab_name")&&jsonObject.has("relation_field")) {
            String tabName = jsonObject.get("tab_name").getAsString();
            String relationField = jsonObject.get("relation_field").getAsString();
            String selectSql = String.format("select %s from %s where id ='%s'", relationField, tabName, child_ID);

            try (PreparedStatement stmt = dm_connection.prepareStatement(selectSql)) {
                // 执行 PreparedStatement 对象的操作
                resultSet = stmt.executeQuery();

                if (resultSet.next()) {
                    tmp[0] = resultSet.getString(relationField);

                }else {
                    logger.info("没有找到关联的主表数据：{}",jsonObject);
                    tmp[1]="0";
                }
            }
        }else if (jsonObject.has("master")) {
            String tabName = jsonObject.get("tab_name").getAsString().trim().toUpperCase();
            String field = jsonObject.get("field").getAsString().trim().toUpperCase();
            String []tmp2 = jsonObject.get("field_value").getAsString().trim().split("#",2);
            String selectSql = String.format("select %s from %s where id ='%s'", field, tabName, tmp[0]);

            try (PreparedStatement stmt = dm_connection.prepareStatement(selectSql)) {
                // 执行 PreparedStatement 对象的操作
                resultSet = stmt.executeQuery();
                if (resultSet.next()) {
                    String status = resultSet.getString(field);
                    if(!status.equalsIgnoreCase(tmp2[0])&&!status.equalsIgnoreCase(tmp2[1])){
                        logger.info("不满足同步状态：{}",jsonObject);
                        tmp[1]="0";
                    }

                }else {
                    logger.info("没有找到关联的主表数据：{}",jsonObject);
                    tmp[1]="0";
                }
            }
        }

        return tmp;
    }

    private static void handleChildren(JsonObject jsonObj, String parent_ID, Connection dm_connection, PreparedStatement selectStatement, ResultSet resultSet, Connection connection) throws Exception {
        if (jsonObj.has("children")) {
            JsonArray children = jsonObj.getAsJsonArray("children");
            for (JsonElement child : children) {
                JsonObject childObj = child.getAsJsonObject();

                String dm_tab=childObj.get("tab_name").getAsString().trim().toUpperCase();
                String relation_field=childObj.get("relation_field").getAsString().trim().toUpperCase();//关联字段

                //从基础映射文件获取映射基础信息表
                JsonObject base_info = ExcelReader.getBaseInfo(dm_tab);
                //获取要被同步的Oracle表
                String oracle_tab = base_info.get("oracle_tab").getAsString().trim();
                //获取达梦与Oracle映射表
                Map<String, String> FIELD_MAPPING = FieldMapping.getFieldMapping(base_info.get("dm_field").getAsString(), base_info.get("oracle_field").getAsString());
                String selectSql=buildSelectSql(dm_tab,base_info.get("dm_field").getAsString().split(","),relation_field,parent_ID);

                selectStatement = dm_connection.prepareStatement(selectSql);
                resultSet = selectStatement.executeQuery();
                // 将查询结果转换为JSON对象
                JsonObject values_from_resultset=null;
                while (resultSet.next()) {
                    values_from_resultset = new JsonObject();
                    ResultSetMetaData meta = resultSet.getMetaData();
                    int columnCount = meta.getColumnCount();
                    for (int i = 1; i <= columnCount; i++) {
                        String fieldName = meta.getColumnName(i);
                        String fieldValue = resultSet.getString(i);
                        if (fieldValue == null) {
                            fieldValue = "NULL";
                        }
                        values_from_resultset.addProperty(fieldName.toUpperCase(), fieldValue);
                    }

                    // 拼接操作语句
                    String sql = buildInsertSql(FIELD_MAPPING, oracle_tab);

                    try (PreparedStatement stmt = connection.prepareStatement(sql)) {
                        // 执行 PreparedStatement 对象的操作
                        setParameterValues(stmt, values_from_resultset, FIELD_MAPPING, sql);
                        stmt.executeUpdate();
                    }
                }

                handleChildren(childObj,values_from_resultset.get("ID").getAsString(),dm_connection,selectStatement,resultSet, connection);
            }
        }
    }

    private static String buildSelectSql(String tableName, String[] fields, String idFieldName, String id) {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ");
        for (int i = 0; i < fields.length; i++) {
            if(fields[i].contains("#")){
                String[] tmp = fields[i].split("#",2);
                sb.append(tmp[0]);
            }else {
                sb.append(fields[i]);
            }

            if (i < fields.length - 1) {
                sb.append(", ");
            }
        }
        sb.append(" FROM ").append(tableName).append(" WHERE ").append(idFieldName).append(" = '").append(id).append("'");
        return sb.toString();
    }

    // 拼接插入语句
    private static String buildInsertSql(Map<String, String> FIELD_MAPPING, String oracle_tab) throws Exception {
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
                String[] tmp = oracle_field.split("=", 2);
                sqlBuilder.append(tmp[0].trim());
                placeholderBuilder.append(tmp[1].trim()).append("(").append("?").append(")");
                isFirst = false;

            } else {
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
    private static String buildUpdateSql(Map<String, String> FIELD_MAPPING, String oracle_tab, JsonObject set) throws Exception {
        StringBuilder sqlBuilder = new StringBuilder("UPDATE ").append(oracle_tab).append(" SET ");
        boolean isFirst = true;
        int setLen=0;
        for (String dm_field : FIELD_MAPPING.keySet()) {
            dm_field=dm_field.toUpperCase();
            //判断达梦字段是否包含了#号
            if (dm_field.contains("#")) {
                String[] tmp = dm_field.split("#");
                if (set.has(tmp[0])) {
                    String oracle_field = FIELD_MAPPING.get(dm_field);
                    if (oracle_field.contains("=")) {//是否存在函数表达式
                        if (!isFirst) {
                            sqlBuilder.append(", ");
                        }
                        tmp = oracle_field.split("=");
                        sqlBuilder.append(tmp[0].trim()).append(" = ").append(tmp[1].trim()).append("(").append("?").append(")");
                        isFirst = false;
                    } else {
                        if (!isFirst) {
                            sqlBuilder.append(", ");
                        }
                        sqlBuilder.append(oracle_field).append(" = ?");
                        isFirst = false;
                    }
                    setLen++;
                }

            } else {
                if (set.has(dm_field)) {
                    String oracle_field = FIELD_MAPPING.get(dm_field);
                    if (oracle_field.contains("=")) {//是否存在函数表达式
                        if (!isFirst) {
                            sqlBuilder.append(", ");
                        }
                        String[] tmp = oracle_field.split("=");
                        sqlBuilder.append(tmp[0].trim()).append(" = ").append(tmp[1].trim()).append("(").append("?").append(")");
                        isFirst = false;
                    } else {
                        if (!isFirst) {
                            sqlBuilder.append(", ");
                        }
                        sqlBuilder.append(oracle_field).append(" = ?");
                        isFirst = false;
                    }
                    setLen++;
                }

            }

        }

        // 添加 WHERE 子句
        sqlBuilder.append(" WHERE your_condition");

        if(setLen==0){
            return null;
        }
        return sqlBuilder.toString();
    }

    // 拼接删除语句
    private static String buildDeleteSql(Map<String, String> FIELD_MAPPING, String oracle_tab) throws Exception {
        StringBuilder sqlBuilder = new StringBuilder("DELETE FROM ").append(oracle_tab);
        sqlBuilder.append(" WHERE your_condition");

        return sqlBuilder.toString();
    }

    // 设置 PreparedStatement 对象的参数值
    private static void setParameterValues(PreparedStatement statement, JsonObject jsonData, Map<String, String> FIELD_MAPPING, String sql) throws Exception {
        // 设置字段值并执行插入
        int index = 1;
        for (String dm_field : FIELD_MAPPING.keySet()) {
            dm_field=dm_field.toUpperCase();
            String value;
            //判断达梦字段是否包含了#号
            if (dm_field.contains("#")) {
                String[] tmp = dm_field.split("#", 2);
                value = jsonData.get(tmp[0]).getAsString().trim();
            } else {
                value = jsonData.get(dm_field).getAsString().trim();
            }

            if (value.equals("NULL")) {
                value = null;
            } else if (value.contains("TIMESTAMP")) {
                Pattern regex = Pattern.compile(pattern);
                Matcher matcher = regex.matcher(value);
                if (matcher.find()) {
                    String timestamp = matcher.group(1);
                    value = timestamp;
                }

            } else if (value.contains("DATE")) {
                Pattern regex = Pattern.compile(pattern);
                Matcher matcher = regex.matcher(value);
                if (matcher.find()) {
                    String timestamp = matcher.group(2);
                    value = timestamp += " 00:00:00";
                }

            }
            statement.setObject(index++, value);

            // 拼接完整的 SQL 语句,主要是方便定位问题
            if (value == null) {
                sql = sql.replaceFirst("\\?", "''");
            } else
                sql = sql.replaceFirst("\\?", "'" + value + "'");


        }

        logger.debug("同步Oracle的sql：{}", sql);
    }

    // 设置 PreparedStatement 对象的参数值
    private static void setUpdateParameterValues(PreparedStatement statement, JsonObject jsonData, JsonObject set, Map<String, String> FIELD_MAPPING, String sql) throws Exception {
        // 设置字段值并执行插入
        int index = 1;
        for (String dm_field : FIELD_MAPPING.keySet()) {
            dm_field=dm_field.toUpperCase();
            String value;
            //判断达梦字段是否包含了#号
            if (dm_field.contains("#")) {
                String[] tmp = dm_field.split("#", 2);
                if (set.has(tmp[0])) {
                    value = jsonData.get(tmp[0]).getAsString().trim();
                    if (value.equals("NULL")) {
                        value = null;
                    } else if (value.contains("TIMESTAMP")) {
                        Pattern regex = Pattern.compile(pattern);
                        Matcher matcher = regex.matcher(value);
                        if (matcher.find()) {
                            String timestamp = matcher.group(1);
                            value = timestamp;
                        }

                    } else if (value.contains("DATE")) {
                        Pattern regex = Pattern.compile(pattern);
                        Matcher matcher = regex.matcher(value);
                        if (matcher.find()) {
                            String timestamp = matcher.group(2);
                            value = timestamp += " 00:00:00";
                        }

                    }

                    statement.setObject(index++, value);

                    // 拼接完整的 SQL 语句,主要是方便定位问题
                    if (value == null) {
                        sql = sql.replaceFirst("\\?", "''");
                    } else
                        sql = sql.replaceFirst("\\?", "'" + value + "'");
                }
            } else {
                if (set.has(dm_field)) {
                    value = jsonData.get(dm_field).getAsString().trim();
                    if (value.equals("NULL")) {
                        value = null;
                    } else if (value.contains("TIMESTAMP")) {
                        Pattern regex = Pattern.compile(pattern);
                        Matcher matcher = regex.matcher(value);
                        if (matcher.find()) {
                            String timestamp = matcher.group(1);
                            value = timestamp;
                        }

                    } else if (value.contains("DATE")) {
                        Pattern regex = Pattern.compile(pattern);
                        Matcher matcher = regex.matcher(value);
                        if (matcher.find()) {
                            String timestamp = matcher.group(2);
                            value = timestamp += " 00:00:00";
                        }

                    }
                    statement.setObject(index++, value);

                    // 拼接完整的 SQL 语句,主要是方便定位问题
                    if (value == null) {
                        sql = sql.replaceFirst("\\?", "''");
                    } else
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
    private static HikariDataSource createDataSource() {

            /*Class.forName("oracle.jdbc.driver.OracleDriver");
            OracleDataSource dataSource = new OracleDataSource();
            dataSource.setURL(ConfigUtil.getProperty("oracle.url"));
            dataSource.setUser(ConfigUtil.getProperty("oracle.user"));
            dataSource.setPassword(ConfigUtil.getProperty("oracle.pwd"));*/
        HikariConfig config = new HikariConfig();

        config.setDriverClassName("oracle.jdbc.driver.OracleDriver");
        config.setJdbcUrl(ConfigUtil.getProperty("oracle.url"));
        config.setUsername(ConfigUtil.getProperty("oracle.user"));
        config.setPassword(ConfigUtil.getProperty("oracle.pwd"));
        // 最小空闲连接数，默认值：10
        config.setMinimumIdle(2);
        // 最大连接数，默认值：10
        config.setMaximumPoolSize(10);
        // 连接超时时间（获取连接的最大等待时间），默认值：30秒
        //config.setConnectionTimeout(30000);
        // 空闲连接超时时间，默认值：10分钟
        config.setIdleTimeout(600000);
        // 最大生命周期时间（连接在连接池中最长的生命周期时间），默认值：30分钟
        config.setMaxLifetime(1800000);
        // 自动提交，默认值：true
        config.setAutoCommit(true);
        // 测试连接SQL语句（用于检测连接是否有效的SQL语句），默认值：无
        // 默认情况下，如果未设置，则会使用“SELECT 1”作为测试语句
        config.setConnectionTestQuery("SELECT 1 FROM DUAL");
        // 等待队列大小（连接池中等待获取连接的请求队列的最大大小），默认值：-1（无限制）
        //config.setQueueSize(-1);
        // 连接初始化SQL语句（连接池创建连接时可以执行的SQL语句），默认值：无
        //config.setInitializationFailFast(false);
        // 连接池的名称，默认值：自动生成的唯一名称
        config.setPoolName("HikariPool-1");

        // 其他可选配置...
        // config.addDataSourceProperty("property_name", "property_value");

        HikariDataSource dataSource = new HikariDataSource(config);
        return dataSource;
    }

    private static void registerShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // 关闭连接池
            if (dataSource != null) {
                try {
                    dataSource.close();
                } catch (Exception e) {
                    logger.error("关闭 Oracle 连接池失败：{}", e.getMessage());

                }
            }
        }));
    }
}


