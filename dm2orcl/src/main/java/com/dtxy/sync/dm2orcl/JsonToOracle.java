package com.dtxy.sync.dm2orcl;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.sql.*;

public class JsonToOracle {
    public static void main(String[] args) throws ClassNotFoundException {
        // Oracle database configuration
        String jdbcUrl = "jdbc:oracle:thin:@//192.168.24.3:1521/ORCL";
        String username = "rldd";
        String password = "fuelsyscenter";

        // JSON data as a string
        String jsonData = "{\"table\":\" JR_AUTH_FUELMC.PUB_SERVICEBEAN_NAME\",\"values\":{\"ID\":\"1666719505134006280\",\"BUSSINESS_KEY\":\" ddzx\",\"BEAN_NAME\":\" ddzx.planManageActivityCompletedServiceImpl\",\"CLASS_NAME\":\" com.dtxytech.rlddnew.planmanage.service.impl.PlanManageActivityCompletedServiceImpl\",\"LOAD_DATE\":\" TIMESTAMP2023-06-08 16:09:15\",\"DESCRIPTION\":\" 鐠佲�冲灊缁犫剝甯剁�光剝鐗�-濞翠胶鈻肩�瑰本鍨氭禍瀣╂\uE0BD\",\"TENANT_ID\":\" NULL\",\"BEAN_TYPE\":\" PROCESS_COMPLETED\"},\"opr\":\"insert\"}"; // Replace with your JSON data

        Class.forName("oracle.jdbc.driver.OracleDriver");
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password)) {
            processJsonData(jsonData, conn);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void processJsonData(String jsonData, Connection conn) throws SQLException {
        try {
            // Parse JSON string to a JsonObject
            JsonObject json = JsonParser.parseString(jsonData).getAsJsonObject();

            // Get table metadata
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet resultSet = metaData.getColumns(null, null, json.get("table").getAsString(), null);
            //ResultSetMetaData rsMetaData = resultSet.getMetaData();

            // Prepare SQL statement based on operation type
            String sql;
            switch (json.get("opr").getAsString()) {
                case "insert":
                    sql = buildInsertStatement(json, resultSet);
                    break;
                case "update":
                    sql = buildUpdateStatement(json, resultSet);
                    break;
                case "delete":
                    sql = buildDeleteStatement(json);
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported operation type: " + json.get("opr").getAsString());
            }

            // Execute SQL statement
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                setStatementParameters(stmt, json.getAsJsonObject("values"), resultSet);
                stmt.executeUpdate();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void setStatementParameters(PreparedStatement stmt, JsonObject json, ResultSet resultSet) throws SQLException {
        /*for (int i = 1; i <= rsMetaData.getColumnCount(); i++) {
            String columnName = rsMetaData.getColumnName(i);
            int columnType = rsMetaData.getColumnType(i);

            if (json.has(columnName)) {
                setStatementParameter(stmt, i, columnType, json.get(columnName));
            } else {
                stmt.setNull(i, columnType);
            }
        }*/

        int columnSize = 1;
        while (resultSet.next()) {
            String columnName = resultSet.getString("COLUMN_NAME");
            int columnType = resultSet.getInt("DATA_TYPE");
            if (json.has(columnName)) {
                setStatementParameter(stmt, columnSize, columnType, json.get(columnName));
            } else {
                stmt.setNull(columnSize, columnType);
            }
            columnSize++;
        }
        //stmt.setString(rsMetaData.getColumnCount() + 1, json.get("ID").getAsString());
    }

    private static void setStatementParameter(PreparedStatement stmt, int index, int columnType, JsonElement jsonValue) throws SQLException {
        if (jsonValue.isJsonNull()) {
            stmt.setNull(index, columnType);
            return;
        }

        Gson gson = new Gson();
        switch (columnType) {
            case Types.INTEGER:
                stmt.setInt(index, jsonValue.getAsInt());
                break;
            case Types.BIGINT:
                stmt.setLong(index, jsonValue.getAsLong());
                break;
            case Types.FLOAT:
                stmt.setFloat(index, jsonValue.getAsFloat());
                break;
            case Types.DOUBLE:
                stmt.setDouble(index, jsonValue.getAsDouble());
                break;
            case Types.BOOLEAN:
                stmt.setBoolean(index, jsonValue.getAsBoolean());
                break;
            case Types.DATE:
                stmt.setDate(index, gson.fromJson(jsonValue, Date.class));
                break;
            case Types.TIMESTAMP:
                stmt.setTimestamp(index, gson.fromJson(jsonValue, Timestamp.class));
                break;
            default:
                stmt.setString(index, jsonValue.getAsString());
                break;
        }
    }

    private static String buildInsertStatement(JsonObject json, ResultSet resultSet) throws SQLException {
        StringBuilder sqlBuilder = new StringBuilder("INSERT INTO ")
                .append(json.get("table").getAsString())
                .append(" (");
        /*for (int i = 1; i <= rsMetaData.getColumnCount(); i++) {
            sqlBuilder.append(rsMetaData.getColumnName(i));
            if (i < rsMetaData.getColumnCount()) {
                sqlBuilder.append(", ");
            }
        }
        sqlBuilder.append(") VALUES (");
        for (int i = 1; i <= rsMetaData.getColumnCount(); i++) {
            sqlBuilder.append("?");
            if (i < rsMetaData.getColumnCount()) {
                sqlBuilder.append(", ");
            }
        }
        sqlBuilder.append(")");*/

        int columnSize = 0;
        while (resultSet.next()) {
            String columnName = resultSet.getString("COLUMN_NAME");
            sqlBuilder.append(columnName).append(", ");
            columnSize++;
        }
        //循环结束后替换掉最后的逗号
        int lastIndex = sqlBuilder.lastIndexOf(",");
        if (lastIndex >= 0) {
            sqlBuilder.replace(lastIndex, lastIndex + 1, ") VALUES (");
        }
        //追加？占位符
        for (int i = 1; i <= columnSize; i++) {
            sqlBuilder.append("?");
            if (i < columnSize) {
                sqlBuilder.append(", ");
            }
        }
        sqlBuilder.append(")");
        return sqlBuilder.toString();
    }

    private static String buildUpdateStatement(JsonObject json, ResultSet resultSet) throws SQLException {
        StringBuilder sqlBuilder = new StringBuilder("UPDATE ")
                .append(json.get("table").getAsString())
                .append(" SET ");
        /*for (int i = 1; i <= rsMetaData.getColumnCount(); i++) {
            sqlBuilder.append(rsMetaData.getColumnName(i))
                    .append(" = ?");
            if (i < rsMetaData.getColumnCount()) {
                sqlBuilder.append(", ");
            }
        }
        sqlBuilder.append(" WHERE id = ?");*/

        while (resultSet.next()) {
            String columnName = resultSet.getString("COLUMN_NAME");
            sqlBuilder.append(columnName).append(" = ?").append(", ");
        }
        //循环结束后替换掉最后的逗号
        int lastIndex = sqlBuilder.lastIndexOf(",");
        if (lastIndex >= 0) {
            sqlBuilder.replace(lastIndex, lastIndex + 1, "");
        }
        sqlBuilder.append(" WHERE id = ?");

        return sqlBuilder.toString();
    }

    private static String buildDeleteStatement(JsonObject json) {
        return "DELETE FROM " + json.get("table").getAsString() + " WHERE id = ?";
    }
}

