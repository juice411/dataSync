package com.dtxy.sync.dm2orcl;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;

import java.util.HashMap;
import java.util.Map;

public class SqlRedoToJsonConverter {
    public static void main(String[] args) {
        String sqlRedo = "INSERT INTO \"RL_FUEL_RLDD\".\"RL_CONTRACT_CLAUSE\"(\"ID\", \"PARENT_ID\", \"VALUATION_ID\", \"LEFT_VALUE\", \"LEFT_SYMBOL\", \"LABORATORY_INDEX\", \"RIGHT_SYMBOL\", \"RIGHT_VALUE\", \"BASE_PIRCE\", \"BASE_UNIT\", \"FLOATING_START\", \"FLOATING_POINT\", \"FLOATING_PRICE\", \"FLOATING_PRICE_UNIT\", \"KL_METHOD\", \"KC_WEIGHT\", \"KC_UNIT\", \"ITEM_ID\", \"YS_PRICE\", \"ORGAN_ID\", \"DATA_STATE\", \"LAST_UPDATE_DATE\", \"LAST_UPDATED_BY\", \"CREATION_DATE\", \"CREATED_BY\", \"LAST_UPDATE_LOGIN\", \"CLAUSE_NO\", \"KL_WEIGHT\", \"YS_UNIT\", \"CAL_MODE\", \"CAR_TYPE\", \"DATA_ID\", \"TAX_RATE\") VALUES('e1dba108-d5ab-43d5-b44a-5247ba79f066', NULL, '602b3edc-e51d-4b0a-ba84-edd8de4f9934', 20, 1, '2', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '1', NULL, NULL, NULL, NULL, NULL, 10, DATE'2023-06-07', '3a2b4a62-bbbc-4848-9d92-5a97d9b7200b', DATE'2023-06-07', '3a2b4a62-bbbc-4848-9d92-5a97d9b7200b', NULL, '01', '閸憖闁诧靠-(1-濮樻潙鍨�)/(1-0.20))', NULL, NULL, NULL, NULL, NULL)";

        // Parse SQL Redo and convert to JSON
        String json = parseInsertSqlRedoToJson(sqlRedo);

        /*String sqlRedo = "DELETE FROM \"JR_FLOWABLE_FUELMC\".\"ACT_RU_VARIABLE\" WHERE \"ID_\" = '4909c3d5-0344-11ee-9af1-5a7c78946bd9' AND \"REV_\" = 1 AND \"TYPE_\" = 'string' AND \"NAME_\" = 'jr_process_terminate' AND \"EXECUTION_ID_\" = '49099cc3-0344-11ee-9af1-5a7c78946bd9' AND \"PROC_INST_ID_\" = '49099cc3-0344-11ee-9af1-5a7c78946bd9' AND \"TASK_ID_\" IS NULL AND \"SCOPE_ID_\" IS NULL AND \"SUB_SCOPE_ID_\" IS NULL AND \"SCOPE_TYPE_\" IS NULL AND \"BYTEARRAY_ID_\" IS NULL AND \"DOUBLE_\" IS NULL AND \"LONG_\" IS NULL AND \"TEXT_\" = '0' AND \"TEXT2_\" IS NULL";

        // Parse SQL Redo and convert to JSON
        String json = parseDelSqlRedoToJson(sqlRedo);*/

        //String sqlRedo = "UPDATE \"RL_FUEL_RLDD\".\"RL_CKC_KDKKSP\" SET \"JSRQ\" = '2019-10-23' WHERE \"ID\" = 'af734cd4-6068-4341-8f4d-3946a6a7d2ce' AND \"JSDID\" = 'b368e8b4-7b70-4517-bb83-865d3c309139' AND \"JSDBH\" = 'CDT-JS-HN-AYFD-1910-026-C' AND \"JSLX\" = '3' AND \"JSYJ\" = '2' AND \"HTBZSL\" = '1.0' AND \"HTBZZL\" = '70' AND \"YSPJEHJ\" = '0' AND \"STATUS\" = '1' AND \"KFCSYF\" = '8912926' AND \"QYCSYF\" = '8912926' AND \"REMARK\" IS NULL AND \"DSFSL\" = '23339.27' AND \"DSFZL\" = '4019' AND \"HTKJ\" = '0.09502' AND \"KFKJ\" = '0.09502' AND \"HTKK\" = '0' AND \"HTKD\" = '0' AND \"HTHJSP\" = '0' AND \"JSHTJJE\" = '0' AND \"BZSPSH\" IS NULL AND \"BZSPLY\" IS NULL AND \"HTBH\" IS NULL AND \"JSRQ\" IS NULL AND \"XFDWNAME\" IS NULL AND \"SKDWMC\" IS NULL AND \"GHSL\" IS NULL AND \"JSSL\" IS NULL AND \"YSRZ\" IS NULL AND \"JSRZ\" IS NULL AND \"JSMJ\" IS NULL AND \"HJDJ\" IS NULL AND \"DCBMDJ\" IS NULL AND \"YSFS\" IS NULL AND \"PROC_INST_ID\" IS NULL AND \"FZGS_QR\" = '1' AND \"FZGS_QRSJ\" = DATE'2019-10-30' AND \"FZGS_QRR\" = '18638269787' AND \"CLFJ\" IS NULL AND \"SHFJ\" IS NULL AND \"FZGSSHYJ\" IS NULL AND \"CREATEUSER\" = '15936822190' AND \"CREATETIME\" = DATE'2019-10-28' AND \"UPDATEUSER\" = '15936822190' AND \"UPDATETIME\" = DATE'2019-10-28' AND \"CREATOR_NAME\" IS NULL AND \"MODIFY_NAME\" IS NULL AND \"POWER_ORGID\" IS NULL AND \"FZGS_ORGID\" IS NULL AND \"HANDLE_STATUS\" = '3' AND \"SKDWID\" IS NULL AND \"MINE_NAME\" IS NULL AND \"SKDWTYPE\" IS NULL AND \"HTSX\" IS NULL AND \"CFZL\" IS NULL AND \"JSZL\" IS NULL AND \"BJ_REASON\" IS NULL AND \"AUDIT_STATUS\" IS NULL";
        /*String sqlRedo = "UPDATE \"RL_FUEL_RLDD\".\"RL_CONTRACT_WEIGHTING\" SET \"WEIGHTING_METHOD\" = '3', \"LEFT_VALUE\" = 4000, \"LEFT_SYMBOL\" = 2, \"LABORATORY_INDEX\" = '14', \"RIGHT_SYMBOL\" = 1, \"RIGHT_VALUE\" = 7000, \"DATA_STATE\" = 10, \"CONTRACT_ID\" = '3f989d912a2a470e9b0682f34bb5342a', \"WEIGHTED_GROUP1\" = '1,2,3,4,5,6', \"WEIGHTED_GROUP2\" = '', \"LAST_UPDATE_DATE\" = TIMESTAMP'2023-06-07 11:19:24.075', \"LAST_UPDATED_BY\" = '3a2b4a62-bbbc-4848-9d92-5a97d9b7200b', \"CREATED_BY\" = '3a2b4a62-bbbc-4848-9d92-5a97d9b7200b' WHERE \"ID\" = 'dc90243b-4db3-4d27-9c51-336d75d16237' AND \"VALUATION_ID\" IS NULL AND \"WEIGHTING_METHOD\" = '3' AND \"LEFT_VALUE\" = 4000 AND \"LEFT_SYMBOL\" = 2 AND \"LABORATORY_INDEX\" = '14' AND \"RIGHT_SYMBOL\" = 1 AND \"RIGHT_VALUE\" = 7000 AND \"IS_SINGLE\" IS NULL AND \"DATA_STATE\" = 10 AND \"CONTRACT_ID\" = '3f989d912a2a470e9b0682f34bb5342a' AND \"WEIGHTED_GROUP1\" = '1,2,3,4,5,6' AND \"WEIGHTED_GROUP2\" = '' AND \"WEIGHTED_GROUP1_NAME\" IS NULL AND \"WEIGHTED_GROUP2_NAME\" IS NULL AND \"SOURCE_ID\" IS NULL AND \"DATA_ID\" IS NULL AND \"LAST_UPDATE_DATE\" = TIMESTAMP'2023-06-06 18:45:47.967' AND \"LAST_UPDATED_BY\" = '3a2b4a62-bbbc-4848-9d92-5a97d9b7200b' AND \"CREATION_DATE\" = TIMESTAMP'2023-06-06 18:43:11.894' AND \"CREATED_BY\" = '3a2b4a62-bbbc-4848-9d92-5a97d9b7200b' AND \"LAST_UPDATE_LOGIN\" IS NULL";

        // Parse SQL Redo and convert to JSON
        String json = parseUpdateSqlRedoToJson(sqlRedo);*/


        System.out.println(json);
    }

    public static String parseInsertSqlRedoToJson(String sqlRedo) {
        String[] parts = sqlRedo.split("\\((?=(?:[^']*'[^']*')*[^']*$)");
        String tableName = parts[0].split("\\bINSERT INTO\\b")[1].replaceAll("\"", "");
        String columnsPart = parts[1].split("\\)")[0];
        String valuesPart = parts[2].split("\\)(?=(?:[^']*'[^']*')*[^']*$)")[0];

        String[] columns = columnsPart.split(", ");
        String[] values = valuesPart.split(",(?=(?:[^']*'[^']*')*[^']*$)");

        if (columns.length != values.length) {
            throw new IllegalArgumentException("Invalid SQL Redo format");
        }

        StringBuilder jsonBuilder = new StringBuilder();
        jsonBuilder.append("{");
        jsonBuilder.append("\"table\": \"").append(tableName).append("\",");
        jsonBuilder.append("\"values\": {");

        for (int i = 0; i < columns.length; i++) {
            String column = columns[i].replaceAll("\"", "");
            String value = values[i].replaceAll("'", ""); // Remove surrounding single quotes

            jsonBuilder.append("\"").append(column).append("\": \"").append(value).append("\"");

            if (i < columns.length - 1) {
                jsonBuilder.append(",");
            }
        }

        jsonBuilder.append("}");
        jsonBuilder.append("}");

        JsonObject jsonObject = new Gson().fromJson(jsonBuilder.toString(), JsonObject.class);
        jsonObject.addProperty("opr", "insert");

        //return new GsonBuilder().setPrettyPrinting().create().toJson(jsonObject);
        return new GsonBuilder().create().toJson(jsonObject);
    }

    public static String parseDelSqlRedoToJson(String sqlRedo) {
        String[] parts = sqlRedo.split("\\bWHERE\\b");
        String tableName = parts[0].split("\\bDELETE FROM\\b")[1].replaceAll("\"", "");

        String[] conditions = parts[1].trim().split("\\bAND\\b");

        Map<String, String> conditionMap = new HashMap<>();
        for (String condition : conditions) {
            String[] keyValue = condition.trim().split("((?<![!=<>])=|<>(?!=)|<=|>=|<|>|\\bIS\\b)");
            String key = keyValue[0].trim().replaceAll("\"", "");
            String value = keyValue[1].trim().replaceAll("'", "");

            if (value.equalsIgnoreCase("NULL")) {
                value = null;
            }

            conditionMap.put(key, value);
        }

        StringBuilder jsonBuilder = new StringBuilder();
        jsonBuilder.append("{");
        jsonBuilder.append("\"table\": \"").append(tableName).append("\",");
        jsonBuilder.append("\"where\": {");

        int i = 0;
        for (Map.Entry<String, String> entry : conditionMap.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();

            jsonBuilder.append("\"").append(key).append("\": \"").append(value).append("\"");

            if (i < conditionMap.size() - 1) {
                jsonBuilder.append(",");
            }
            i++;
        }

        jsonBuilder.append("}");
        jsonBuilder.append("}");

        return convertDelUpdateToInsert(jsonBuilder.toString(), true);

    }

    public static String parseUpdateSqlRedoToJson(String sqlRedo) {
        String[] parts = sqlRedo.split("\\bSET\\b|\\bWHERE\\b");
        String tableName = parts[0].split("\\bUPDATE\\b")[1].trim().replaceAll("\"", "");
        String setClause = parts[1].trim();
        String whereClause = parts[2].trim();

        StringBuilder jsonBuilder = new StringBuilder();
        jsonBuilder.append("{");

        jsonBuilder.append("\"table\": \"").append(tableName).append("\",");

        // Parse SET clause
        jsonBuilder.append("\"set\": {");
        String[] setPairs = setClause.split(",(?=(?:[^']*'[^']*')*[^']*$)");
        for (int i = 0; i < setPairs.length; i++) {
            String pair = setPairs[i];
            String[] keyValue = pair.split("=|\\bIS\\b");
            if (keyValue.length != 2) {
                throw new IllegalArgumentException("Invalid SQL Redo format");
            }
            String key = keyValue[0].trim().replaceAll("\"", "");
            String value = keyValue[1].trim().replaceAll("'", "");
            jsonBuilder.append("\"").append(key).append("\": \"").append(value).append("\"");
            if (i < setPairs.length - 1) {
                jsonBuilder.append(",");
            }
        }
        jsonBuilder.append("},");

        // Parse WHERE clause
        jsonBuilder.append("\"where\": {");
        String[] conditions = whereClause.split("\\bAND\\b");
        for (int i = 0; i < conditions.length; i++) {
            String condition = conditions[i].trim();
            String[] keyValue = condition.split("((?<![!=<>])=|<>(?!=)|<=|>=|<|>|\\bIS\\b)");
            if (keyValue.length != 2) {
                throw new IllegalArgumentException("Invalid SQL Redo format");
            }
            String key = keyValue[0].trim().replaceAll("\"", "");
            String value = keyValue[1].trim().replaceAll("'", "");
            jsonBuilder.append("\"").append(key).append("\": \"").append(value).append("\"");
            if (i < conditions.length - 1) {
                jsonBuilder.append(",");
            }
        }
        jsonBuilder.append("}");

        jsonBuilder.append("}");

        return convertDelUpdateToInsert(jsonBuilder.toString(), false);
    }

    private static String convertDelUpdateToInsert(String updateResultJson, boolean isDel) {
        JsonObject updateResultObject = new Gson().fromJson(updateResultJson, JsonObject.class);
        JsonObject insertResultObject = new JsonObject();

        insertResultObject.addProperty("table", updateResultObject.get("table").getAsString());

        JsonObject valuesObject = new JsonObject();

        for (String key : updateResultObject.get("where").getAsJsonObject().keySet()) {
            valuesObject.addProperty(key, updateResultObject.get("where").getAsJsonObject().get(key).getAsString());
        }

        if (!isDel) {
            //保留sets信息
            JsonObject setsObject = new JsonObject();
            for (String key : updateResultObject.get("set").getAsJsonObject().keySet()) {
                valuesObject.addProperty(key, updateResultObject.get("set").getAsJsonObject().get(key).getAsString());
                setsObject.addProperty(key, updateResultObject.get("set").getAsJsonObject().get(key).getAsString());
            }
            insertResultObject.addProperty("opr", "update");
            insertResultObject.add("set", setsObject);
        } else {
            insertResultObject.addProperty("opr", "delete");
        }

        insertResultObject.add("values", valuesObject);


        //return new GsonBuilder().setPrettyPrinting().create().toJson(insertResultObject);
        return new GsonBuilder().create().toJson(insertResultObject);
    }
}
