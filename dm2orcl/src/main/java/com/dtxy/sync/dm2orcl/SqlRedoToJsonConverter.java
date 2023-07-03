package com.dtxy.sync.dm2orcl;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import org.apache.commons.text.StringEscapeUtils;

import java.util.HashMap;
import java.util.Map;

public class SqlRedoToJsonConverter {
    public static void main(String[] args) {
        //String sqlRedo = "INSERT INTO \"RL_FUEL_RLDD\".\"RL_CONTRACT_CLAUSE\"(\"ID\", \"PARENT_ID\", \"VALUATION_ID\", \"LEFT_VALUE\", \"LEFT_SYMBOL\", \"LABORATORY_INDEX\", \"RIGHT_SYMBOL\", \"RIGHT_VALUE\", \"BASE_PIRCE\", \"BASE_UNIT\", \"FLOATING_START\", \"FLOATING_POINT\", \"FLOATING_PRICE\", \"FLOATING_PRICE_UNIT\", \"KL_METHOD\", \"KC_WEIGHT\", \"KC_UNIT\", \"ITEM_ID\", \"YS_PRICE\", \"ORGAN_ID\", \"DATA_STATE\", \"LAST_UPDATE_DATE\", \"LAST_UPDATED_BY\", \"CREATION_DATE\", \"CREATED_BY\", \"LAST_UPDATE_LOGIN\", \"CLAUSE_NO\", \"KL_WEIGHT\", \"YS_UNIT\", \"CAL_MODE\", \"CAR_TYPE\", \"DATA_ID\", \"TAX_RATE\") VALUES('e1dba108-d5ab-43d5-b44a-5247ba79f066', NULL, '602b3edc-e51d-4b0a-ba84-edd8de4f9934', 20, 1, '2', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '1', NULL, NULL, NULL, NULL, NULL, 10, DATE'2023-06-07', '3a2b4a62-bbbc-4848-9d92-5a97d9b7200b', DATE'2023-06-07', '3a2b4a62-bbbc-4848-9d92-5a97d9b7200b', NULL, '01', '閸憖闁诧靠-(1-濮樻潙鍨�)/(1-0.20))', NULL, NULL, NULL, NULL, NULL)";
        //String sqlRedo="INSERT INTO \"RL_FUEL_RLDD\".\"RL_CONTRACT\"(\"ID\", \"CHANGED_CONTRACT_ID\", \"CONTRACT_TYPE\", \"ORDER_GOODS_MODE\", \"CONTRACT_TAB\", \"FIRST_PARTY_ID\", \"SECOND_PARTY_ID\", \"BILL_NO\", \"VERSION_NO\", \"NAME\", \"SIGN_PLACE\", \"SIGN_DATE\", \"START_DATE\", \"END_DATE\", \"TRANSPORT\", \"AUDIT_STATE\", \"ORGAN_ID\", \"DATA_STATE\", \"LAST_UPDATE_DATE\", \"LAST_UPDATED_BY\", \"CREATION_DATE\", \"CREATED_BY\", \"DATA_ID\", \"CONTRACT_CATEGORY\", \"BUY_MODE\", \"BUY_TYPE\", \"WEIGHT_ACCORDING\", \"ASSAY_ACCORDING\", \"IS_LOSS\", \"LOSS\", \"PRICE_TYPE\", \"BILLING_MODE\", \"RECEIVING_TYPE\", \"IS_BEAR\", \"COUNTERSIGNED_CONTENT\", \"PROFIT_EVALUATION\", \"BUSINESS_TYPE\", \"MANAGE_TYPE\", \"CONTRACT_MAIN_ID\", \"BALANCE_DEPTCODE\", \"BIDDING_NO\", \"BIDDING_ID\", \"MATCH_DATE_TYPE\", \"STATUS\", \"ORGLAYER\", \"APPROVAL_TYPE\", \"SOURCE_ID\", \"CONTRACT_TERMS\", \"NOUSE_STANDARD_TEMPLATE\", \"UNIFY_RESULTMSG\", \"PAYMENT_METHOD\", \"STANDARD_NUMBER\", \"STANDARD_QUALITY\", \"CURRENCY_UNIT\", \"SUPPLY_CHAIN_UNIT\", \"ISLAW\", \"IN_ID\", \"ATTR1\", \"IF_ACCOUNT_BY_GOODS\", \"IF_DLRL_CONTRACT\", \"REMOVE_BPM_TIME\", \"REMOVE_ACCOUNT\", \"FIRST_AUDIT_DATE\", \"IF_DZCG\", \"APPLY_UNLOCK\", \"IS_PRICE\", \"OPTYPE\", \"GX_STATUS\", \"IS_EDIT\", \"PROC_INST_ID\", \"CREATOR_NAME\", \"MODIFY_NAME\", \"FORMAL_SIGN\") VALUES('cfa2c23512664b2192e874671f34aa89', NULL, '30', NULL, '30', '8ad184247ee84d1b017ee866f70a0167', '40288a81513e22bd01513e7427d70245', 'CDT-DTNMGDLMHGYXZRGS-2307-0001-CZ-L', '001', '测试杂费合同_yd2', '长沙', DATE'2023-07-03', DATE'2023-07-03', DATE'2023-11-30', NULL, '0', '8ad184247ee84d1b017ee866f70a0167', 10, TIMESTAMP'2023-07-03 10:34:55', 'cdlm', TIMESTAMP'2023-07-03 10:34:55', 'cdlm', NULL, '10', '20', NULL, '30', NULL, NULL, NULL, NULL, '1', NULL, NULL, '会签单主要内容文本域', '利润测算文本域', NULL, '10', '813e50949ccd4a45baede559a1266842', NULL, NULL, NULL, '10', '0', NULL, '02', NULL, '<p><span style=\"color: rgb(13, 20, 30); background-color: rgb(255, 255, 255);\">任职福建、浙江期间，习近平曾多次到访香港；到中央工作后，他负责中央港澳事务协调工作，对香港情况了解更加深入。2008年7月，时任国家副主席的习近平赴香港考察，给香港民众留下深刻印象。2017年、2022年，在香港回归祖国20周年、25周年的重大时刻，习近平主席都来到这里，为祝福、为支持、更为谋划更好的发展。</span></p>', '<p><span style=\"color: rgb(51, 51, 51); background-color: rgb(255, 255, 255); font-size: 13px;\">蔡徐坤事件发酵近一周之后，仍旧未等到其回应，反而等来了各大平台删除其信息的动态。继央视频后，多家平台清空其相关内容</span></p>', NULL, NULL, 23, 4577, '10', '104', NULL, NULL, NULL, '0', '0', NULL, NULL, NULL, NULL, NULL, '0', NULL, NULL, NULL, NULL, '测试多伦煤化工', '测试多伦煤化工', NULL)";
        // Parse SQL Redo and convert to JSON
        //String json = parseInsertSqlRedoToJson(sqlRedo);

        /*String sqlRedo = "DELETE FROM \"JR_FLOWABLE_FUELMC\".\"ACT_RU_VARIABLE\" WHERE \"ID_\" = '4909c3d5-0344-11ee-9af1-5a7c78946bd9' AND \"REV_\" = 1 AND \"TYPE_\" = 'string' AND \"NAME_\" = 'jr_process_terminate' AND \"EXECUTION_ID_\" = '49099cc3-0344-11ee-9af1-5a7c78946bd9' AND \"PROC_INST_ID_\" = '49099cc3-0344-11ee-9af1-5a7c78946bd9' AND \"TASK_ID_\" IS NULL AND \"SCOPE_ID_\" IS NULL AND \"SUB_SCOPE_ID_\" IS NULL AND \"SCOPE_TYPE_\" IS NULL AND \"BYTEARRAY_ID_\" IS NULL AND \"DOUBLE_\" IS NULL AND \"LONG_\" IS NULL AND \"TEXT_\" = '0' AND \"TEXT2_\" IS NULL";

        // Parse SQL Redo and convert to JSON
        String json = parseDelSqlRedoToJson(sqlRedo);*/

        //String sqlRedo = "UPDATE \"RL_FUEL_RLDD\".\"RL_CKC_KDKKSP\" SET \"JSRQ\" = '2019-10-23' WHERE \"ID\" = 'af734cd4-6068-4341-8f4d-3946a6a7d2ce' AND \"JSDID\" = 'b368e8b4-7b70-4517-bb83-865d3c309139' AND \"JSDBH\" = 'CDT-JS-HN-AYFD-1910-026-C' AND \"JSLX\" = '3' AND \"JSYJ\" = '2' AND \"HTBZSL\" = '1.0' AND \"HTBZZL\" = '70' AND \"YSPJEHJ\" = '0' AND \"STATUS\" = '1' AND \"KFCSYF\" = '8912926' AND \"QYCSYF\" = '8912926' AND \"REMARK\" IS NULL AND \"DSFSL\" = '23339.27' AND \"DSFZL\" = '4019' AND \"HTKJ\" = '0.09502' AND \"KFKJ\" = '0.09502' AND \"HTKK\" = '0' AND \"HTKD\" = '0' AND \"HTHJSP\" = '0' AND \"JSHTJJE\" = '0' AND \"BZSPSH\" IS NULL AND \"BZSPLY\" IS NULL AND \"HTBH\" IS NULL AND \"JSRQ\" IS NULL AND \"XFDWNAME\" IS NULL AND \"SKDWMC\" IS NULL AND \"GHSL\" IS NULL AND \"JSSL\" IS NULL AND \"YSRZ\" IS NULL AND \"JSRZ\" IS NULL AND \"JSMJ\" IS NULL AND \"HJDJ\" IS NULL AND \"DCBMDJ\" IS NULL AND \"YSFS\" IS NULL AND \"PROC_INST_ID\" IS NULL AND \"FZGS_QR\" = '1' AND \"FZGS_QRSJ\" = DATE'2019-10-30' AND \"FZGS_QRR\" = '18638269787' AND \"CLFJ\" IS NULL AND \"SHFJ\" IS NULL AND \"FZGSSHYJ\" IS NULL AND \"CREATEUSER\" = '15936822190' AND \"CREATETIME\" = DATE'2019-10-28' AND \"UPDATEUSER\" = '15936822190' AND \"UPDATETIME\" = DATE'2019-10-28' AND \"CREATOR_NAME\" IS NULL AND \"MODIFY_NAME\" IS NULL AND \"POWER_ORGID\" IS NULL AND \"FZGS_ORGID\" IS NULL AND \"HANDLE_STATUS\" = '3' AND \"SKDWID\" IS NULL AND \"MINE_NAME\" IS NULL AND \"SKDWTYPE\" IS NULL AND \"HTSX\" IS NULL AND \"CFZL\" IS NULL AND \"JSZL\" IS NULL AND \"BJ_REASON\" IS NULL AND \"AUDIT_STATUS\" IS NULL";
        String sqlRedo = "UPDATE \"RL_FUEL_RLDD\".\"RL_CONTRACT\" SET \"CONTRACT_TYPE\" = '30', \"CONTRACT_TAB\" = '30', \"FIRST_PARTY_ID\" = '8ad184247ee84d1b017ee866f70a0167', \"SECOND_PARTY_ID\" = '40288a81513e22bd01513e7427d70245', \"BILL_NO\" = 'CDT-DTNMGDLMHGYXZRGS-2307-0001-CZ-L', \"VERSION_NO\" = '001', \"NAME\" = '测试杂费合同_yd2', \"SIGN_PLACE\" = '长沙', \"SIGN_DATE\" = DATE'2023-07-03', \"START_DATE\" = DATE'2023-07-03', \"END_DATE\" = DATE'2023-11-30', \"AUDIT_STATE\" = '0', \"ORGAN_ID\" = '8ad184247ee84d1b017ee866f70a0167', \"DATA_STATE\" = 10, \"LAST_UPDATE_DATE\" = TIMESTAMP'2023-07-03 10:38:16', \"LAST_UPDATED_BY\" = 'cdlm', \"CREATED_BY\" = 'cdlm', \"CONTRACT_CATEGORY\" = '10', \"BUY_MODE\" = '20', \"WEIGHT_ACCORDING\" = '30', \"BILLING_MODE\" = '1', \"COUNTERSIGNED_CONTENT\" = '会签单主要内容文本域', \"PROFIT_EVALUATION\" = '利润测算文本域', \"MANAGE_TYPE\" = '10', \"CONTRACT_MAIN_ID\" = '813e50949ccd4a45baede559a1266842', \"MATCH_DATE_TYPE\" = '10', \"STATUS\" = '0', \"APPROVAL_TYPE\" = '02', \"CONTRACT_TERMS\" = '<p><span style=\"color: rgb(13, 20, 30); background-color: rgb(255, 255, 255);\">任职福建、浙江期间，习近平曾多次到访香港；到中央工作后，他负责中央港澳事务协调工作，对香港情况了解更加深入。2008年7月，时任国家副主席的习近平赴香港考察，给香港民众留下深刻印象。2017年、2022年，在香港回归祖国20周年、25周年的重大时刻，习近平主席都来到这里，为祝福、为支持、更为谋划更好的发展。</span></p>', \"NOUSE_STANDARD_TEMPLATE\" = '<p><span style=\"color: rgb(51, 51, 51); background-color: rgb(255, 255, 255); font-size: 13px;\">蔡徐坤事件发酵近一周之后，仍旧未等到其回应，反而等来了各大平台删除其信息的动态。继央视频后，多家平台清空其相关内容</span></p>', \"STANDARD_NUMBER\" = 23, \"STANDARD_QUALITY\" = 4577, \"CURRENCY_UNIT\" = '10', \"SUPPLY_CHAIN_UNIT\" = '104', \"IF_ACCOUNT_BY_GOODS\" = '0', \"IF_DLRL_CONTRACT\" = '0', \"IS_PRICE\" = '0', \"CREATOR_NAME\" = '测试多伦煤化工', \"MODIFY_NAME\" = '测试多伦煤化工' WHERE \"ID\" = 'cfa2c23512664b2192e874671f34aa89' AND \"CHANGED_CONTRACT_ID\" IS NULL AND \"CONTRACT_TYPE\" = '30' AND \"ORDER_GOODS_MODE\" IS NULL AND \"CONTRACT_TAB\" = '30' AND \"FIRST_PARTY_ID\" = '8ad184247ee84d1b017ee866f70a0167' AND \"SECOND_PARTY_ID\" = '40288a81513e22bd01513e7427d70245' AND \"BILL_NO\" = 'CDT-DTNMGDLMHGYXZRGS-2307-0001-CZ-L' AND \"VERSION_NO\" = '001' AND \"NAME\" = '测试杂费合同_yd2' AND \"SIGN_PLACE\" = '长沙' AND \"SIGN_DATE\" = DATE'2023-07-03' AND \"START_DATE\" = DATE'2023-07-03' AND \"END_DATE\" = DATE'2023-11-30' AND \"TRANSPORT\" IS NULL AND \"AUDIT_STATE\" = '0' AND \"ORGAN_ID\" = '8ad184247ee84d1b017ee866f70a0167' AND \"DATA_STATE\" = 10 AND \"LAST_UPDATE_DATE\" = TIMESTAMP'2023-07-03 10:34:55' AND \"LAST_UPDATED_BY\" = 'cdlm' AND \"CREATION_DATE\" = TIMESTAMP'2023-07-03 10:34:55' AND \"CREATED_BY\" = 'cdlm' AND \"DATA_ID\" IS NULL AND \"CONTRACT_CATEGORY\" = '10' AND \"BUY_MODE\" = '20' AND \"BUY_TYPE\" IS NULL AND \"WEIGHT_ACCORDING\" = '30' AND \"ASSAY_ACCORDING\" IS NULL AND \"IS_LOSS\" IS NULL AND \"LOSS\" IS NULL AND \"PRICE_TYPE\" IS NULL AND \"BILLING_MODE\" = '1' AND \"RECEIVING_TYPE\" IS NULL AND \"IS_BEAR\" IS NULL AND \"COUNTERSIGNED_CONTENT\" = '会签单主要内容文本域' AND \"PROFIT_EVALUATION\" = '利润测算文本域' AND \"BUSINESS_TYPE\" IS NULL AND \"MANAGE_TYPE\" = '10' AND \"CONTRACT_MAIN_ID\" = '813e50949ccd4a45baede559a1266842' AND \"BALANCE_DEPTCODE\" IS NULL AND \"BIDDING_NO\" IS NULL AND \"BIDDING_ID\" IS NULL AND \"MATCH_DATE_TYPE\" = '10' AND \"STATUS\" = '0' AND \"ORGLAYER\" IS NULL AND \"APPROVAL_TYPE\" = '02' AND \"SOURCE_ID\" IS NULL AND \"UNIFY_RESULTMSG\" IS NULL AND \"PAYMENT_METHOD\" IS NULL AND \"STANDARD_NUMBER\" = 23 AND \"STANDARD_QUALITY\" = 4577 AND \"CURRENCY_UNIT\" = '10' AND \"SUPPLY_CHAIN_UNIT\" = '104' AND \"ISLAW\" IS NULL AND \"IN_ID\" IS NULL AND \"ATTR1\" IS NULL AND \"IF_ACCOUNT_BY_GOODS\" = '0' AND \"IF_DLRL_CONTRACT\" = '0' AND \"REMOVE_BPM_TIME\" IS NULL AND \"REMOVE_ACCOUNT\" IS NULL AND \"FIRST_AUDIT_DATE\" IS NULL AND \"IF_DZCG\" IS NULL AND \"APPLY_UNLOCK\" IS NULL AND \"IS_PRICE\" = '0' AND \"OPTYPE\" IS NULL AND \"GX_STATUS\" IS NULL AND \"IS_EDIT\" IS NULL AND \"PROC_INST_ID\" IS NULL AND \"CREATOR_NAME\" = '测试多伦煤化工' AND \"MODIFY_NAME\" = '测试多伦煤化工' AND \"FORMAL_SIGN\" IS NULL;";

        // Parse SQL Redo and convert to JSON
        String json = parseUpdateSqlRedoToJson(sqlRedo);


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
            // 给内部双引号加上转义符
            value = StringEscapeUtils.escapeJava(value);

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
            String[] keyValue = condition.trim().split("((?<![!=<>])=|<>(?!=)|<=|>=|<|>|\\bIS\\b)",2);
            String key = keyValue[0].trim().replaceAll("\"", "");
            String value = keyValue[1].trim().replaceAll("'", "");
            // 给内部双引号加上转义符
            value = StringEscapeUtils.escapeJava(value);
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
            String[] keyValue = pair.split("=|\\bIS\\b",2);
            if (keyValue.length != 2) {
                throw new IllegalArgumentException("Invalid SQL Redo format");
            }
            String key = keyValue[0].trim().replaceAll("\"", "");
            String value = keyValue[1].trim().replaceAll("'", "");

            // 给内部双引号加上转义符
            value = StringEscapeUtils.escapeJava(value);

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
            String[] keyValue = condition.split("((?<![!=<>])=|<>(?!=)|<=|>=|<|>|\\bIS\\b)",2);
            if (keyValue.length != 2) {
                throw new IllegalArgumentException("Invalid SQL Redo format");
            }
            String key = keyValue[0].trim().replaceAll("\"", "");
            String value = keyValue[1].trim().replaceAll("'", "");

            // 给内部双引号加上转义符
            value = StringEscapeUtils.escapeJava(value);

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
