package com.dtxy.sync.dm2orcl;

import com.google.gson.Gson;
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
        String sqlRedo = "UPDATE \"RL_FUEL_RLDD\".\"RL_DYGL_WATERCOALPLAN\" SET \"FOREIGNID\" = '9dc4ec87b9e942bea6b482f535196145', \"JHMZ\" = '03', \"RZ\" = 101, \"LF\" = 10, \"HFF\" = 10, \"QT\" = 120, \"YJZCRQ\" = DATE'2023-07-07', \"NZCG\" IS NULL, \"JHCM\" = '长沙南01', \"JHHC\" = '3', \"JHZHL\" = 200, \"JHKJ\" IS NULL, \"HTSX\" = '20', \"GYSID\" = 'd0c6cc58a66546a59ccf67d1a3470b47', \"GYSNAME\" = '新疆宝鑫昆仑矿业有限责任公司', \"JHCGL\" IS NULL, \"NBHCBM\" = 'DTJT-HNGS-JZS-202310-002', \"YJDCRQ\" = DATE'2023-07-07', \"MDG\" IS NULL, \"SJHCBM\" IS NULL, \"HCLAYER\" IS NULL, \"DYDW\" = '102', \"YJDGSJ\" = DATE'2023-07-07', \"SJDGSJ\" IS NULL, \"CBTBSJ\" IS NULL, \"BW\" IS NULL, \"KBSJ\" IS NULL, \"KGSJ\" IS NULL, \"WGSJ\" IS NULL, \"LGSJ\" = DATE'2023-08-06', \"SJZHL\" IS NULL, \"SJMZ\" IS NULL, \"QNET\" IS NULL, \"MT\" IS NULL, \"AAR\" IS NULL, \"VAR\" IS NULL, \"STAR\" IS NULL, \"STD\" IS NULL, \"ST\" = 20, \"WCQK\" IS NULL, \"SJDCRQ\" IS NULL, \"XQDW\" = '金竹山', \"XQDWID\" = '304', \"SJZHL_DC\" IS NULL, \"SJMZ_DC\" IS NULL, \"QNET_DC\" IS NULL, \"MT_DC\" IS NULL, \"AAR_DC\" IS NULL, \"VAR_DC\" IS NULL, \"STAR_DC\" IS NULL, \"STD_DC\" IS NULL, \"ST_DC\" IS NULL, \"CREATEUSER\" = '模拟用户001', \"STATE\" = '2', \"CREATEORGID\" IS NULL, \"RQ\" IS NULL, \"CID\" IS NULL, \"LCRQ\" IS NULL, \"JHCMID\" = 'de2a5aa7-c456-443f-8e94-63ff1edf22ad', \"HF\" = 10, \"QS\" = 10.23, \"BCLD\" = 11, \"KJ\" = 0, \"YF\" = 1.2, \"DCJ\" = 101, \"BMDJ\" = 7000, \"KDID\" = '8ad184247ce3f851017ce42b1fe3057f', \"VARIETYID\" = 'CFDB82F73C020338E0530A5104740593', \"KDNAME\" = '牛山煤矿', \"VARIETY\" = '牛山煤矿+无烟煤+5300', \"HTH\" = 'CDT-HNGS-JZS-2306-0009-CM', \"YSDWID\" IS NULL, \"YSDW\" IS NULL, \"REMARK\" IS NULL, \"YJLGSJ\" = DATE'2023-08-06', \"ZF\" = 100, \"YJDCSJ\" = DATE'2023-07-07', \"YJLCSJ\" = DATE'2023-08-06', \"YSHTH\" IS NULL, \"RLHTH\" IS NULL, \"CD\" IS NULL, \"UPDATETIME\" = TIMESTAMP'2023-07-07 11:24:20.929', \"CGJHXMID\" IS NULL, \"HRD\" IS NULL, \"SCMCGQKJJG\" IS NULL, \"MXDCXSHTJ\" = 201, \"KBDRPCSCJ\" IS NULL, \"DYSCJ\" = 100, \"MYD\" IS NULL, \"MYDMZ\" IS NULL, \"SZGK\" IS NULL, \"ZGXHSL\" IS NULL, \"ZGLJRZ\" IS NULL, \"ZGLJLF\" IS NULL, \"ZGLJHRD\" IS NULL, \"ZGDWH\" IS NULL, \"HXDCQK\" IS NULL, \"ISGKBHCZ\" IS NULL, \"ISCBASDG\" IS NULL, \"ISCBZHZC\" IS NULL, \"DYYCYY\" IS NULL, \"ZHG\" IS NULL, \"DDZGSJ\" IS NULL, \"DGCCJHTS\" IS NULL, \"ZGKBSJ\" IS NULL, \"ZGLGSJ\" IS NULL, \"ZGYS\" IS NULL, \"ZGYBCS\" IS NULL, \"ZGSJZZL\" IS NULL, \"ZGRZ\" IS NULL, \"ZGLF\" IS NULL, \"ZGHRD\" IS NULL, \"DYXHG\" IS NULL, \"DDXGSJ\" IS NULL, \"XHGKBSJ\" IS NULL, \"XHGLGSJ\" IS NULL, \"XGYS\" IS NULL, \"XGSL\" IS NULL, \"XGRZ\" IS NULL, \"XGLF\" IS NULL, \"XGHRD\" IS NULL, \"XGBZ\" IS NULL, \"CREATETIME\" = TIMESTAMP'2023-07-07 09:26:55', \"UPDATEUSER\" = '48dc3dc5-dcc1-437d-b2ac-ef5805b45c01', \"CREATOR_NAME\" = '模拟用户001', \"MODIFY_NAME\" = '模拟用户001', \"HTID\" = 'ef1a1ef549614c899d05575502efa457', \"DETERMINE_PRICE\" = 100, \"CONFIRM_PURCHASE\" = 'tst', \"FZID\" = '8ad184247ce3f851017ce42b201c0580', \"FZ\" = '牛山煤矿' WHERE \"ID\" = '38fd2c51bbea485ab7dbf5460aaa5a5f' AND \"FOREIGNID\" = '9dc4ec87b9e942bea6b482f535196145' AND \"JHMZ\" = '03' AND \"RZ\" = 101 AND \"LF\" = 10 AND \"HFF\" = 10 AND \"QT\" = 120 AND \"YJZCRQ\" = DATE'2023-07-07' AND \"NZCG\" IS NULL AND \"JHCM\" = '长沙南01' AND \"JHHC\" = '3' AND \"JHZHL\" = 200 AND \"JHKJ\" IS NULL AND \"HTSX\" = '20' AND \"GYSID\" = 'd0c6cc58a66546a59ccf67d1a3470b47' AND \"GYSNAME\" = '新疆宝鑫昆仑矿业有限责任公司' AND \"JHCGL\" IS NULL AND \"NBHCBM\" = 'DTJT-HNGS-JZS-202310-002' AND \"YJDCRQ\" = DATE'2023-07-07' AND \"MDG\" IS NULL AND \"SJHCBM\" IS NULL AND \"HCLAYER\" IS NULL AND \"DYDW\" = '102' AND \"YJDGSJ\" = DATE'2023-07-07' AND \"SJDGSJ\" IS NULL AND \"CBTBSJ\" IS NULL AND \"BW\" IS NULL AND \"KBSJ\" IS NULL AND \"KGSJ\" IS NULL AND \"WGSJ\" IS NULL AND \"LGSJ\" = DATE'2023-08-06' AND \"SJZHL\" IS NULL AND \"SJMZ\" IS NULL AND \"QNET\" IS NULL AND \"MT\" IS NULL AND \"AAR\" IS NULL AND \"VAR\" IS NULL AND \"STAR\" IS NULL AND \"STD\" IS NULL AND \"ST\" = 20 AND \"WCQK\" IS NULL AND \"SJDCRQ\" IS NULL AND \"XQDW\" = '金竹山' AND \"XQDWID\" = '304' AND \"SJZHL_DC\" IS NULL AND \"SJMZ_DC\" IS NULL AND \"QNET_DC\" IS NULL AND \"MT_DC\" IS NULL AND \"AAR_DC\" IS NULL AND \"VAR_DC\" IS NULL AND";
        String rest = "CL\" IS NULL AND \"EJGLJGDZHYTGCL_TITLE\" IS NULL AND \"EJGLJGDZHYTGCL\" IS NULL AND \"TZZTSX\" IS NULL AND \"TZZTSX_TITLE\" IS NULL AND \"SPLX_TITLE\" = '集团审批' AND \"GYSZT_TITLE\" = '正常' AND \"JYNL\" IS NULL AND \"jjptid\" = '0f04a9d8df924b6e85d2fa96bf69ccbb'";
        // Parse SQL Redo and convert to JSON
        JsonObject json = parseUpdateSqlRedoToJson(sqlRedo);


        System.out.println(json);
    }

    public static JsonObject parseInsertSqlRedoToJson(String sqlRedo) {
        String[] parts = sqlRedo.split("\\((?=(?:[^']*'[^']*')*[^']*$)");
        String tableName = parts[0].split("\\bINSERT INTO\\b")[1].replaceAll("\"", "").toUpperCase();
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
            String key = columns[i].replaceAll("\"", "").trim();
            String value = values[i].replaceAll("'", ""); // Remove surrounding single quotes
            // 给内部双引号加上转义符
            value = StringEscapeUtils.escapeJava(value);

            jsonBuilder.append("\"").append(key.toUpperCase()).append("\": \"").append(value).append("\"");

            if (i < columns.length - 1) {
                jsonBuilder.append(",");
            }
        }

        jsonBuilder.append("}");
        jsonBuilder.append("}");


        JsonObject jsonObject = new Gson().fromJson(jsonBuilder.toString(), JsonObject.class);
        jsonObject.addProperty("opr", "insert");

        //return new GsonBuilder().setPrettyPrinting().create().toJson(jsonObject);
        //return new GsonBuilder().create().toJson(jsonObject);
        return jsonObject;
    }

    public static JsonObject parseDelSqlRedoToJson(String sqlRedo) {
        String[] parts = sqlRedo.split("\\bWHERE\\b");
        String tableName = parts[0].split("\\bDELETE FROM\\b")[1].replaceAll("\"", "").toUpperCase();

        String[] conditions = parts[1].trim().split("\\bAND\\b");

        Map<String, String> conditionMap = new HashMap<>();
        for (String condition : conditions) {
            String[] keyValue = condition.trim().split("((?<![!=<>])=|<>(?!=)|<=|>=|<|>|\\bIS\\b)", 2);
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

            jsonBuilder.append("\"").append(key.toUpperCase()).append("\": \"").append(value).append("\"");

            if (i < conditionMap.size() - 1) {
                jsonBuilder.append(",");
            }
            i++;
        }

        jsonBuilder.append("}");
        jsonBuilder.append("}");

        return convertDelUpdateToInsert(jsonBuilder.toString(), true);

    }

    public static JsonObject parseUpdateSqlRedoToJson(String sqlRedo) {
        String[] parts = sqlRedo.split("\\bSET\\b|\\bWHERE\\b");
        String tableName = parts[0].split("\\bUPDATE\\b")[1].trim().replaceAll("\"", "").toUpperCase();
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
            String[] keyValue = pair.split("=|\\bIS\\b", 2);
            if (keyValue.length != 2) {
                throw new IllegalArgumentException("Invalid SQL Redo format");
            }
            String key = keyValue[0].trim().replaceAll("\"", "");
            String value = keyValue[1].trim().replaceAll("'", "");

            // 给内部双引号加上转义符
            value = StringEscapeUtils.escapeJava(value);

            jsonBuilder.append("\"").append(key.toUpperCase()).append("\": \"").append(value).append("\"");
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
            String[] keyValue = condition.split("((?<![!=<>])=|<>(?!=)|<=|>=|<|>|\\bIS\\b)", 2);
            if (keyValue.length != 2) {
                throw new IllegalArgumentException("Invalid SQL Redo format");
            }
            String key = keyValue[0].trim().replaceAll("\"", "");
            String value = keyValue[1].trim().replaceAll("'", "");

            // 给内部双引号加上转义符
            value = StringEscapeUtils.escapeJava(value);

            jsonBuilder.append("\"").append(key.toUpperCase()).append("\": \"").append(value).append("\"");
            if (i < conditions.length - 1) {
                jsonBuilder.append(",");
            }
        }
        jsonBuilder.append("}");

        jsonBuilder.append("}");

        return convertDelUpdateToInsert(jsonBuilder.toString(), false);
    }

    private static JsonObject convertDelUpdateToInsert(String updateResultJson, boolean isDel) {
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
        //return new GsonBuilder().create().toJson(insertResultObject);
        return insertResultObject;
    }
}
