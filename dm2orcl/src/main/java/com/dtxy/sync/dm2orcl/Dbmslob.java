package com.dtxy.sync.dm2orcl;

import com.google.gson.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;

public class Dbmslob {
    private static final Logger logger = LoggerFactory.getLogger(Dbmslob.class);
    private static final String moniterTable;

    static {

        //加载记录位置
        try {
            PositionRecorder.loadLastProcessedPosition();
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("出错了：{}", e.getMessage());
        }

        moniterTable = ExcelReader.getTables();

    }

    public static void parseMinerLog(String archPath) {
        Connection connection = null;
        Statement statement = null;
        PreparedStatement selectStatement = null;
        ResultSet resultSet = null;
        try {
            connection = ConfigUtil.getDMDs().getConnection();
            statement = connection.createStatement();

            String sql_addLogFile = String.format("DBMS_LOGMNR.ADD_LOGFILE('%s');", archPath);
            //String sql_startLogmnr="DBMS_LOGMNR.START_LOGMNR(OPTIONS=>2128 , STARTTIME=>TO_DATE('2023-07-07 11:20:00','YYYY-MM-DD HH24:MI:SS') , ENDTIME=>TO_DATE('2023-07-07 11:25:00','YYYY-MM-DD HH24:MI:SS'));";
            String sql_startLogmnr = String.format("DBMS_LOGMNR.START_LOGMNR(OPTIONS=>2128 , STARTSCN=>%d);", PositionRecorder.getLastProcessedPosition() + 1);

            statement.addBatch(sql_addLogFile);
            statement.addBatch(sql_startLogmnr);

            statement.executeBatch();
            statement.close();

            // 查询数据
            String selectSql = String.format("select scn,sql_redo,OPERATION_CODE,COMMIT_TIMESTAMP,SEG_OWNER,TABLE_NAME,ssn,csf from v$logmnr_contents where scn >=%d and seg_owner in (%s) and table_name in (%s) and operation_code in (1,2,3) order by scn", PositionRecorder.getLastProcessedPosition() + 1, ConfigUtil.getProperty("monitor.segOwner"), moniterTable);
            selectStatement = connection.prepareStatement(selectSql);
            resultSet = selectStatement.executeQuery();

            //临时拼接sql,保障完整性
            StringBuilder sqlBuilder = new StringBuilder();

            // 解析查询结果
            while (resultSet.next()) {
                long scn = resultSet.getLong("scn");
                String sql_redo = resultSet.getString("sql_redo");
                String commitTime = resultSet.getString("COMMIT_TIMESTAMP");
                int opr_code = resultSet.getInt("OPERATION_CODE");
                int ssn = resultSet.getInt("ssn");
                int csf = resultSet.getInt("csf");

                //检查sql完整性
                if (ssn == 0 && csf == 1) {//说明sql还有后续内容
                    if (sqlBuilder.length() > 0) {//说明后边的部分已经提前追加到里面了
                        sqlBuilder.insert(0, sql_redo);
                        sql_redo = sqlBuilder.toString();
                        // 清空内容
                        sqlBuilder.setLength(0);
                    } else {
                        sqlBuilder.append(sql_redo);
                        continue;
                    }

                } else if (ssn == 1 && csf == 1) {//说明sql仍然未结束
                    sqlBuilder.append(" ").append(sql_redo);
                    continue;
                } else if (ssn == 1 && csf == 0) {//说明sql已结束
                    if (sqlBuilder.length() == 0) {//说明后边的部分先到了，需要等前边的过来
                        sqlBuilder.append(" ").append(sql_redo);
                        continue;
                    } else {
                        sqlBuilder.append(" ").append(sql_redo);
                        sql_redo = sqlBuilder.toString();
                        // 清空内容
                        sqlBuilder.setLength(0);
                    }

                }

                // 处理查询结果
                JsonObject jsonObject = null;
                if (opr_code == 1) {
                    jsonObject = SqlRedoToJsonConverter.parseInsertSqlRedoToJson(sql_redo);

                } else if (opr_code == 2) {
                    jsonObject = SqlRedoToJsonConverter.parseDelSqlRedoToJson(sql_redo);

                } else if (opr_code == 3) {
                    jsonObject = SqlRedoToJsonConverter.parseUpdateSqlRedoToJson(sql_redo);

                }

                logger.debug("原始信息：scn：{}，commit_time:{}，sql:{}", scn, commitTime, sql_redo);
                OracleWriter.sync2Oracle(jsonObject);

                //记录处理位置
                PositionRecorder.recordPosition(scn);
            }

        } catch (Exception e) {
            e.printStackTrace();
            logger.error("出错了：{}", e.getMessage());
        } finally {
            try {
                PositionRecorder.savePositionToFile();
            } catch (IOException e) {
                e.printStackTrace();
                logger.error("保存scn出错了：{}", e.getMessage());
            }

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

    public static void parseMinerLog() {
        //方便调试,用完记得注释
        int opr_code=3;
        String sql_redo="UPDATE \"RL_FUEL_RLDD\".\"rl_gysgl_gysxx\" SET \"Gysqc\" = '马鞍山源润物流有限公司', \"Sh\" = '340521087550717', \"Sjgysbm\" = '340521087550717', \"Gysbm\" = '340521087550717', \"Entityid\" = 'jjpt1694871edee304ae1afbf187bd1b5cb97', \"Splx\" = '1', \"Frdb\" = '曹江水', \"Gysflname\" = '6', \"Dqid\" = '00340005', \"Dqname\" = '马鞍山市', \"Sfsc\" = '1', \"Ndscnl\" = 0, \"Jjkfnx\" = 0, \"Gysclsj\" = TIMESTAMP'2018-05-18 14:17:54', \"YSFSID\" = '50', \"Dwdz\" = '安徽省马鞍山市当涂县振兴路交通局6楼', \"Gszcxx\" = '当涂经济开发区', \"Gysjc\" = '马鞍山源润物流有限公司', \"Sfls\" = '1', \"Sfxn\" = '1', \"Zczbj\" = '1000.0000', \"Status\" = '结束', \"Bz\" = '来源于竞价平台', \"Lxr\" = '曹江水', \"Phone\" = '15056491999,13956497939', \"Email\" = '123940790@qq.com', \"Yzbm\" = '243100', \"Cz\" = '0555-6719190', \"Lxdz\" = '安徽省马鞍山市当涂县振兴路交通局6楼', \"TJDWTBR\" = 'czx', \"TJDW\" = '101', \"Djbh\" = '694871edee304ae1afbf187bd1b5cb97', \"Sqdcname\" = '马鞍山当涂发电有限公司', \"Sqrname\" = '袁荣超', \"Sqdcid\" = '701', \"ORGID\" = '40288d904815e0eb014815f869ef0007', \"GYSZT\" = '1', \"S_TYPE\" = 'default', \"S_CREATOR_ORG\" = '701', \"S_FORM_CODE\" = 'ddzxGYSXX1482470037', \"S_PROC_STATE\" = '100103', \"S_CREATOR_NAME\" = '袁荣超', \"S_CREATOR_ON\" = '马鞍山当涂发电有限公司', \"S_CREATOR_DN\" = '18755547579', \"S_IS_DELETE\" = '0', \"JYFW_TITLE\" = '燃煤', \"GYSFLNAME_TITLE\" = '六类', \"DQID_TITLE\" = '马鞍山市', \"YSFSID_TITLE\" = '水运', \"SFSC_TITLE\" = '是', \"SFLS_TITLE\" = '是', \"SFXN_TITLE\" = '是', \"IS_DEL\" = '0', \"SPLX_TITLE\" = '集团审批', \"GYSZT_TITLE\" = '正常', \"jjptid\" = '694871edee304ae1afbf187bd1b5cb97' WHERE \"Id\" = 'jjpt1694871edee304ae1afbf187bd1b5cb97' AND \"Gysqc\" = '马鞍山源润物流有限公司' AND \"Sh\" = '340521087550717' AND \"Sjgysbm\" = '340521087550717' AND \"Gysbm\" = '340521087550717' AND \"Entityid\" = 'jjpt1694871edee304ae1afbf187bd1b5cb97' AND \"Splx\" = '1' AND \"CYM\" IS NULL AND \"CYSH\" IS NULL AND \"Frdb\" = '曹江水' AND \"JYFW\" IS NULL AND \"Gysbz\" IS NULL AND \"Gysflname\" = '6' AND \"Dqid\" = '00340005' AND \"Dqname\" = '马鞍山市' AND \"Fmisdq\" IS NULL AND \"SJGYSID\" IS NULL AND \"Sjgysname\" IS NULL AND \"Sfsc\" = '1' AND \"Mc\" IS NULL AND \"Ndscnl\" = 0 AND \"Jjkfnx\" = 0 AND \"Gysclsj\" = TIMESTAMP'2018-05-18 14:17:54' AND \"YSFSNAME\" IS NULL AND \"YSFSID\" = '50' AND \"Dwdz\" = '安徽省马鞍山市当涂县振兴路交通局6楼' AND \"Gszcxx\" = '当涂经济开发区' AND \"Gysjc\" = '马鞍山源润物流有限公司' AND \"Khhyzh\" IS NULL AND \"Sfls\" = '1' AND \"Khhezh\" IS NULL AND \"Sfxn\" = '1' AND \"Khhe\" IS NULL AND \"Zczbj\" = '1000.0000' AND \"Khhy\" IS NULL AND \"Status\" = '结束' AND \"Hzyx\" IS NULL AND \"Bz\" = '来源于竞价平台' AND \"Lxr\" = '曹江水' AND \"Phone\" = '15056491999,13956497939' AND \"Email\" = '123940790@qq.com' AND \"Yzbm\" = '243100' AND \"Cz\" = '0555-6719190' AND \"Lxdz\" = '安徽省马鞍山市当涂县振兴路交通局6楼' AND \"TJDWTBR\" = 'czx' AND \"TJDW\" = '101' AND \"TJDWLXFS\" = '17711111111' AND \"Djbh\" = '694871edee304ae1afbf187bd1b5cb97' AND \"Sqdcname\" = '马鞍山当涂发电有限公司' AND \"Sqrname\" = '袁荣超' AND \"Sqdcid\" = '701' AND \"Sqsj\" IS NULL AND \"Sqrid\" IS NULL AND \"Dbgh\" IS NULL AND \"Creator_ID\" IS NULL AND \"Creator_DATE\" IS NULL AND \"Modify_Date\" IS NULL AND \"Modify_ID\" IS NULL AND \"ORGNAME\" IS NULL AND \"ORGID\" = '40288d904815e0eb014815f869ef0007' AND \"GYSZT\" = '1' AND \"GYSFLID\" IS NULL AND \"SCZT\" IS NULL AND \"SPLSID\" IS NULL AND \"FJID\" IS NULL AND \"S_UPD_FORM_VERSION\" IS NULL AND \"S_PARENT_ID\" IS NULL AND \"S_TOP_ID\" IS NULL AND \"S_ENGINE_VERSION\" IS NULL AND \"S_FORM_VERSION\" IS NULL AND \"S_TYPE\" = 'default' AND \"S_PROC_INST_ID\" IS NULL AND \"S_CREATOR\" IS NULL AND \"S_CREATOR_DEPT\" IS NULL AND \"S_CREATOR_ORG\" = '701' AND \"S_CREATE_TIME\" IS NULL AND \"S_FORM_CODE\" = 'ddzxGYSXX1482470037' AND \"S_PROC_STATE\" = '100103' AND \"S_CREATOR_NAME\" = '袁荣超' AND \"S_CREATOR_ON\" = '马鞍山当涂发电有限公司' AND \"S_CREATOR_DN\" = '18755547579' AND \"S_IS_DELET E\" = '0' AND \"S_UPDATE_TIME\" IS NULL AND \"JYFW_TITLE\" = '燃煤' AND \"GYSFLNAME_TITLE\" = '六类' AND \"DQID_TITLE\" = '马鞍山市' AND \"SJGYSID_TITLE\" IS NULL AND \"YSFSID_TITLE\" = '水运' AND \"SFSC_TITLE\" = '是' AND \"SFLS_TITLE\" = '是' AND \"SFXN_TITLE\" = '是' AND \"IS_DEL\" = '0' AND \"CGDWSHCL_TITLE\" IS NULL AND \"CGDWSHCL\" IS NULL AND \"EJGLJGDZHYTGCL_TITLE\" IS NULL AND \"EJGLJGDZHYTGCL\" IS NULL AND \"TZZTSX\" IS NULL AND \"TZZTSX_TITLE\" IS NULL AND \"SPLX_TITLE\" = '集团审批' AND \"GYSZT_TITLE\" = '正常' AND \"JYNL\" IS NULL AND \"jjptid\" = '694871edee304ae1afbf187bd1b5cb97'";

        // 处理查询结果
        JsonObject jsonObject = null;
        if (opr_code == 1) {
            jsonObject = SqlRedoToJsonConverter.parseInsertSqlRedoToJson(sql_redo);

        } else if (opr_code == 2) {
            jsonObject = SqlRedoToJsonConverter.parseDelSqlRedoToJson(sql_redo);

        } else if (opr_code == 3) {
            jsonObject = SqlRedoToJsonConverter.parseUpdateSqlRedoToJson(sql_redo);

        }

        logger.debug("原始信息：sql:{}", sql_redo);
        try {
            OracleWriter.sync2Oracle(jsonObject);
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            System.exit(1);
        }
    }
}
