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
        String sql_redo="UPDATE \"RL_FUEL_RLDD\".\"RL_DYGL_JHDXQK_DETAIL\" SET \"ORGNAME\" = '你的的测试' WHERE \"ID\" = '53d52cf2-c135-4e5c-9749-eb1bb3efffff' AND \"ORGID\" = '1301' AND \"ORGNAME\" IS NULL AND \"ORGLAYER\" = '0001001900010002' AND \"DW\" = '发耳电厂' AND \"YF\" = '2014-04' AND \"FILEID\" = '31d54933-900c-4cd8-9371-fcd98e3d3a19' AND \"HTBH\" = 'CDT-HT-DTGZ-FEGS-14-0156' AND \"GYS\" = '六盘水鸿盛矿山机电设备有限公司' AND \"YSFS\" = '汽运' AND \"SJDHL\" = 2536.01 AND \"SJLMFRL\" = 4263.92 AND \"SJLMHFF\" = 12.85 AND \"SJLMLF\" = 3.53 AND \"SJLMKJ\" = 469.89 AND \"SJLMYF\" = 0 AND \"SJLMQTF\" = 0 AND \"SJLMDCJ\" = 469.89 AND \"SJLMHSBD\" = 771.4 AND \"SJLMBHSBD\" = 659.32 AND \"JSDBH\" IS NULL AND \"MZLX\" IS NULL AND \"MZ\" IS NULL AND \"MZSYS\" = '掺配煤种' AND \"REMARK\" IS NULL AND \"ISAGREE\" = '同意' AND \"DJFS\" IS NULL AND \"FZGS\" IS NULL AND \"FJ\" IS NULL AND \"DJ\" IS NULL AND \"FZ\" IS NULL AND \"DZ\" IS NULL AND \"FHDW\" IS NULL AND \"SHDW\" IS NULL AND \"VARIETY\" IS NULL AND \"HY\" IS NULL AND \"JHDXQKID\" = '004e77ca-e3f4-4895-ab30-4165620fffff' AND \"GYSID\" IS NULL AND \"FZID\" IS NULL AND \"DZID\" IS NULL AND \"FJID\" IS NULL AND \"DJID\" IS NULL AND \"FHDWID\" IS NULL AND \"SHDWID\" IS NULL AND \"VARIETYID\" IS NULL AND \"YSFSID\" IS NULL AND \"DWID\" IS NULL AND \"MZID\" IS NULL AND \"CREATEUSER\" = '袁七一' AND \"CREATETIME\" = DATE'2015-09-23' AND \"UPDATEUSER\" IS NULL AND \"UPDATETIME\" IS NULL";

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
