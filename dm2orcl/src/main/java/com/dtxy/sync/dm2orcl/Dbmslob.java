package com.dtxy.sync.dm2orcl;

import com.dameng.logmnr.LogmnrDll;
import com.dameng.logmnr.LogmnrRecord;
import com.google.gson.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
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

            String sql_addLogFile = String.format("DBMS_LOGMNR.ADD_LOGFILE('%s');",archPath);
            //String sql_startLogmnr="DBMS_LOGMNR.START_LOGMNR(OPTIONS=>2128 , STARTTIME=>TO_DATE('2023-07-07 11:20:00','YYYY-MM-DD HH24:MI:SS') , ENDTIME=>TO_DATE('2023-07-07 11:25:00','YYYY-MM-DD HH24:MI:SS'));";
            String sql_startLogmnr = String.format("DBMS_LOGMNR.START_LOGMNR(OPTIONS=>2128 , STARTSCN=>%d);", PositionRecorder.getLastProcessedPosition() + 1);
            String sql_endLogmnr = "dbms_logmnr.end_logmnr()";
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

            //关闭日志分析
            statement = connection.createStatement();
            statement.addBatch(sql_endLogmnr);
            statement.executeBatch();

        } catch (Exception e) {
            e.printStackTrace();
            logger.error("解析配置出错了：{}", e.getMessage());
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

    private static void print2file(LogmnrRecord[] arr) throws FileNotFoundException, UnsupportedEncodingException {
        PrintStream ps = new PrintStream("E:\\project\\dataSync\\result.txt");
        System.setOut(ps);
        System.out.println("日志分析结果打印：");
        for (int i = 0; i < arr.length; i++) {
            System.out.println("-----------------------------" + i + "-----------------------------" + "\n");
            System.out.println("xid:" + arr[i].getXid() + "\n");
            System.out.println("operation:" + arr[i].getOperation() + "\n");
            if (arr[i].getSqlRedo() != null) {
                System.out.println("sqlRedo:" + new String(arr[i].getSqlRedo().getBytes("UTF8"), "UTF8") + "\n");
            } else {
                System.out.println("sqlRedo:" + "" + "\n");
            }

            System.out.println("########################" + i + "########################" + "\n");
            System.out.println("scn:" + arr[i].getScn() + "\n");
            System.out.println("startScn:" + arr[i].getStartScn() + "\n");
            System.out.println("commitScn:" + arr[i].getCommitScn() + "\n");
            System.out.println("timestamp:" + arr[i].getTimestamp() + "\n");
            System.out.println("startTimestamp:" + arr[i].getStartTimestamp() + "\n");
            System.out.println("commitTimestamp:" + arr[i].getCommitTimestamp() + "\n");
            System.out.println("operationCode:" + arr[i].getOperationCode() + "\n");
            System.out.println("rollBack:" + arr[i].getRollBack() + "\n");
            System.out.println("segOwner:" + arr[i].getSegOwner() + "\n");
            System.out.println("tableName:" + arr[i].getTableName() + "\n");

            /*System.out.println("rowId:" + arr[i].getRowId() + "\n");
            System.out.println("rbasqn:" + arr[i].getRbasqn() + "\n");
            System.out.println("rbablk:" + arr[i].getRbablk() + "\n");
            System.out.println("rbabyte:" + arr[i].getRbabyte() + "\n");
            System.out.println("dataObj:" + arr[i].getDataObj() + "\n");
            System.out.println("dataObjv:" + arr[i].getDataObjv() + "\n");
            System.out.println("//dataObjd:" + arr[i].getDataObjd() + "\n");
            System.out.println("rsId:" + arr[i].getRsId() + "\n");
            System.out.println("ssn:" + arr[i].getSsn() + "\n");
            System.out.println("csf:" + arr[i].getCsf() + "\n");
            System.out.println("status:" + arr[i].getStatus() + "\n");*/
            System.out.println("########################" + i + "########################" + "\n");
        }
        System.out.println("结果打印完毕");
        ps.flush();
        ps.close();
    }

}
