package com.dtxy.sync.dm2orcl;

import com.dameng.logmnr.LogmnrDll;
import com.dameng.logmnr.LogmnrRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashSet;

public class Dbmslob {
    private static final Logger logger = LoggerFactory.getLogger(Dbmslob.class);
    private static KafkaProducerService kafkaProducerService = new KafkaProducerService(ConfigUtil.getProperty("kafka.bootstrap.servers"));
    private static HashSet<String> tableSet = new HashSet<>();

    static {
        // 加载文件中的字符串到 HashSet
        try (BufferedReader reader = new BufferedReader(new FileReader(ConfigUtil.getFilePath("monitor.tables.config.path")))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String table=line.toUpperCase().trim();
                tableSet.add(table);
                logger.info("被监控的表：{}", table);
            }

            //加载记录位置
            PositionRecorder.loadLastProcessedPosition();
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("出错了：{}", e.getMessage());
        }

    }


    public static void parseMinerLog(String host, int port, String user, String pwd, String logPath, int maxRec) throws FileNotFoundException, UnsupportedEncodingException {
        LogmnrDll.initLogmnr();
        long connid = LogmnrDll.createConnect(host, port, user, pwd);
        LogmnrDll.addLogFile(connid, logPath, 3);
        LogmnrDll.startLogmnr(connid, -1, null, null);
        LogmnrRecord[] arr = LogmnrDll.getData(connid, maxRec);
        LogmnrDll.endLogmnr(connid, 1);
        LogmnrDll.deinitLogmnr();
        //调试用
        //print2file(arr);


        for (LogmnrRecord rec : arr) {
            if (rec.getScn() <= PositionRecorder.getLastProcessedPosition()) {
                logger.debug("被忽略的scn：{}", rec.getScn());
                continue;
            } else {
                //判断操作类型（1、2、3）并且是否为被同步的表
                if (rec.getOperationCode() == 1) {
                    if (tableSet.contains(rec.getSegOwner() + "." + rec.getTableName())) {
                        kafkaProducerService.sendMessage("logmnr", SqlRedoToJsonConverter.parseInsertSqlRedoToJson(rec.getSqlRedo()));
                        /*System.out.println("=========================================");
                        System.out.println(rec.getScn());
                        System.out.println(rec.getSqlRedo());
                        System.out.println(SqlRedoToJsonConverter.parseInsertSqlRedoToJson(rec.getSqlRedo()));*/

                    }

                } else if (rec.getOperationCode() == 2) {
                    if (tableSet.contains(rec.getSegOwner() + "." + rec.getTableName())) {
                        kafkaProducerService.sendMessage("logmnr", SqlRedoToJsonConverter.parseDelSqlRedoToJson(rec.getSqlRedo()));
                        /*System.out.println("=========================================");
                        System.out.println(rec.getScn());
                        System.out.println(rec.getSqlRedo());
                        System.out.println(SqlRedoToJsonConverter.parseDelSqlRedoToJson(rec.getSqlRedo()));*/

                    }

                } else if (rec.getOperationCode() == 3) {
                    if (tableSet.contains(rec.getSegOwner() + "." + rec.getTableName())) {
                        kafkaProducerService.sendMessage("logmnr", SqlRedoToJsonConverter.parseUpdateSqlRedoToJson(rec.getSqlRedo()));
                        /*System.out.println("=========================================");
                        System.out.println(rec.getScn());
                        System.out.println(rec.getSqlRedo());
                        System.out.println(SqlRedoToJsonConverter.parseUpdateSqlRedoToJson(rec.getSqlRedo()));*/

                    }

                }

                logger.info("已处理scn：{},原始redo_sql：{}", rec.getScn(), rec.getSqlRedo());


                //记录处理位置
                PositionRecorder.recordPosition(rec.getScn());
            }

        }

        try {
            PositionRecorder.savePositionToFile();
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("出错了：{}", e.getMessage());
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

    public static void main(String[] args) {
        try {
            parseMinerLog("192.168.24.4", 5236, "RL_FUEL_RLDD", "RL_FUEL_RLDD", "E:\\project\\myTest\\src\\main\\resources\\ARCHIVE_LOCAL1_20230606174857596249_0.log", 100000);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
