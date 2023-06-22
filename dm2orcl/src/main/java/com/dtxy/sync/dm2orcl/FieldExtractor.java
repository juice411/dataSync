package com.dtxy.sync.dm2orcl;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class FieldExtractor {
    private static final Logger logger = LoggerFactory.getLogger(FieldExtractor.class);

    public static void main(String[] args) {
        /*String selectStatement = "select ID,KDQC,SSDQID,SSDQNAME,YSFS YSFSID,'' YSFSNAME,SJKDID,SJKDNAME,case when IFLS='1' then 1 else 0 end IFLS," +
                "sqrid LXR,case when ifxn='1' then 1 else 0 end IFXN,case when IFDK='1' then 1 else 0 end IFDK,case when IFZD='1' then 1 else 0 end IFZD," +
                "case when IFTP='1' then 1 else 0 end IFTP,BZ,SQYY,DZ,JD,WD,'' MKDQID,MKDQQC MKDQNAME,0 SCZT,CREATEUSER TBR,'' TBDW," +
                "sysdate LRSJ,CREATEUSER Creator_ID,'2023-03-25 00:00:00' Creator_DATE,'2023-03-25 00:00:00' Modify_Date,'' Modify_ID," +
                "STATUS,'' ORGNAME,ORGID,'' SPLSID,SSDQID SSDQID_TITLE,SJKDID SJKDID_TITLE,MKDQQC MKDQID_TITLE,'' S_UPD_FORM_VERSION,'' S_PARENT_ID," +
                "'' S_TOP_ID,'' S_ENGINE_VERSION,'' S_FORM_VERSION,'' S_TYPE,'' S_PROC_INST_ID,CREATEUSER S_CREATOR,'' S_CREATOR_DEPT," +
                "orgid S_CREATOR_ORG,'2023-03-25 00:00:00' S_CREATE_TIME,'ddzxMKXXGL8784550102' S_FORM_CODE,'100103' S_PROC_STATE," +
                "sqrname S_CREATOR_NAME,sqrname S_CREATOR_ON,sqrid S_CREATOR_DN,0 S_IS_DELETE,'2023-03-25 00:00:00' S_UPDATE_TIME,ORGID ORGID_TITLE," +
                "case when ysfs = '10' then '火运' when ysfs = '30' then '汽运' when ysfs = '50' then '水运' when ysfs = '10,30' then '火运,汽运' " +
                "when ysfs = '10,30，50' then '火运,汽运，水运' when ysfs='10,20,30,50,40' then '火运,水陆联运,汽运,皮带,水运' " +
                "when ysfs='10,20,30,50,40,60' then '火运,水陆联运,汽运,皮带,水运,管道' when ysfs='10,20,30,50,40,60,90' then '火运,水陆联运,汽运,皮带,水运,管道,其它' " +
                "when ysfs='10,20,30,50,40,90' then '火运,水陆联运,汽运,皮带,水运,其它' when ysfs='10,20,30,50,60' then '火运,水陆联运,汽运,管道,水运' " +
                "when ysfs='10,20,90' then '火运,水陆联运,其它' when ysfs='10,20,50' then '火运,水陆联运,水运' end YSFSID_TITLE," +
                "case when IFLS='1' then '是' else '否' end IFLS_TITLE,case when IFXN='1' then '是' else '否' end IFXN_TITLE," +
                "case when IFDK='1' then '是' else '否' end IFDK_TITLE,case when IFZD='1' then '是' else '否' end IFZD_TITLE," +
                "case when IFTP='1' then '是' else '否' end IFTP_TITLE,case when IFQY='1' then '是' else '否' end IFQY_TITLE," +
                "case when IFQY='1' then 1 else 0 end IFQY,KDBM,SSDQNAME SSDQNAME_TITLE,SJKDNAME SJKDNAME_TITLE,'' LXFS,KDJC " +
                "from rldd.RL_GYSGL_KDGL where id not in (select id from rl_fuel_rldd.RL_GYSGL_KDGL)";

        List<String> fields = extractFieldsFromSelect(selectStatement);*/

        String insertStatement = "INSERT INTO RL_FUEL_RLDD.RL_GYSGL_KDGL (ID, KDQC, SSDQID, SSDQNAME, YSFSID, YSFSNAME, SJKDID, SJKDNAME, IFLS, LXR, IFXN, IFDK, IFZD, IFTP, BZ, SQYY, DZ, JD, WD, MKDQID, MKDQNAME, SCZT, TBR, TBDW, LRSJ, Creator_ID, Creator_DATE, Modify_Date, Modify_ID, STATUS, ORGNAME, ORGID, SPLSID, SSDQID_TITLE, SJKDID_TITLE, MKDQID_TITLE, S_UPD_FORM_VERSION, S_PARENT_ID, S_TOP_ID, S_ENGINE_VERSION, S_FORM_VERSION, S_TYPE, S_PROC_INST_ID, S_CREATOR, S_CREATOR_DEPT, S_CREATOR_ORG, S_CREATE_TIME, S_FORM_CODE, S_PROC_STATE, S_CREATOR_NAME, S_CREATOR_ON, S_CREATOR_DN, S_IS_DELETE, S_UPDATE_TIME, ORGID_TITLE, YSFSID_TITLE, IFLS_TITLE, IFXN_TITLE, IFDK_TITLE, IFZD_TITLE, IFTP_TITLE, IFQY_TITLE, IFQY, KDBM, SSDQNAME_TITLE, SJKDNAME_TITLE, LXFS, KDJC) VALUES (1, 'KDQC1', 1, 'SSDQNAME1', 2, 'YSFSNAME1', 3, 'SJKDNAME1', 'IFLS1', 'LXR1', 'IFXN1', 'IFDK1', 'IFZD1', 'IFTP1', 'BZ1', 'SQYY1', 'DZ1', 'JD1', 'WD1', 'MKDQID1', 'MKDQNAME1', 'SCZT1', 'TBR1', 'TBDW1', 'LRSJ1', 'Creator_ID1', 'Creator_DATE1', 'Modify_Date1', 'Modify_ID1', 'STATUS1', 'ORGNAME1', 'ORGID1', 'SPLSID1', 'SSDQID_TITLE1', 'SJKDID_TITLE1', 'MKDQID_TITLE1', 'S_UPD_FORM_VERSION1', 'S_PARENT_ID1', 'S_TOP_ID1', 'S_ENGINE_VERSION1', 'S_FORM_VERSION1', 'S_TYPE1', 'S_PROC_INST_ID1', 'S_CREATOR1', 'S_CREATOR_DEPT1', 'S_CREATOR_ORG1', 'S_CREATE_TIME1', 'S_FORM_CODE1', 'S_PROC_STATE1', 'S_CREATOR_NAME1', 'S_CREATOR_ON1', 'S_CREATOR_DN1', 'S_IS_DELETE1', 'S_UPDATE_TIME1', 'ORGID_TITLE1', 'YSFSID_TITLE1', 'IFLS_TITLE1', 'IFXN_TITLE1', 'IFDK_TITLE1', 'IFZD_TITLE1', 'IFTP_TITLE1', 'IFQY_TITLE1', 1, 'KDBM1', 'SSDQNAME_TITLE1', 'SJKDNAME_TITLE1', 'LXFS1', 'KDJC1')";
        List<String> fields = extractFieldsFromInsert(insertStatement);

        System.out.println("Fields:");
        for (String field : fields) {
            System.out.println(field);
        }
    }

    public static List<String> extractFieldsFromSelect(String selectStatement) {
        List<String> fields = new ArrayList<>();

        try {
            Statement statement = CCJSqlParserUtil.parse(selectStatement);
            if (statement instanceof Select) {
                Select select = (Select) statement;
                PlainSelect plainSelect = (PlainSelect) select.getSelectBody();

                List<SelectItem> selectItems = plainSelect.getSelectItems();
                for (SelectItem selectItem : selectItems) {
                    if (selectItem instanceof SelectExpressionItem) {
                        SelectExpressionItem expressionItem = (SelectExpressionItem) selectItem;
                        Expression expression = expressionItem.getExpression();
                        String fieldName = null;

                        if (expression instanceof CaseExpression) {
                            CaseExpression caseExpression = (CaseExpression) expression;
                            List<WhenClause> whenClauses = caseExpression.getWhenClauses();
                            if (whenClauses.size() > 0) {
                                WhenClause whenClause = whenClauses.get(0);
                                Expression whenExpression = whenClause.getWhenExpression();
                                if (whenExpression instanceof BinaryExpression) {
                                    BinaryExpression binaryExpression = (BinaryExpression) whenExpression;
                                    Expression leftExpression = binaryExpression.getLeftExpression();
                                    if (leftExpression instanceof Column) {
                                        Column column = (Column) leftExpression;
                                        fieldName = column.getColumnName();
                                    }
                                }
                            }
                        } else {
                            if (expression instanceof Column) {
                                Column column = (Column) expression;
                                fieldName = column.getColumnName();
                            } else if (expression instanceof Function) {
                                Function function = (Function) expression;
                                fieldName = function.getName();
                            } else if (expression instanceof StringValue) {
                                fieldName = null;
                            } else {
                                fieldName = expression.toString();
                            }
                        }

                        fields.add(fieldName);
                    }
                }
            }
        } catch (JSQLParserException e) {
            e.printStackTrace();
            logger.error("出错了：{}", e.getMessage());
        }

        return fields;
    }

    public static List<String> extractFieldsFromInsert(String insertStatement) {
        List<String> fields = new ArrayList<>();

        try {
            Statement statement = CCJSqlParserUtil.parse(insertStatement);
            if (statement instanceof Insert) {
                Insert insert = (Insert) statement;

                String tableName = insert.getTable().getName();
                System.out.println("Table Name: " + tableName);

                List<Column> columns = insert.getColumns();
                for (Column column : columns) {
                    String columnName = column.getColumnName();
                    fields.add(columnName);
                }
            }
        } catch (JSQLParserException e) {
            e.printStackTrace();
            logger.error("出错了：{}", e.getMessage());
        }

        return fields;
    }
}

