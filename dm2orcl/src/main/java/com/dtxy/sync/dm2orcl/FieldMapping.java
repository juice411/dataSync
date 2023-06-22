package com.dtxy.sync.dm2orcl;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class FieldMapping {
    public static void main(String[] args) {
        String insertStatement = "INSERT INTO RL_FUEL_RLDD.RL_GYSGL_KDGL (ID, KDQC, SSDQID, SSDQNAME, YSFSID, YSFSNAME, SJKDID, SJKDNAME, IFLS, LXR, IFXN, IFDK, IFZD, IFTP, BZ, SQYY, DZ, JD, WD, MKDQID, MKDQNAME, SCZT, TBR, TBDW, LRSJ, Creator_ID, Creator_DATE, Modify_Date, Modify_ID, STATUS, ORGNAME, ORGID, SPLSID, SSDQID_TITLE, SJKDID_TITLE, MKDQID_TITLE, S_UPD_FORM_VERSION, S_PARENT_ID, S_TOP_ID, S_ENGINE_VERSION, S_FORM_VERSION, S_TYPE, S_PROC_INST_ID, S_CREATOR, S_CREATOR_DEPT, S_CREATOR_ORG, S_CREATE_TIME, S_FORM_CODE, S_PROC_STATE, S_CREATOR_NAME, S_CREATOR_ON, S_CREATOR_DN, S_IS_DELETE, S_UPDATE_TIME, ORGID_TITLE, YSFSID_TITLE, IFLS_TITLE, IFXN_TITLE, IFDK_TITLE, IFZD_TITLE, IFTP_TITLE, IFQY_TITLE, IFQY, KDBM, SSDQNAME_TITLE, SJKDNAME_TITLE, LXFS, KDJC) VALUES (1, 'KDQC1', 1, 'SSDQNAME1', 2, 'YSFSNAME1', 3, 'SJKDNAME1', 'IFLS1', 'LXR1', 'IFXN1', 'IFDK1', 'IFZD1', 'IFTP1', 'BZ1', 'SQYY1', 'DZ1', 'JD1', 'WD1', 'MKDQID1', 'MKDQNAME1', 'SCZT1', 'TBR1', 'TBDW1', 'LRSJ1', 'Creator_ID1', 'Creator_DATE1', 'Modify_Date1', 'Modify_ID1', 'STATUS1', 'ORGNAME1', 'ORGID1', 'SPLSID1', 'SSDQID_TITLE1', 'SJKDID_TITLE1', 'MKDQID_TITLE1', 'S_UPD_FORM_VERSION1', 'S_PARENT_ID1', 'S_TOP_ID1', 'S_ENGINE_VERSION1', 'S_FORM_VERSION1', 'S_TYPE1', 'S_PROC_INST_ID1', 'S_CREATOR1', 'S_CREATOR_DEPT1', 'S_CREATOR_ORG1', 'S_CREATE_TIME1', 'S_FORM_CODE1', 'S_PROC_STATE1', 'S_CREATOR_NAME1', 'S_CREATOR_ON1', 'S_CREATOR_DN1', 'S_IS_DELETE1', 'S_UPDATE_TIME1', 'ORGID_TITLE1', 'YSFSID_TITLE1', 'IFLS_TITLE1', 'IFXN_TITLE1', 'IFDK_TITLE1', 'IFZD_TITLE1', 'IFTP_TITLE1', 'IFQY_TITLE1', 1, 'KDBM1', 'SSDQNAME_TITLE1', 'SJKDNAME_TITLE1', 'LXFS1', 'KDJC1')";
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
                "from rldd.RL_GYSGL_KDGL where id not in (select id from rl_fuel_rldd.RL_GYSGL_KDGL)";*/
        String selectStatement = "\n" +
                "select  \n" +
                "   ID,\n" +
                "   ZDXXWHID,\n" +
                "   ZDXXWHNAME,\n" +
                "   S_CREATOR_ORG,\n" +
                "   type from  rl_fuel_rldd.vw_gysgl_powerzd;";
        String selectStatement2 = "\n" +
                "  select   zd.ID,\n" +
                "                  zd.zdid,\n" +
                "                  zd.ZDNAME,\n" +
                "                  zd.kdid,\n" +
                "                  TYPE \n" +
                "    from rldd.RL_GYSGL_KDZD zd";

        Map<String, String> fieldMapping = getFieldMapping(selectStatement, selectStatement2);
        System.out.println("Field Mapping:");
        for (Map.Entry<String, String> entry : fieldMapping.entrySet()) {
            System.out.println(entry.getKey() + " -> " + entry.getValue());
        }
    }

    public static Map<String, String> getFieldMapping(String insertStatement, String selectStatement) {

        //return createMap(FieldExtractor.extractFieldsFromInsert(insertStatement), FieldExtractor.extractFieldsFromSelect(selectStatement));
        return createMap(FieldExtractor.extractFieldsFromSelect(insertStatement), FieldExtractor.extractFieldsFromSelect(selectStatement));
    }

    private static <K, V> Map<K, V> createMap(List<K> keys, List<V> values) {
        if (keys.size() != values.size()) {
            throw new IllegalArgumentException("Lists must have the same size");
        }

        Map<K, V> map = new LinkedHashMap<>();
        for (int i = 0; i < keys.size(); i++) {
            K key = keys.get(i);
            V value = values.get(i);
            map.put(key, value);
        }

        return map;
    }
}

