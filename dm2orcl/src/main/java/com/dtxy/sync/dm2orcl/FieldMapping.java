package com.dtxy.sync.dm2orcl;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FieldMapping {
    public static void main(String[] args) {
        String dm_field = "ID, KDQC, SSDQID, SSDQNAME";
        String oracle_field = "ID, otc=fun1, SSDQID, SSDQNAME=fun2";

        Map<String, String> resultMap = getFieldMapping(dm_field, oracle_field);
        System.out.println("Field Mapping:");
        for (Map.Entry<String, String> entry : resultMap.entrySet()) {
            System.out.println(entry.getKey() + " -> " + entry.getValue());
        }
    }

    public static Map<String, String> getFieldMapping(String dm_field, String oracle_field) {
        /*if(containsSelect(dm_field)||containsSelect(oracle_field)){
            return getFieldMappingFromSelect(dm_field,oracle_field);
        }else
            return getFieldMappingFromSimple(dm_field,oracle_field);*/

        return getFieldMappingFromSimple(dm_field,oracle_field);
    }

    //从简单的字段列表获取
    private static Map<String, String> getFieldMappingFromSimple(String dm_field, String oracle_field) {
        String[] keys = dm_field.split(",\\s*");
        String[] values = oracle_field.split(",\\s*");

        if (keys.length != values.length) {
            throw new IllegalArgumentException("dm_field and oracle_field must have the same number");
        }

        Map<String, String> map = new LinkedHashMap<>();
        for (int i = 0; i < keys.length; i++) {
            map.put(keys[i].trim(), values[i].trim());
        }

        return map;
    }

    private static Map<String, String> getFieldMappingFromSelect(String dm_select, String oracle_select) {

        //return createMap(FieldExtractor.extractFieldsFromInsert(insertStatement), FieldExtractor.extractFieldsFromSelect(selectStatement));
        return createMap(FieldExtractor.extractFieldsFromSelect(dm_select), FieldExtractor.extractFieldsFromSelect(oracle_select));
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
    private static boolean containsSelect(String sql) {
        String regex = "(?i)SELECT"; // (?i)表示忽略大小写
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(sql);

        return matcher.find();
    }
}

