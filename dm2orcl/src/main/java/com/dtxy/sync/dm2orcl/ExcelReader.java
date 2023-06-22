package com.dtxy.sync.dm2orcl;

import com.google.gson.JsonObject;
import org.apache.poi.ss.usermodel.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ExcelReader {
    private static final Logger logger = LoggerFactory.getLogger(ExcelReader.class);
    private static Map<String, JsonObject> dataMap;

    public static void main(String[] args) {
        /*String filePath = "E:\\project\\dataSync\\test.xlsx";

        getBaseInfoFromExcel(filePath);*/
    }

    public ExcelReader() {
        getBaseInfoFromExcel(ConfigUtil.getFilePath("base.mapper.info.path"));
    }

    public static JsonObject getBaseInfo(String dm_tab) {
        return dataMap.get(dm_tab);
    }

    private static void getBaseInfoFromExcel(String filePath) {
        try (FileInputStream fis = new FileInputStream(new File(filePath));
             Workbook workbook = WorkbookFactory.create(fis)) {

            Sheet sheet = workbook.getSheetAt(0);
            Row headerRow = sheet.getRow(0);


            dataMap = new HashMap<>();

            for (Row row : sheet) {
                if (row.getRowNum() == 0) continue; // Skip header row

                JsonObject rowData = new JsonObject();

                Cell keyCell = row.getCell(0); // First column as key
                String keyValue = keyCell.getStringCellValue();

                for (Cell cell : row) {
                    int columnIndex = cell.getColumnIndex();
                    String columnName = headerRow.getCell(columnIndex).getStringCellValue();

                    switch (cell.getCellType()) {
                        case STRING:
                            rowData.addProperty(columnName, cell.getStringCellValue());
                            break;
                        case NUMERIC:
                            rowData.addProperty(columnName, cell.getNumericCellValue());
                            break;
                        // Handle other cell types if needed
                    }
                }

                dataMap.put(keyValue, rowData);
            }

            logger.debug("加载excel获取映射基础信息：{}", dataMap);
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("出错了：{}", e.getMessage());
        }

    }
}
