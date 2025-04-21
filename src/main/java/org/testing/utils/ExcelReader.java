package org.testing.utils;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.testing.model.ClusterData;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ExcelReader {

    public static List<ClusterData> readExcel(byte[] fileBytes, String fileName, String usuarioCreacion) throws IOException {
        List<ClusterData> dataList = new ArrayList<>();
        Workbook workbook;

        if (fileName.toLowerCase().endsWith(".xlsx")) {
            workbook = new XSSFWorkbook(new ByteArrayInputStream(fileBytes));
        } else if (fileName.toLowerCase().endsWith(".xls")) {
            workbook = new HSSFWorkbook(new ByteArrayInputStream(fileBytes));
        } else {
            throw new IllegalArgumentException("Formato de archivo Excel no soportado");
        }

        Sheet sheet = workbook.getSheetAt(0);
        Iterator<Row> rowIterator = sheet.iterator();
        rowIterator.next();

        while (rowIterator.hasNext()) {
            Row row = rowIterator.next();
            Cell emailCell = row.getCell(0);
            Cell clusterCell = row.getCell(1);

            if (emailCell != null && clusterCell != null) {
                String email = emailCell.getStringCellValue();
                String clusterCampana = clusterCell.getStringCellValue();
                dataList.add(new ClusterData(email, clusterCampana, usuarioCreacion));
            }
        }
        workbook.close();
        return dataList;
    }

    public static ClusterData readRow(Row row, String usuarioCreacion) {
        try {
            Cell emailCell = row.getCell(0);
            Cell clusterCell = row.getCell(1);

            if (emailCell != null && clusterCell != null) {
                String email = emailCell.getStringCellValue();
                String clusterCampana = clusterCell.getStringCellValue();
                return new ClusterData(email, clusterCampana, usuarioCreacion);
            } else {
                return null; // O manejar el caso de celdas nulas de otra manera
            }
        } catch (Exception e) {
            // Manejar excepciones aquí (por ejemplo, loggear el error)
            return null;
        }
    }
}