package org.testing.service;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.springframework.beans.factory.annotation.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.springframework.stereotype.Service;
import org.testing.integration.ClusterDataIntegrationService;
import org.testing.model.BatchResult;
import org.testing.model.ClusterData;
import org.testing.utils.ExcelReader;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import java.util.concurrent.LinkedBlockingQueue;

@Slf4j
@Service
public class FileUploadService {

    private final ClusterDataIntegrationService clusterDataIntegrationService;
    @Value("${net8.api.batch.size}") // Lee el tamaño del lote desde application.properties
    private int batchSize;

    public FileUploadService(ClusterDataIntegrationService clusterDataIntegrationService) {
        this.clusterDataIntegrationService = clusterDataIntegrationService;
    }

    public void processFile(byte[] fileBytes, String fileName, String contentType, String usuarioCreacion) {
        try {
            LinkedBlockingQueue<ClusterData> rowQueue = new LinkedBlockingQueue<>();
            try (InputStream is = new ByteArrayInputStream(fileBytes);
                 Workbook workbook = WorkbookFactory.create(is)) {
                Sheet sheet = workbook.getSheetAt(0);
                for (Row row : sheet) {
                    if (row.getRowNum() == 0) continue; // Omitir la fila de encabezado
                    ClusterData clusterData = ExcelReader.readRow(row, usuarioCreacion);
                    if (clusterData != null) {
                        rowQueue.put(clusterData);
                    }
                }
            }
            log.info("Total de filas en la cola: {}", rowQueue.size());

            int numThreads = 10;
            ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
            for (int i = 0; i < numThreads; i++) {
                executorService.submit(() -> processRowsFromQueue(rowQueue));
            }

            executorService.shutdown();
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

        } catch (IOException | InterruptedException e) {
            log.error("Error al procesar el archivo", e);
        } catch (Exception e) {
            log.error("Error inesperado al procesar el archivo", e);
            throw new RuntimeException(e);
        }
    }


    private void processRowsFromQueue(LinkedBlockingQueue<ClusterData> rowQueue) {
        try {
            while (true) {
                List<ClusterData> batch = new ArrayList<>();
                for (int i = 0; i < batchSize; i++) {
                    ClusterData clusterData = rowQueue.poll();
                    if (clusterData == null) {
                        break;
                    }
                    batch.add(clusterData);
                }
                if (batch.isEmpty()) {
                    break;
                }
                try {
                    BatchResult result = clusterDataIntegrationService.sendBatchDataToNet8Api(batch);
                    log.info("Lote procesado y enviado: {} registros. Creados: {}, Actualizados: {}", result.getTotal(), result.getCreated(), result.getUpdated());
                } catch (Exception e) {
                    log.error("Error al procesar el lote: {}", batch, e);
                }
            }
        } catch (Exception e) {
            log.error("Error inesperado al procesar lotes", e);
        }
    }

    private List<ClusterData> readTxt(byte[] fileBytes, String usuarioCreacion) throws IOException {
        List<ClusterData> dataList = new ArrayList<>();
        BufferedReader reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(fileBytes)));
        String line;
        while ((line = reader.readLine()) != null) {
            String[] parts = line.split("\t"); // Asumiendo que las columnas están separadas por tabulaciones
            if (parts.length == 2) {
                dataList.add(new ClusterData(parts[0], parts[1], usuarioCreacion));
            }
        }
        return dataList;
    }

    private List<ClusterData> readCsv(byte[] fileBytes, String usuarioCreacion) throws IOException {
        List<ClusterData> dataList = new ArrayList<>();
        Iterable<CSVRecord> records = CSVFormat.DEFAULT.parse(new InputStreamReader(new ByteArrayInputStream(fileBytes)));
        for (CSVRecord record : records) {
            if (record.size() == 2) {
                dataList.add(new ClusterData(record.get(0), record.get(1), usuarioCreacion));
            }
        }
        return dataList;
    }
}