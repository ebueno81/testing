package org.testing.service;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import org.testing.integration.ClusterDataIntegrationService;
import org.testing.model.ClusterData;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Service
public class CloudStorageService {

    @Autowired
    private Storage storage;

    @Value("${gcp.bucket.name}")
    private String bucketName;

    @Autowired
    private ClusterDataIntegrationService clusterDataIntegrationService;

    // upload y processamos el archivo
    public String uploadAndProcessFile(MultipartFile file, String fileName) throws IOException, InterruptedException, NoSuchAlgorithmException, KeyManagementException {
        // Subir el archivo a GCS
        BlobId blobId = BlobId.of(bucketName, fileName);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType(file.getContentType()).build();
        storage.create(blobInfo, file.getInputStream());

        // Read and process file as exceñ desde GCS in second plane
        CompletableFuture.runAsync(() -> {
            try {
                processExcelFileFromGCS(fileName);
            } catch (IOException | InterruptedException | NoSuchAlgorithmException | KeyManagementException e) {
                System.err.println("Error processing file: " + fileName + ", error: " + e.getMessage());
            }
        });

        return "Archivo subido y el procesamiento se ha iniciado en segundo plano: " + fileName;
    }

    // Proceso de Excel archivo desde GCP
    private void processExcelFileFromGCS(String objectName) throws IOException, InterruptedException, NoSuchAlgorithmException, KeyManagementException {
        Blob blob = storage.get(BlobId.of(bucketName, objectName));
        try (InputStream inputStream = new ByteArrayInputStream(blob.getContent());
             XSSFWorkbook workbook = new XSSFWorkbook(inputStream)) {

            XSSFSheet sheet = workbook.getSheetAt(0);
            List<ClusterData> block = new ArrayList<>();
            int count = 0;

            for (int rowIndex = 1; rowIndex <= sheet.getLastRowNum(); rowIndex++) {
                XSSFRow row = sheet.getRow(rowIndex);
                if (row != null) {
                    ClusterData clusterData = createClusterDataFromRow(row);
                    block.add(clusterData);
                    count++;
                    if (count == 5000) {
                        clusterDataIntegrationService.sendBatchDataToNet8Api(block);
                        block.clear();
                        count = 0;
                    }
                }
            }
            if (!block.isEmpty()) {
                clusterDataIntegrationService.sendBatchDataToNet8Api(block);
            }
        }
    }

    // Crear Cluster Data desde Fila
    private ClusterData createClusterDataFromRow(XSSFRow row) {
        ClusterData clusterData = new ClusterData();
        clusterData.setEmail(row.getCell(0).getStringCellValue());
        clusterData.setClusterCampana(row.getCell(1).getStringCellValue());
        clusterData.setUsuarioCreacion("2023"); // Valor fijo "2022"
        return clusterData;
    }

    // se debe aplicar el scheudeler
    // Método que se ejecutará automáticamente cada minuto
    @Scheduled(fixedRate = 30000) // Ejecuta cada 60000 ms (1 minuto)
    public void logExecution() {
        System.out.println("Ejecutando tarea programada cada 30 segundos: " + System.currentTimeMillis());
    }


}