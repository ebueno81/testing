package org.testing.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import org.testing.service.CloudStorageService;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;

@RestController
@RequestMapping("/api/filesgcp")
public class FileUploadGcpController {

    @Autowired
    private CloudStorageService cloudStorageService;

    @PostMapping("/upload")
    public ResponseEntity<String> uploadFile(@RequestParam("file") MultipartFile file) {
        try {
            String result = cloudStorageService.uploadAndProcessFile(file, file.getOriginalFilename());
            return ResponseEntity.ok(result);
        } catch (IOException | InterruptedException | NoSuchAlgorithmException | KeyManagementException e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error al subir y procesar el archivo: " + e.getMessage());
        }
    }
}