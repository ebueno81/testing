package org.testing.integration;

import com.google.gson.Gson;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.testing.model.BatchResult;
import org.testing.model.ClusterData;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.List;

@Service
public class ClusterDataIntegrationService {

    @Value("${net8.api.url}")
    private String net8ApiUrl;

    public BatchResult sendBatchDataToNet8Api(List<ClusterData> dataList) throws IOException, InterruptedException, NoSuchAlgorithmException, KeyManagementException {
        HttpClient httpClient = createHttpClientWithInsecureSSL();
        Gson gson = new Gson();
        String requestBody = gson.toJson(dataList);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(net8ApiUrl + "/PostClusterDataBatchOptimized"))
                .header("Content-Type", MediaType.APPLICATION_JSON_VALUE)
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() != 200) {
            throw new IOException("Error al enviar lote a la API .NET Core: " + response.statusCode());
        }

        return gson.fromJson(response.body(), BatchResult.class); // Deserializa la respuesta a BatchResult
    }


    private HttpClient createHttpClientWithInsecureSSL() throws NoSuchAlgorithmException, KeyManagementException {
        TrustManager[] trustAllCerts = new TrustManager[]{
                new X509TrustManager() {
                    public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                        return null;
                    }

                    public void checkClientTrusted(X509Certificate[] certs, String authType) {
                    }

                    public void checkServerTrusted(X509Certificate[] certs, String authType) {
                    }
                }
        };

        SSLContext sslContext = SSLContext.getInstance("SSL");
        sslContext.init(null, trustAllCerts, new java.security.SecureRandom());

        return HttpClient.newBuilder()
                .sslContext(sslContext)
                .sslParameters(sslContext.getDefaultSSLParameters())
                .build();
    }
}