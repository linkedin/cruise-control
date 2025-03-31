package com.linkedin.kafka.cruisecontrol.analyzer.netflix.kaas;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.metatron.ipc.security.MetatronSslContext;
import org.apache.commons.io.IOUtils;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import java.io.InputStream;
import java.net.URL;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

public class KaasControllerClient {
    private static final String BASE_URL = "https://kaascontroller.cluster.us-east-1.prod.cloud.netflix.net:8443/REST/v1/";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final HttpClient httpClient;
    private static final String METATRON_APP = "kaascontroller";

    public KaasControllerClient() {
        SSLContext sslContext = MetatronSslContext.forClient(METATRON_APP);
        httpClient = HttpClient.newBuilder()
                .sslContext(sslContext)
                .connectTimeout(Duration.ofSeconds(10))
                .build();
    }

    public int getClusterId() {
        return 1;
    }

    /**
     * Returns topics for given kaas cluster.
     * This is useful to get more info about topics like numPartitions, numReplicas, etc.
     * @return List of TopicInfo
     */
    public List<TopicInfo> getTopics(int clusterId) throws Exception {
        final String url = BASE_URL + String.format("clusters/%d/topics", clusterId);
        final String response = sendRequest(url);
        return OBJECT_MAPPER.readValue(response, new TypeReference<List<TopicInfo>>() {
        });
    }

    /**
     * Returns broker info for given kaas cluster in verbose mode
     * @return List of BrokerInfo
     */
    public List<BrokerInfo> getBrokers(int clusterId) throws Exception {
        final String url = BASE_URL + String.format("clusters/%d/brokers?verbose=true", clusterId);
        final String response = sendRequest(url);
        return OBJECT_MAPPER.readValue(response, new TypeReference<List<BrokerInfo>>() {
        });
    }

    private String sendRequest(String url) throws Exception {
        HttpsURLConnection connection = (HttpsURLConnection) new URL(url).openConnection();
        connection.setSSLSocketFactory(MetatronSslContext.forClient(METATRON_APP).getSocketFactory());
        connection.setHostnameVerifier((hostname, session) -> true);
        try (InputStream is = connection.getInputStream()) {
            return IOUtils.toString(is, StandardCharsets.UTF_8);
        }
    }
}
