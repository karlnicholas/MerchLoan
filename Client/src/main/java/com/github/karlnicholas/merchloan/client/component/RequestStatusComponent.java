package com.github.karlnicholas.merchloan.client.component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.github.karlnicholas.merchloan.dto.RequestStatusDto;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.ParseException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Optional;
import java.util.UUID;

@Component
@Slf4j
public class RequestStatusComponent {
    private final ObjectMapper objectMapper;
    private final CloseableHttpClient httpclient;

    public RequestStatusComponent(PoolingHttpClientConnectionManager connManager) {
        this.objectMapper = new ObjectMapper().findAndRegisterModules()
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        httpclient = HttpClients.custom().setConnectionManager(connManager).build();
    }

    private Optional<RequestStatusDto> requestStatus(UUID id) {
        HttpGet httpGet = new HttpGet("http://localhost:8090/api/query/request/" + id.toString());
        httpGet.setHeader("Accept", ContentType.WILDCARD.getMimeType());
        try (CloseableHttpResponse response = httpclient.execute(httpGet)) {
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                HttpEntity entity = response.getEntity();
                return Optional.of(objectMapper.readValue(EntityUtils.toString(entity), RequestStatusDto.class));
            }
        } catch (ParseException | IOException e) {
            log.error("accountRequest", e);
        }
        return Optional.empty();
    }

    public Optional<UUID> checkRequestStatus(UUID id) {
        // Check request status
        int requestCount = 0;
        boolean loop = true;
        int waitTime = 300;
        do {
            try {
                Optional<RequestStatusDto> requestStatusDto = requestStatus(id);
                loop = requestStatusDto.isEmpty();
                if (!loop) {
                    RequestStatusDto statusDto = requestStatusDto.get();
                    if (statusDto != null && statusDto.getStatus().compareToIgnoreCase("SUCCESS") == 0) {
                        return Optional.of(id);
                    } else {
                        // try again
                        loop = true;
                    }
                }
            } catch (Exception ex) {
                if (requestCount >= 3) {
                    log.warn("Request Status exception: {}", ex.getMessage());
                    loop = false;
                }
            }
            requestCount++;
            if (requestCount > 3) {
                loop = false;
            }
            if (loop) {
                sleep(waitTime);
                waitTime *= 3;
            }
        } while (loop);
        return Optional.empty();
    }

    private void sleep(int waitTime) {
        try {
            Thread.sleep(waitTime);
        } catch (InterruptedException ex) {
            log.error("Sleep while check status interrupted", ex);
            Thread.currentThread().interrupt();
        }
    }
}
