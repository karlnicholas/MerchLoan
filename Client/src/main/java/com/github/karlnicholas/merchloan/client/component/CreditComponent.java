package com.github.karlnicholas.merchloan.client.component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.karlnicholas.merchloan.apimessage.message.CreditRequest;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpEntity;
import org.apache.http.ParseException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;

@Component
@Slf4j
public class CreditComponent {
    private final ObjectMapper objectMapper;
    private final PoolingHttpClientConnectionManager connManager;

    public CreditComponent(ObjectMapper objectMapper, PoolingHttpClientConnectionManager connManager) {
        this.objectMapper = objectMapper;
        this.connManager = connManager;
    }

    private Optional<UUID> creditRequest(UUID loanId, BigDecimal amount, String description) throws JsonProcessingException {
        CloseableHttpClient httpclient = HttpClients.custom().setConnectionManager(connManager).build();
            String strJson = objectMapper.writeValueAsString(new CreditRequest(loanId, amount, description));
            StringEntity strEntity = new StringEntity(strJson, ContentType.APPLICATION_JSON);
            HttpPost httpPost = new HttpPost("http://localhost:8080/api/v1/service/creditRequest");
            httpPost.setHeader("Accept", ContentType.WILDCARD.getMimeType());
//            httpPost.setHeader("Content-type", "application/json");
            httpPost.setEntity(strEntity);

            try (CloseableHttpResponse response = httpclient.execute(httpPost)) {
                HttpEntity entity = response.getEntity();
                return Optional.of(UUID.fromString(EntityUtils.toString(entity)));
            } catch (ParseException | IOException e) {
                log.error("accountRequest", e);
            }
//        } catch (IOException e) {
//            log.error("accountRequest", e);
//        }
        return Optional.empty();
//        HttpHeaders headers = new HttpHeaders();
//        headers.setAccept(Collections.singletonList(MediaType.ALL));
//        headers.setContentType(MediaType.APPLICATION_JSON);
//        HttpEntity<CreditRequest> request = new HttpEntity<>(new CreditRequest(loanId, amount, description), headers);
//        return restTemplate.exchange("http://localhost:8080/api/v1/service/creditRequest", HttpMethod.POST, request, UUID.class);
    }

    public Optional<UUID> makePayment(UUID loanId, BigDecimal amount, String description) {
        // Open Account
        int requestCount = 0;
        boolean loop = true;
        do {
            try {
                Optional<UUID> creditId = creditRequest(loanId, amount, description);
                loop = creditId.isEmpty();
                if (!loop) {
                    return Optional.of(creditId.get());
                }
            } catch (Exception ex) {
                if (requestCount >= 3) {
                    log.warn("MAKE PAYMENT EXCEPTION: {}", ex.getMessage());
                    loop = false;
                }
            }
            requestCount++;
            if( requestCount > 3) {
                loop = false;
            }
        } while (loop);
        return Optional.empty();
    }
}
