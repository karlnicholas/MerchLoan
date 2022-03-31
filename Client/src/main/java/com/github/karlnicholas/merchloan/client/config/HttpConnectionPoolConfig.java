package com.github.karlnicholas.merchloan.client.config;

import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class HttpConnectionPoolConfig {
    @Bean
    public PoolingHttpClientConnectionManager getPoolingHttpClientConnectionManager() {
        PoolingHttpClientConnectionManager connManager = new PoolingHttpClientConnectionManager();
        connManager.setMaxTotal(100);
        connManager.setDefaultMaxPerRoute(100);
        return connManager;
//        CloseableHttpClient client
//                = HttpClients.custom().setConnectionManager(poolingConnManager)
//                .build();
//        client.execute(new HttpGet("/"));
//        assertTrue(poolingConnManager.getTotalStats().getLeased() == 1);

    }
}
