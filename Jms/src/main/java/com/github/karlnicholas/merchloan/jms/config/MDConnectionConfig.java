package com.github.karlnicholas.merchloan.jms.config;

import jakarta.annotation.Resource;
import jakarta.enterprise.context.ApplicationScoped;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ServerLocator;

@ApplicationScoped
@Slf4j
public class MDConnectionConfig {
//    @Value("${rabbitmq.username:guest}")
//    private String username;
//    @Value("${rabbitmq.password:guest}")
//    private String password;
//    @Value("${rabbitmq.host:localhost}")
//    private String host;
//    @Value("${rabbitmq.port:61616}")
//    private Integer port;
//    @Value("${rabbitmq.virtual-host:/}")
//    private String virtualHost;

    @Resource
    public ServerLocator getServerLocator() throws Exception {
        ServerLocator locator = ActiveMQClient.createServerLocator("tcp://localhost:61616");
//        locator.setUseGlobalPools(false);
//        locator.setThreadPoolMaxSize(100);
        return locator;
    }
}