package com.github.karlnicholas.merchloan.servicerequest.configuration;

import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PreDestroy;

@Configuration
@Slf4j
public class ClientSession {
    @Autowired
    private ClientSessionFactory sessionFactory;
    @PreDestroy
    public void preDestroy() {
        log.info("Session PreDestroy");
        sessionFactory.close();
    }
    @Bean
    public org.apache.activemq.artemis.api.core.client.ClientSession getClientSession() throws ActiveMQException {
        return sessionFactory.createSession();
    }
}
