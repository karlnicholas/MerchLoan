package com.github.karlnicholas.merchloan.jms.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.messaginghub.pooled.jms.JmsPoolConnectionFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.jms.ConnectionFactory;


@Configuration
@Slf4j
public class MQConnectionConfig {
    @Value("${rabbitmq.host:localhost}")
    private String host;
    @Value("${rabbitmq.port:61616}")
    private Integer port;
    @Value("${rabbitmq.virtual-host:/}")
    private String virtualHost;

    @Bean
    public ConnectionFactory getConnectionFactory() {
//        JmsPoolConnectionFactory poolingFactory = new JmsPoolConnectionFactory();
//        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://" + host + ":" + port);
//        poolingFactory.setConnectionFactory(connectionFactory);
//        return poolingFactory;
        return new ActiveMQConnectionFactory("tcp://" + host + ":" + port);
    }

//    @Bean
//    public Connection getConnection() throws InterruptedException, JMSException {
////        JmsPoolConnectionFactory poolingFactory = new JmsPoolConnectionFactory();
////        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://" + host + ":" + port);
////        poolingFactory.setConnectionFactory(connectionFactory);
////        return poolingFactory;
//        return getConnectionFactory().createConnection();
//    }

//    @Bean
//    public Connection getConnection(ConnectionFactory connectionFactory) throws InterruptedException, JMSException {
//        int retryCount = 0;
//        while (retryCount < 3) {
//            try {
//                return connectionFactory.createConnection();
//            } catch (JMSException e) {
//                Thread.sleep(3000);
//                // apply retry logic
//                retryCount++;
//                if (retryCount >= 3) {
//                    throw e;
//                }
//                throw new RuntimeException(e);
//            }
//        }
//        return null;
//    }

}