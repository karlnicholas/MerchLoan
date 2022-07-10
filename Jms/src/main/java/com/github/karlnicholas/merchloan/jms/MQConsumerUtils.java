package com.github.karlnicholas.merchloan.jms;


import lombok.Data;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.MessageHandler;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import java.io.IOException;

@Configuration
@ConfigurationProperties(prefix = "rabbitmq")
@PropertySource(value = "classpath:rabbitmq-config.properties")
@Data
public class MQConsumerUtils {

    public ClientConsumer bindConsumer(ClientSession clientSession, SimpleString queueName, boolean temporary, MessageHandler messageHandler) throws ActiveMQException {
        ClientSession.QueueQuery query = clientSession.queueQuery(queueName);
        if (!query.isExists()) {
            QueueConfiguration queueConfiguration = new QueueConfiguration(queueName);
            queueConfiguration.setDurable(false);
            queueConfiguration.setAutoDelete(true);
            queueConfiguration.setTemporary(temporary);
            queueConfiguration.setRoutingType(RoutingType.ANYCAST);
            clientSession.createQueue(queueConfiguration);
        }
        ClientConsumer clientConsumer = clientSession.createConsumer(queueName);
        clientConsumer.setMessageHandler(messageHandler);
        return clientConsumer;
//        Channel channel = connection.createChannel();
//        channel.exchangeDeclare(exchange, BuiltinExchangeType.DIRECT, false, true, null);
//        channel.queueDeclare(queueName, false, exclusive, true, null);
//        channel.queueBind(queueName, exchange, queueName);
//        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {});
    }

    private String exchange;

    private String accountCreateAccountQueue;
    private String accountFundingQueue;
    private String accountValidateCreditQueue;
    private String accountValidateDebitQueue;
    private String accountCloseLoanQueue;
    private String accountLoanClosedQueue;
    private String accountQueryStatementHeaderQueue;
    private String accountQueryLoansToCycleQueue;
    private String accountQueryAccountIdQueue;
    private String accountQueryLoanIdQueue;
    private String accountBillingCycleChargeQueue;

    private String servicerequestQueue;
    private String servicerequestQueryIdQueue;
    private String serviceRequestCheckRequestQueue;
    private String serviceRequestBillLoanQueue;
    private String serviceRequestStatementCompleteQueue;

    private String statementStatementQueue;
    private String statementCloseStatementQueue;
    private String statementQueryStatementQueue;
    private String statementQueryStatementsQueue;
    private String statementQueryMostRecentStatementQueue;
}
