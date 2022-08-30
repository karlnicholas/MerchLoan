package com.github.karlnicholas.merchloan.jms;


import com.rabbitmq.client.*;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import java.io.IOException;

@Configuration
@ConfigurationProperties(prefix = "rabbitmq")
@PropertySource(value = "classpath:rabbitmq-config.properties")
@Data
public class MQConsumerUtils {

    public void bindConsumer(Channel channel, String exchange, String queueName, boolean exclusive, DeliverCallback deliverCallback) throws IOException {
        channel.exchangeDeclare(exchange, BuiltinExchangeType.DIRECT, false, true, null);
        channel.queueDeclare(queueName, false, exclusive, true, null);
        channel.queueBind(queueName, exchange, queueName);
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {});
    }

    private String exchange;

    private String accountCreateAccountQueue;
    private String accountFundingQueue;
    private String accountValidateCreditQueue;
    private String accountValidateDebitQueue;
    private String accountCloseLoanQueue;
    private String accountLoanClosedQueue;
    private String accountQueryLoansToCycleQueue;
    private String accountQueryAccountIdQueue;
    private String accountQueryLoanIdQueue;
    private String accountBillingCycleChargeQueue;
    private String accountLoanIdComputeQueue;
    private String accountStatementStatementHeaderQueue;
    private String accountFeeChargeQueue;
    private String accountInterestChargeQueue;

    private String queryResponseQueue;

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
    private String statementLoanIdQueue;
    private String statementContinueQueue;
    private String statementContinue2Queue;
    private String statementContinue3Queue;

}
