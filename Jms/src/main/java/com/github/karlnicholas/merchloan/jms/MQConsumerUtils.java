package com.github.karlnicholas.merchloan.jms;


import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

@Configuration
@ConfigurationProperties(prefix = "rabbitmq")
@PropertySource(value = "classpath:rabbitmq-config.properties")
@Data
public class MQConsumerUtils {

//    public void bindConsumer(Session session, Queue queue, MessageListener messageListener) throws JMSException {
//        MessageConsumer consumer = session.createConsumer(queue);
//        consumer.setMessageListener(messageListener);
////        Channel channel = connection.createChannel();
////        channel.exchangeDeclare(exchange, BuiltinExchangeType.DIRECT, false, true, null);
////        channel.queueDeclare(queueName, false, exclusive, true, null);
////        channel.queueBind(queueName, exchange, queueName);
////        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {});
//    }

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
