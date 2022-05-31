package com.github.karlnicholas.merchloan.statement.message;

import com.github.karlnicholas.merchloan.jms.MQConsumerUtils;
import com.github.karlnicholas.merchloan.jms.ReplyWaitingHandler;
import com.github.karlnicholas.merchloan.jmsmessage.BillingCycleCharge;
import com.github.karlnicholas.merchloan.jmsmessage.ServiceRequestResponse;
import com.github.karlnicholas.merchloan.jmsmessage.StatementCompleteResponse;
import com.github.karlnicholas.merchloan.jmsmessage.StatementHeader;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.artemis.jms.client.ActiveMQQueue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import jakarta.jms.*;

import java.util.UUID;

@Component
@Slf4j
public class MQProducers {
    private final ConnectionFactory connectionFactory;
    private final ReplyWaitingHandler replyWaitingHandler;
    private final Queue statementReplyQueue;
    private final Queue accountBillingCycleChargeQueue;
    private final Queue accountQueryStatementHeaderQueue;
    private final Queue servicerequestQueue;
    private final Queue accountLoanClosedQueue;
    private final Queue serviceRequestStatementCompleteQueue;

    @Autowired
    public MQProducers(ConnectionFactory connectionFactory, MQConsumerUtils mqConsumerUtils) {
        this.connectionFactory = connectionFactory;

        accountBillingCycleChargeQueue = ActiveMQQueue.createQueue(mqConsumerUtils.getAccountBillingCycleChargeQueue());
        accountQueryStatementHeaderQueue = ActiveMQQueue.createQueue(mqConsumerUtils.getAccountQueryStatementHeaderQueue());
        servicerequestQueue = ActiveMQQueue.createQueue(mqConsumerUtils.getServicerequestQueue());
        accountLoanClosedQueue = ActiveMQQueue.createQueue(mqConsumerUtils.getAccountLoanClosedQueue());
        serviceRequestStatementCompleteQueue = ActiveMQQueue.createQueue(mqConsumerUtils.getServiceRequestStatementCompleteQueue());

        replyWaitingHandler = new ReplyWaitingHandler();
        JMSContext consumerContext = connectionFactory.createContext();
        statementReplyQueue = consumerContext.createTemporaryQueue();
        consumerContext.createConsumer(statementReplyQueue).setMessageListener(replyWaitingHandler::onMessage);
    }

    public Object accountBillingCycleCharge(BillingCycleCharge billingCycleCharge) throws InterruptedException, JMSException {
        log.debug("accountBillingCycleCharge: {}", billingCycleCharge);
        String responseKey = UUID.randomUUID().toString();
        replyWaitingHandler.put(responseKey);
        try ( JMSContext jmsContext = connectionFactory.createContext()) {
            Message message = jmsContext.createObjectMessage(billingCycleCharge);
            message.setJMSCorrelationID(responseKey);
            message.setJMSReplyTo(statementReplyQueue);
            jmsContext.createProducer().setDeliveryMode(DeliveryMode.NON_PERSISTENT).send(accountBillingCycleChargeQueue, message);
            return replyWaitingHandler.getReply(responseKey);
        }
    }

    public Object accountQueryStatementHeader(StatementHeader statementHeader) throws InterruptedException, JMSException {
        log.debug("accountQueryStatementHeader: {}", statementHeader);
        String responseKey = UUID.randomUUID().toString();
        replyWaitingHandler.put(responseKey);
        try ( JMSContext jmsContext = connectionFactory.createContext()) {
            Message message = jmsContext.createObjectMessage(statementHeader);
            message.setJMSCorrelationID(responseKey);
            message.setJMSReplyTo(statementReplyQueue);
            jmsContext.createProducer().setDeliveryMode(DeliveryMode.NON_PERSISTENT).send(accountQueryStatementHeaderQueue, message);
            return replyWaitingHandler.getReply(responseKey);
        }
    }

    public void serviceRequestServiceRequest(ServiceRequestResponse serviceRequest) {
        log.debug("serviceRequestServiceRequest: {}", serviceRequest);
        try ( JMSContext jmsContext = connectionFactory.createContext()) {
            jmsContext.createProducer().setDeliveryMode(DeliveryMode.NON_PERSISTENT).send(servicerequestQueue, jmsContext.createObjectMessage(serviceRequest));
        }
    }

    public void accountLoanClosed(StatementHeader statementHeader) throws JMSException {
        log.debug("accountLoanClosed: {}", statementHeader);
        try ( JMSContext jmsContext = connectionFactory.createContext()) {
            jmsContext.createProducer().setDeliveryMode(DeliveryMode.NON_PERSISTENT).send(accountLoanClosedQueue, jmsContext.createObjectMessage(statementHeader));
        }
    }

    public void serviceRequestStatementComplete(StatementCompleteResponse requestResponse) throws JMSException {
        log.debug("serviceRequestStatementComplete: {}", requestResponse);
        try ( JMSContext jmsContext = connectionFactory.createContext()) {
            jmsContext.createProducer().setDeliveryMode(DeliveryMode.NON_PERSISTENT).send(serviceRequestStatementCompleteQueue, jmsContext.createObjectMessage(requestResponse));
        }
    }

}
