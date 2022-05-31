package com.github.karlnicholas.merchloan.accounts.message;

import com.github.karlnicholas.merchloan.jms.MQConsumerUtils;
import com.github.karlnicholas.merchloan.jms.ReplyWaitingHandler;
import com.github.karlnicholas.merchloan.jmsmessage.ServiceRequestResponse;
import com.github.karlnicholas.merchloan.jmsmessage.StatementHeader;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.artemis.jms.client.ActiveMQQueue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import jakarta.jms.*;

import java.util.UUID;

@Service
@Slf4j
public class MQProducers {
    private final ConnectionFactory connectionFactory;
    private final ReplyWaitingHandler replyWaitingHandler;
    private final Destination accountsReplyQueue;
    private final Destination servicerequestQueue;
    private final Destination statementCloseStatementQueue;
    private final Destination statementQueryMostRecentStatementQueue;

    @Autowired
    public MQProducers(ConnectionFactory connectionFactory, MQConsumerUtils mqConsumerUtils) throws JMSException {
        this.connectionFactory = connectionFactory;
        servicerequestQueue = ActiveMQQueue.createQueue(mqConsumerUtils.getServicerequestQueue());

        statementCloseStatementQueue = ActiveMQQueue.createQueue(mqConsumerUtils.getStatementCloseStatementQueue());

        statementQueryMostRecentStatementQueue = ActiveMQQueue.createQueue(mqConsumerUtils.getStatementQueryMostRecentStatementQueue());

        replyWaitingHandler = new ReplyWaitingHandler();
        JMSContext jmsContext = connectionFactory.createContext();
        accountsReplyQueue = jmsContext.createTemporaryQueue();
        jmsContext.createConsumer(accountsReplyQueue).setMessageListener(replyWaitingHandler::onMessage);

    }

    public void serviceRequestServiceRequest(ServiceRequestResponse serviceRequest) throws JMSException {
        log.debug("serviceRequestServiceRequest: {}", serviceRequest);
        try ( JMSContext jmsContext = connectionFactory.createContext()) {
            jmsContext.createProducer().setDeliveryMode(DeliveryMode.NON_PERSISTENT).send(servicerequestQueue, jmsContext.createObjectMessage(serviceRequest));
        }
    }

    public void statementCloseStatement(StatementHeader statementHeader) {
        log.debug("statementCloseStatement: {}", statementHeader);
        try ( JMSContext jmsContext = connectionFactory.createContext()) {
            jmsContext.createProducer().setDeliveryMode(DeliveryMode.NON_PERSISTENT).send(statementCloseStatementQueue, jmsContext.createObjectMessage(statementHeader));
        }
    }

    public Object queryMostRecentStatement(UUID loanId) throws InterruptedException, JMSException {
        log.debug("queryMostRecentStatement: {}", loanId);
        String responseKey = UUID.randomUUID().toString();
        replyWaitingHandler.put(responseKey);
        try ( JMSContext jmsContext = connectionFactory.createContext()) {
            Message message = jmsContext.createObjectMessage(loanId);
            message.setJMSCorrelationID(responseKey);
            message.setJMSReplyTo(accountsReplyQueue);
            jmsContext.createProducer().setDeliveryMode(DeliveryMode.NON_PERSISTENT).send(statementQueryMostRecentStatementQueue, message);
            return replyWaitingHandler.getReply(responseKey);
        }
    }
}
