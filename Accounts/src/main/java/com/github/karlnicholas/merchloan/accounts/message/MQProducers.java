package com.github.karlnicholas.merchloan.accounts.message;

import com.github.karlnicholas.merchloan.jms.MQConsumerUtils;
import com.github.karlnicholas.merchloan.jms.ReplyWaitingHandler;
import com.github.karlnicholas.merchloan.jmsmessage.ServiceRequestResponse;
import com.github.karlnicholas.merchloan.jmsmessage.StatementHeader;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.artemis.jms.client.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQQueue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.jms.JmsProperties;
import org.springframework.stereotype.Service;

import jakarta.jms.*;

import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.Session;
import java.util.UUID;

@Service
@Slf4j
public class MQProducers {
    private final ConnectionFactory connectionFactory;
    private final ReplyWaitingHandler replyWaitingHandler;
    private final Queue accountsReplyQueue;
    private final Queue servicerequestQueue;
    private final Queue statementCloseStatementQueue;
    private final Queue statementQueryMostRecentStatementQueue;

    @Autowired
    public MQProducers(ConnectionFactory connectionFactory, MQConsumerUtils mqConsumerUtils) throws JMSException {
        this.connectionFactory = connectionFactory;
        servicerequestQueue = new ActiveMQQueue(mqConsumerUtils.getServicerequestQueue());

        statementCloseStatementQueue = new ActiveMQQueue(mqConsumerUtils.getStatementCloseStatementQueue());

        statementQueryMostRecentStatementQueue = new ActiveMQQueue(mqConsumerUtils.getStatementQueryMostRecentStatementQueue());

        replyWaitingHandler = new ReplyWaitingHandler();
        Session session = connectionFactory.createConnection().createSession(false, Session.AUTO_ACKNOWLEDGE);
        accountsReplyQueue = session.createTemporaryQueue();
        session.createConsumer(accountsReplyQueue).setMessageListener(replyWaitingHandler::onMessage);

    }

    public void serviceRequestServiceRequest(ServiceRequestResponse serviceRequest) throws JMSException {
        log.debug("serviceRequestServiceRequest: {}", serviceRequest);
        try ( Session session = connectionFactory.createConnection().createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            session.createProducer().send(servicerequestQueue, session.createObjectMessage(serviceRequest));
        }
    }

    public void statementCloseStatement(StatementHeader statementHeader) {
        log.debug("statementCloseStatement: {}", statementHeader);
        try ( Session session = connectionFactory.createConnection().createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            session.createProducer().send(statementCloseStatementQueue, session.createObjectMessage(statementHeader));
        }
    }

    public Object queryMostRecentStatement(UUID loanId) throws InterruptedException, JMSException {
        log.debug("queryMostRecentStatement: {}", loanId);
        String responseKey = UUID.randomUUID().toString();
        replyWaitingHandler.put(responseKey);
        try ( Session session = connectionFactory.createConnection().createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            Message message = session.createObjectMessage(loanId);
            message.setJMSCorrelationID(responseKey);
            message.setJMSReplyTo(accountsReplyQueue);
            session.createProducer().send(statementQueryMostRecentStatementQueue, message);
            return replyWaitingHandler.getReply(responseKey);
        }
    }
}
