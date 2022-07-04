package com.github.karlnicholas.merchloan.accounts.message;

import com.github.karlnicholas.merchloan.jms.MQConsumerUtils;
import com.github.karlnicholas.merchloan.jms.ReplyWaitingHandler;
import com.github.karlnicholas.merchloan.jmsmessage.ServiceRequestResponse;
import com.github.karlnicholas.merchloan.jmsmessage.StatementHeader;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.SerializationUtils;

import javax.annotation.PreDestroy;
import java.util.UUID;

@Service
@Slf4j
public class MQProducers {
    private final ClientSession clientSession;
    private final ClientSession replySession;
    private final ClientProducer statementQueryMostRecentStatementProducer ;
    private final ClientProducer statementCloseStatementProducer;
    private final ReplyWaitingHandler replyWaitingHandler;
    private final ClientProducer servicerequestProducer;
    private final String accountsReplyQueue;

    @Autowired
    public MQProducers(ServerLocator locator, MQConsumerUtils mqConsumerUtils) throws Exception {
        ClientSessionFactory producerFactory =  locator.createSessionFactory();
        clientSession = producerFactory.createSession();
        clientSession.addMetaData(ClientSession.JMS_SESSION_IDENTIFIER_PROPERTY, "jms-client-id");
        clientSession.addMetaData("jms-client-id", "accounts-producers");
        servicerequestProducer = clientSession.createProducer(mqConsumerUtils.getServicerequestQueue());
        statementCloseStatementProducer = clientSession.createProducer(mqConsumerUtils.getStatementCloseStatementQueue());
        statementQueryMostRecentStatementProducer = clientSession.createProducer(mqConsumerUtils.getStatementQueryMostRecentStatementQueue());
        clientSession.start();

        replyWaitingHandler = new ReplyWaitingHandler();
        accountsReplyQueue = "accounts-reply-"+UUID.randomUUID();
        ClientSessionFactory replyFactory =  locator.createSessionFactory();
        replySession = replyFactory.createSession();
        replySession.addMetaData(ClientSession.JMS_SESSION_IDENTIFIER_PROPERTY, "jms-client-id");
        replySession.addMetaData("jms-client-id", "accounts-reply");
        mqConsumerUtils.bindConsumer(replySession, accountsReplyQueue, true, replyWaitingHandler::handleReplies);
        replySession.start();
    }
    @PreDestroy
    public void preDestroy() throws ActiveMQException {
        log.info("producers preDestroy");
        clientSession.close();
        replySession.close();
    }

    public void serviceRequestServiceRequest(ServiceRequestResponse serviceRequest) throws ActiveMQException {
        log.debug("serviceRequestServiceRequest: {}", serviceRequest);
        ClientMessage message = clientSession.createMessage(false);
        message.getBodyBuffer().writeBytes(SerializationUtils.serialize(serviceRequest));
        servicerequestProducer.send(message, null);
    }

    public void statementCloseStatement(StatementHeader statementHeader) throws ActiveMQException {
        log.debug("statementCloseStatement: {}", statementHeader);
        ClientMessage message = clientSession.createMessage(false);
        message.getBodyBuffer().writeBytes(SerializationUtils.serialize(statementHeader));
        statementCloseStatementProducer.send(message, null);
    }

    public Object queryMostRecentStatement(UUID loanId) throws InterruptedException, ActiveMQException {
        log.debug("queryMostRecentStatement: {}", loanId);
        String responseKey = UUID.randomUUID().toString();
        replyWaitingHandler.put(responseKey);
        ClientMessage message = clientSession.createMessage(false);
        message.setCorrelationID(responseKey);
        message.setReplyTo(SimpleString.toSimpleString(accountsReplyQueue));
        message.getBodyBuffer().writeBytes(SerializationUtils.serialize(loanId));
        statementQueryMostRecentStatementProducer.send(message, null);
        return replyWaitingHandler.getReply(responseKey);
    }

}
