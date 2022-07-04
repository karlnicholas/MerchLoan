package com.github.karlnicholas.merchloan.query.message;

import com.github.karlnicholas.merchloan.jms.MQConsumerUtils;
import com.github.karlnicholas.merchloan.jms.ReplyWaitingHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.*;
import org.springframework.stereotype.Service;
import org.springframework.util.SerializationUtils;

import javax.annotation.PreDestroy;
import java.util.UUID;

@Service
@Slf4j
public class MQProducers {
    private final ClientSession clientSession;
    private final ClientSession replySession;
    private final ClientProducer servicerequestQueryIdProducer;
    private final ClientProducer accountQueryAccountIdProducer;
    private final ClientProducer accountQueryLoanIdProducer;
    private final ClientProducer statementQueryStatementProducer;
    private final ClientProducer statementQueryStatementsProducer;
    private final ClientProducer serviceRequestCheckRequestProducer;
    private final ReplyWaitingHandler replyWaitingHandler;
    private final String queryReplyQueue;

    public MQProducers(ServerLocator locator, MQConsumerUtils mqConsumerUtils) throws Exception {
        ClientSessionFactory producerFactory =  locator.createSessionFactory();
        clientSession = producerFactory.createSession();
        clientSession.addMetaData(ClientSession.JMS_SESSION_IDENTIFIER_PROPERTY, "jms-client-id");
        clientSession.addMetaData("jms-client-id", "query-producers");
        servicerequestQueryIdProducer = clientSession.createProducer(mqConsumerUtils.getServicerequestQueryIdQueue());
        accountQueryAccountIdProducer = clientSession.createProducer(mqConsumerUtils.getAccountQueryAccountIdQueue());
        accountQueryLoanIdProducer = clientSession.createProducer(mqConsumerUtils.getAccountQueryLoanIdQueue());
        statementQueryStatementProducer = clientSession.createProducer(mqConsumerUtils.getStatementQueryStatementQueue());
        statementQueryStatementsProducer = clientSession.createProducer(mqConsumerUtils.getStatementQueryStatementsQueue());
        serviceRequestCheckRequestProducer = clientSession.createProducer(mqConsumerUtils.getServiceRequestCheckRequestQueue());
        clientSession.start();

        ClientSessionFactory replyFactory =  locator.createSessionFactory();
        replySession = replyFactory.createSession();
        replySession.addMetaData(ClientSession.JMS_SESSION_IDENTIFIER_PROPERTY, "jms-client-id");
        replySession.addMetaData("jms-client-id", "query-reply");
        replyWaitingHandler = new ReplyWaitingHandler();
        queryReplyQueue = "query-reply-"+UUID.randomUUID();
        mqConsumerUtils.bindConsumer(replySession, queryReplyQueue, true, replyWaitingHandler::handleReplies);
        replySession.start();
    }

    @PreDestroy
    public void preDestroy() throws ActiveMQException {
        log.info("producers preDestroy");
        clientSession.close();
        replySession.close();
    }
    public Object queryServiceRequest(UUID id) {
        log.debug("queryServiceRequest: {}", id);
        String responseKey = UUID.randomUUID().toString();
        replyWaitingHandler.put(responseKey);
        ClientMessage message = clientSession.createMessage(false);
        message.setCorrelationID(responseKey);
        message.setReplyTo(SimpleString.toSimpleString(queryReplyQueue));
        message.getBodyBuffer().writeBytes(SerializationUtils.serialize(id));
        try {
            servicerequestQueryIdProducer.send(message, null);
            return replyWaitingHandler.getReply(responseKey);
        } catch (InterruptedException | ActiveMQException e) {
            log.error("queryServiceRequest", e);
            Thread.currentThread().interrupt();
            return null;
        }
    }

    public Object queryAccount(UUID id) {
        log.debug("queryAccount: {}", id);
        String responseKey = UUID.randomUUID().toString();
        replyWaitingHandler.put(responseKey);
        ClientMessage message = clientSession.createMessage(false);
        message.setCorrelationID(responseKey);
        message.setReplyTo(SimpleString.toSimpleString(queryReplyQueue));
        message.getBodyBuffer().writeBytes(SerializationUtils.serialize(id));
        try {
            accountQueryAccountIdProducer.send(message, null);
            return replyWaitingHandler.getReply(responseKey);
        } catch (InterruptedException | ActiveMQException e) {
            log.error("queryAccount", e);
            Thread.currentThread().interrupt();
            return null;
        }
    }

    public Object queryLoan(UUID id) {
        log.debug("queryLoan: {}", id);
        String responseKey = UUID.randomUUID().toString();
        replyWaitingHandler.put(responseKey);
        ClientMessage message = clientSession.createMessage(false);
        message.setCorrelationID(responseKey);
        message.setReplyTo(SimpleString.toSimpleString(queryReplyQueue));
        message.getBodyBuffer().writeBytes(SerializationUtils.serialize(id));
        try {
            accountQueryLoanIdProducer.send(message, null);
            return replyWaitingHandler.getReply(responseKey);
        } catch (InterruptedException | ActiveMQException e) {
            log.error("queryLoan", e);
            Thread.currentThread().interrupt();
            return null;
        }
    }

    public Object queryStatement(UUID id) {
        log.debug("queryStatement: {}", id);
        String responseKey = UUID.randomUUID().toString();
        replyWaitingHandler.put(responseKey);
        ClientMessage message = clientSession.createMessage(false);
        message.setCorrelationID(responseKey);
        message.setReplyTo(SimpleString.toSimpleString(queryReplyQueue));
        message.getBodyBuffer().writeBytes(SerializationUtils.serialize(id));
        try {
            statementQueryStatementProducer.send(message, null);
            return replyWaitingHandler.getReply(responseKey);
        } catch (InterruptedException | ActiveMQException e) {
            log.error("queryStatement", e);
            Thread.currentThread().interrupt();
            return null;
        }
    }

    public Object queryStatements(UUID id) {
        log.debug("queryStatements: {}", id);
        String responseKey = UUID.randomUUID().toString();
        replyWaitingHandler.put(responseKey);
        ClientMessage message = clientSession.createMessage(false);
        message.setCorrelationID(responseKey);
        message.setReplyTo(SimpleString.toSimpleString(queryReplyQueue));
        message.getBodyBuffer().writeBytes(SerializationUtils.serialize(id));
        try {
            statementQueryStatementsProducer.send(message, null);
            return replyWaitingHandler.getReply(responseKey);
        } catch (InterruptedException | ActiveMQException e) {
            log.error("queryStatements", e);
            Thread.currentThread().interrupt();
            return null;
        }
    }

    public Object queryCheckRequest() {
        log.debug("queryCheckRequest:");
        String responseKey = UUID.randomUUID().toString();
        replyWaitingHandler.put(responseKey);
        ClientMessage message = clientSession.createMessage(false);
        message.setCorrelationID(responseKey);
        message.setReplyTo(SimpleString.toSimpleString(queryReplyQueue));
        message.getBodyBuffer().writeBytes(SerializationUtils.serialize(new byte[0]));
        try {
            serviceRequestCheckRequestProducer.send(message, null);
            return replyWaitingHandler.getReply(responseKey);
        } catch (InterruptedException | ActiveMQException e) {
            log.error("queryCheckRequest", e);
            Thread.currentThread().interrupt();
            return null;
        }
    }
}
