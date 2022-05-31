package com.github.karlnicholas.merchloan.query.message;

import com.github.karlnicholas.merchloan.jms.MQConsumerUtils;
import com.github.karlnicholas.merchloan.jms.ReplyWaitingHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.artemis.jms.client.ActiveMQDestination;
import org.apache.activemq.artemis.jms.client.ActiveMQQueue;
import org.springframework.stereotype.Service;

import jakarta.jms.*;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

@Service
@Slf4j
public class MQProducers {
    private final ConnectionFactory connectionFactory;
    private final ReplyWaitingHandler replyWaitingHandler;
    private final Queue queryReplyQueue;
    private final Queue servicerequestQueryIdQueue;
    private final Queue accountQueryAccountIdQueue;
    private final Queue accountQueryLoanIdQueue;
    private final Queue statementQueryStatementQueue;
    private final Queue statementQueryStatementsQueue;
    private final Queue serviceRequestCheckRequestQueue;

    public MQProducers(ConnectionFactory connectionFactory, MQConsumerUtils mqConsumerUtils) {
        this.connectionFactory = connectionFactory;
        servicerequestQueryIdQueue = new ActiveMQQueue(mqConsumerUtils.getServicerequestQueryIdQueue());
        accountQueryAccountIdQueue = new ActiveMQQueue(mqConsumerUtils.getAccountQueryAccountIdQueue());
        accountQueryLoanIdQueue = new ActiveMQQueue(mqConsumerUtils.getAccountQueryLoanIdQueue());
        statementQueryStatementQueue = new ActiveMQQueue(mqConsumerUtils.getStatementQueryStatementQueue());
        statementQueryStatementsQueue = new ActiveMQQueue(mqConsumerUtils.getStatementQueryStatementsQueue());
        serviceRequestCheckRequestQueue = new ActiveMQQueue(mqConsumerUtils.getServiceRequestCheckRequestQueue());

        replyWaitingHandler = new ReplyWaitingHandler();
        JMSContext queueReplyContext = connectionFactory.createConnection().createSession(false, Session.AUTO_ACKNOWLEDGE);
        queueReplysession.setClientID("Query::queueReplyContext");
        queryReplyQueue = queueReplysession.createTemporaryQueue();
        JMSConsumer replyConsumer = queueReplysession.createConsumer(queryReplyQueue);
        replyConsumer.setMessageListener(replyWaitingHandler);
    }

    public Object queryServiceRequest(UUID id) {
        Instant start = Instant.now();
        String responseKey = UUID.randomUUID().toString();
        replyWaitingHandler.put(responseKey);
        try (Session session = connectionFactory.createConnection().createSession(false, Session.AUTO_ACKNOWLEDGE) ) {
            Message message = session.createObjectMessage(id);
            message.setJMSCorrelationID(responseKey);
            message.setJMSReplyTo(queryReplyQueue);
            session.createProducer().send(servicerequestQueryIdQueue, message);
            Object r = replyWaitingHandler.getReply(responseKey);
            log.debug("queryServiceRequest {}", Duration.between(Instant.now(), start));
            return r;
        } catch (InterruptedException | JMSException e) {
            log.error("queryServiceRequest", e);
            Thread.currentThread().interrupt();
            return null;
        }
    }

    public Object queryAccount(UUID id) {
        Instant start = Instant.now();
        String responseKey = UUID.randomUUID().toString();
        replyWaitingHandler.put(responseKey);
        try (Session session = connectionFactory.createConnection().createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            Message message = session.createObjectMessage(id);
            message.setJMSCorrelationID(responseKey);
            message.setJMSReplyTo(queryReplyQueue);
            session.createProducer().send(accountQueryAccountIdQueue, message);
            Object r = replyWaitingHandler.getReply(responseKey);
            log.debug("queryAccount {}", Duration.between(Instant.now(), start));
            return r;
        } catch (JMSException | InterruptedException e) {
            log.error("queryAccount", e);
            Thread.currentThread().interrupt();
            return null;
        }
    }

    public Object queryLoan(UUID id) {
        Instant start = Instant.now();
        String responseKey = UUID.randomUUID().toString();
        replyWaitingHandler.put(responseKey);
        try (Session session = connectionFactory.createConnection().createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            Message message = session.createObjectMessage(id);
            message.setJMSCorrelationID(responseKey);
            message.setJMSReplyTo(queryReplyQueue);
            session.createProducer().send(accountQueryLoanIdQueue, message);
            Object r = replyWaitingHandler.getReply(responseKey);
            log.debug("queryLoan {}", Duration.between(Instant.now(), start));
            return r;
        } catch (JMSException | InterruptedException e) {
            log.error("queryLoan", e);
            Thread.currentThread().interrupt();
            return null;
        }
    }

    public Object queryStatement(UUID id) {
        Instant start = Instant.now();
        String responseKey = UUID.randomUUID().toString();
        replyWaitingHandler.put(responseKey);
        try (Session session = connectionFactory.createConnection().createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            Message message = session.createObjectMessage(id);
            message.setJMSCorrelationID(responseKey);
            message.setJMSReplyTo(queryReplyQueue);
            session.createProducer().send(statementQueryStatementQueue, message);
            Object r = replyWaitingHandler.getReply(responseKey);
            log.debug("queryStatement {}", Duration.between(Instant.now(), start));
            return r;
        } catch (JMSException | InterruptedException e) {
            log.error("queryStatement", e);
            Thread.currentThread().interrupt();
            return null;
        }
    }

    public Object queryStatements(UUID id) {
        Instant start = Instant.now();
        String responseKey = UUID.randomUUID().toString();
        replyWaitingHandler.put(responseKey);
        try (Session session = connectionFactory.createConnection().createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            Message message = session.createObjectMessage(id);
            message.setJMSCorrelationID(responseKey);
            message.setJMSReplyTo(queryReplyQueue);
            session.createProducer().send(statementQueryStatementsQueue, message);
            Object r = replyWaitingHandler.getReply(responseKey);
            log.debug("queryStatements {}", Duration.between(Instant.now(), start));
            return r;
        } catch (JMSException | InterruptedException e) {
            log.error("queryStatements", e);
            Thread.currentThread().interrupt();
            return null;
        }
    }

    public Object queryCheckRequest() {
        Instant start = Instant.now();
        String responseKey = UUID.randomUUID().toString();
        replyWaitingHandler.put(responseKey);
        try (Session session = connectionFactory.createConnection().createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            Message message = session.createObjectMessage(new byte[0]);
            message.setJMSCorrelationID(responseKey);
            message.setJMSReplyTo(queryReplyQueue);
            session.createProducer().send(serviceRequestCheckRequestQueue, message);
            Object r = replyWaitingHandler.getReply(responseKey);
            log.debug("queryCheckRequest {}", Duration.between(Instant.now(), start));
            return r;
        } catch (JMSException | InterruptedException e) {
            log.error("queryCheckRequest", e);
            Thread.currentThread().interrupt();
            return null;
        }
    }
}
