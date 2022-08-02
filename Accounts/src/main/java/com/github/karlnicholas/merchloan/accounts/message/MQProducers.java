package com.github.karlnicholas.merchloan.accounts.message;

import com.github.karlnicholas.merchloan.jms.MQConsumerUtils;
import com.github.karlnicholas.merchloan.jms.ReplyWaitingHandler;
import com.github.karlnicholas.merchloan.jmsmessage.ServiceRequestResponse;
import com.github.karlnicholas.merchloan.jmsmessage.StatementHeader;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
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
    private final ClientSessionFactory producerFactory;
    private final ClientSession clientSession;
    private final ClientProducer statementQueryMostRecentStatementProducer;
    private final SimpleString mostRecentStatementReplyQueueName;
    private final ClientProducer statementCloseStatementProducer;
    private final ClientProducer servicerequestProducer;
    private final ReplyWaitingHandler replyWaitingHandlerMostRecentStatement;
    private final ClientConsumer mostRecentStatementReplyConsumer;


    @Autowired
    public MQProducers(ServerLocator locator, MQConsumerUtils mqConsumerUtils) throws Exception {
        producerFactory = locator.createSessionFactory();
        clientSession = producerFactory.createSession();
        clientSession.addMetaData(ClientSession.JMS_SESSION_IDENTIFIER_PROPERTY, "jms-client-id");
        clientSession.addMetaData("jms-client-id", "accounts-producers");
        servicerequestProducer = clientSession.createProducer(mqConsumerUtils.getServicerequestQueue());
        statementCloseStatementProducer = clientSession.createProducer(mqConsumerUtils.getStatementCloseStatementQueue());
        statementQueryMostRecentStatementProducer = clientSession.createProducer(mqConsumerUtils.getStatementQueryMostRecentStatementQueue());

        mostRecentStatementReplyQueueName = SimpleString.toSimpleString("mostRecentStatement" + UUID.randomUUID());
        QueueConfiguration queueConfiguration = new QueueConfiguration(mostRecentStatementReplyQueueName);
        queueConfiguration.setDurable(false);
        queueConfiguration.setAutoDelete(true);
        queueConfiguration.setTemporary(true);
        queueConfiguration.setRoutingType(RoutingType.ANYCAST);
        clientSession.createQueue(queueConfiguration);
        mostRecentStatementReplyConsumer = clientSession.createConsumer(mostRecentStatementReplyQueueName);

        replyWaitingHandlerMostRecentStatement = new ReplyWaitingHandler();
        mostRecentStatementReplyConsumer.setMessageHandler(message->{
            byte[] mo = new byte[message.getBodyBuffer().readableBytes()];
            message.getBodyBuffer().readBytes(mo);
            replyWaitingHandlerMostRecentStatement.handleReply((String)message.getCorrelationID(), SerializationUtils.deserialize(mo));
        });
        clientSession.start();
    }

    @PreDestroy
    public void preDestroy() throws ActiveMQException {
        log.info("producers preDestroy");
        clientSession.close();
        producerFactory.close();
    }

    public void serviceRequestServiceRequest(ServiceRequestResponse serviceRequest) throws ActiveMQException {
        log.debug("serviceRequestServiceRequest: {}", serviceRequest.getId());
        ClientMessage message = clientSession.createMessage(false);
        message.getBodyBuffer().writeBytes(SerializationUtils.serialize(serviceRequest));
        servicerequestProducer.send(message);
    }

    public void statementCloseStatement(StatementHeader statementHeader) throws ActiveMQException {
        log.debug("statementCloseStatement: loanId: {}", statementHeader.getLoanId());
        ClientMessage message = clientSession.createMessage(false);
        message.getBodyBuffer().writeBytes(SerializationUtils.serialize(statementHeader));
        statementCloseStatementProducer.send(message);
    }

    public Object queryMostRecentStatement(UUID loanId) throws ActiveMQException, InterruptedException {
        log.debug("queryMostRecentStatement: {}", loanId);
        String responseKey = UUID.randomUUID().toString();
        replyWaitingHandlerMostRecentStatement.put(responseKey);
        ClientMessage message = clientSession.createMessage(false);
        message.setReplyTo(mostRecentStatementReplyQueueName);
        message.setCorrelationID(responseKey);
        message.getBodyBuffer().writeBytes(SerializationUtils.serialize(loanId));
        statementQueryMostRecentStatementProducer.send(message, ack->{
            log.debug("ACK {}", ack);
        });
        return replyWaitingHandlerMostRecentStatement.getReply(responseKey);
//        ClientMessage reply = mostRecentStatementReplyConsumer.receive();
//        byte[] mo = new byte[reply.getBodyBuffer().readableBytes()];
//        reply.getBodyBuffer().readBytes(mo);
//        return SerializationUtils.deserialize(mo);
    }

}
