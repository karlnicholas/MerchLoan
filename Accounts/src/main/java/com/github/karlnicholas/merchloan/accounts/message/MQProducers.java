package com.github.karlnicholas.merchloan.accounts.message;

import com.github.karlnicholas.merchloan.jms.MQConsumerUtils;
import com.github.karlnicholas.merchloan.jmsmessage.ServiceRequestResponse;
import com.github.karlnicholas.merchloan.jmsmessage.StatementHeader;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.*;

import java.io.IOException;
import java.util.UUID;

@ApplicationScoped
@Slf4j
public class MQProducers {
    private final MQConsumerUtils mqConsumerUtils;
    private final ClientSessionFactory producerFactory;
    private final ClientSession clientSession;
    private final ClientProducer statementQueryMostRecentStatementProducer;
    private final SimpleString mostRecentStatementReplyQueueName;
    private final ClientConsumer mostRecentStatementReplyConsumer;
    private final ClientProducer statementCloseStatementProducer;
    private final ClientProducer servicerequestProducer;

    @Inject
    public MQProducers(ServerLocator locator, MQConsumerUtils mqConsumerUtils) throws Exception {
        this.mqConsumerUtils = mqConsumerUtils;
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

        clientSession.start();
    }

    @PreDestroy
    public void preDestroy() throws ActiveMQException {
        log.info("producers preDestroy");
        clientSession.close();
        producerFactory.close();
    }

    public void serviceRequestServiceRequest(ServiceRequestResponse serviceRequest) throws ActiveMQException {
        try {
            log.debug("serviceRequestServiceRequest: {}", serviceRequest.getId());
            ClientMessage message = clientSession.createMessage(false);
            mqConsumerUtils.serializeToMessage(message, serviceRequest);
            servicerequestProducer.send(message);
        } catch (IOException e) {
            log.error("MQProducers ", e);
        }
    }

    public void statementCloseStatement(StatementHeader statementHeader) throws ActiveMQException {
        try {
            log.debug("statementCloseStatement: loanId: {}", statementHeader.getLoanId());
            ClientMessage message = clientSession.createMessage(false);
            mqConsumerUtils.serializeToMessage(message, statementHeader);
            statementCloseStatementProducer.send(message);
        } catch (IOException e) {
            log.error("MQProducers ", e);
        }
    }

    public Object queryMostRecentStatement(UUID loanId) throws ActiveMQException {
        try {
            log.debug("queryMostRecentStatement: {}", loanId);
            ClientMessage message = clientSession.createMessage(false);
            message.setReplyTo(mostRecentStatementReplyQueueName);
            mqConsumerUtils.serializeToMessage(message, loanId);
            statementQueryMostRecentStatementProducer.send(message, ack -> {
                log.debug("ACK {}", ack);
            });
            return mqConsumerUtils.deserialize(mostRecentStatementReplyConsumer.receive());
        } catch (IOException | ClassNotFoundException e) {
            log.error("MQProducers ", e);
        }
        return null;
    }

}
