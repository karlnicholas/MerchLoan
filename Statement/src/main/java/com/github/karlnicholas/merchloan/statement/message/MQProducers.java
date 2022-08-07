package com.github.karlnicholas.merchloan.statement.message;

import com.github.karlnicholas.merchloan.jms.MQConsumerUtils;
import com.github.karlnicholas.merchloan.jms.ReplyWaitingHandler;
import com.github.karlnicholas.merchloan.jmsmessage.BillingCycleCharge;
import com.github.karlnicholas.merchloan.jmsmessage.ServiceRequestResponse;
import com.github.karlnicholas.merchloan.jmsmessage.StatementCompleteResponse;
import com.github.karlnicholas.merchloan.jmsmessage.StatementHeader;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.SerializationUtils;

import javax.annotation.PreDestroy;
import java.util.UUID;

@Component
@Slf4j
public class MQProducers {
    private final ClientSessionFactory producerFactory;
    private final ClientSession clientSession;
    private final ClientProducer servicerequestProducer;
    private final ClientProducer accountLoanClosedProducer;
    private final ClientProducer accountBillingCycleChargeProducer;
    private final SimpleString billingCycleChargeQueueName;
    private final ClientConsumer billingCycleChargeConsumer;
    private final ReplyWaitingHandler billingCycleChargeReplyHandler;
    private final ClientProducer accountQueryStatementHeaderProducer;
    private final SimpleString queryStatementHeaderReplyQueueName;
    private final ClientConsumer queryStatementHeaderConsumer;
    private final ReplyWaitingHandler queryStatementHeaderReplyHandler;
    private final ClientProducer serviceRequestStatementCompleteProducer;

    @Autowired
    public MQProducers(ServerLocator locator, MQConsumerUtils mqConsumerUtils) throws Exception {
        producerFactory =  locator.createSessionFactory();
        clientSession = producerFactory.createSession();
        clientSession.addMetaData(ClientSession.JMS_SESSION_IDENTIFIER_PROPERTY, "jms-client-id");
        clientSession.addMetaData("jms-client-id", "statement-producers");
        serviceRequestStatementCompleteProducer = clientSession.createProducer(mqConsumerUtils.getServiceRequestStatementCompleteQueue());
        servicerequestProducer = clientSession.createProducer(mqConsumerUtils.getServicerequestQueue());
        accountLoanClosedProducer = clientSession.createProducer(mqConsumerUtils.getAccountLoanClosedQueue());

        accountBillingCycleChargeProducer = clientSession.createProducer(mqConsumerUtils.getAccountBillingCycleChargeQueue());
        billingCycleChargeQueueName = SimpleString.toSimpleString("billingCycleCharge" + UUID.randomUUID());
        QueueConfiguration queueConfiguration = new QueueConfiguration(billingCycleChargeQueueName);
        queueConfiguration.setDurable(false);
        queueConfiguration.setAutoDelete(true);
        queueConfiguration.setTemporary(true);
        queueConfiguration.setRoutingType(RoutingType.ANYCAST);
        clientSession.createQueue(queueConfiguration);
        billingCycleChargeConsumer = clientSession.createConsumer(billingCycleChargeQueueName);
        billingCycleChargeReplyHandler = new ReplyWaitingHandler();
        billingCycleChargeConsumer.setMessageHandler(message->{
            byte[] mo = new byte[message.getBodyBuffer().readableBytes()];
            message.getBodyBuffer().readBytes(mo);
            billingCycleChargeReplyHandler.handleReply(message.getCorrelationID().toString(), SerializationUtils.deserialize(mo));
        });


        accountQueryStatementHeaderProducer = clientSession.createProducer(mqConsumerUtils.getAccountQueryStatementHeaderQueue());
        queryStatementHeaderReplyQueueName = SimpleString.toSimpleString("queryStatementHeader" + UUID.randomUUID());
        queueConfiguration = new QueueConfiguration(queryStatementHeaderReplyQueueName);
        queueConfiguration.setDurable(false);
        queueConfiguration.setAutoDelete(true);
        queueConfiguration.setTemporary(true);
        queueConfiguration.setRoutingType(RoutingType.ANYCAST);
        clientSession.createQueue(queueConfiguration);
        queryStatementHeaderConsumer = clientSession.createConsumer(queryStatementHeaderReplyQueueName);
        queryStatementHeaderReplyHandler = new ReplyWaitingHandler();
        queryStatementHeaderConsumer.setMessageHandler(message->{
            byte[] mo = new byte[message.getBodyBuffer().readableBytes()];
            message.getBodyBuffer().readBytes(mo);
            queryStatementHeaderReplyHandler.handleReply(message.getCorrelationID().toString(), SerializationUtils.deserialize(mo));
        });

        clientSession.start();

    }
    @PreDestroy
    public void preDestroy() throws ActiveMQException {
        log.info("producers preDestroy");
        clientSession.close();
        producerFactory.close();
    }
    public Object accountBillingCycleCharge(BillingCycleCharge billingCycleCharge) throws InterruptedException, ActiveMQException {
        log.debug("accountBillingCycleCharge: {}", billingCycleCharge);
        String responseKey = UUID.randomUUID().toString();
        billingCycleChargeReplyHandler.put(responseKey, billingCycleCharge.getLoanId());
        ClientMessage message = clientSession.createMessage(false);
        message.setReplyTo(billingCycleChargeQueueName);
        message.setCorrelationID(responseKey);
        message.getBodyBuffer().writeBytes(SerializationUtils.serialize(billingCycleCharge));
        accountBillingCycleChargeProducer.send(message);

        return billingCycleChargeReplyHandler.getReply(responseKey);
    }

    public Object accountQueryStatementHeader(StatementHeader statementHeader) throws InterruptedException, ActiveMQException {
        log.debug("accountQueryStatementHeader: {}", statementHeader);
        String responseKey = UUID.randomUUID().toString();
        queryStatementHeaderReplyHandler.put(responseKey, statementHeader.getLoanId());
        ClientMessage message = clientSession.createMessage(false);
        message.setCorrelationID(responseKey);
        message.setReplyTo(queryStatementHeaderReplyQueueName);
        message.getBodyBuffer().writeBytes(SerializationUtils.serialize(statementHeader));
        accountQueryStatementHeaderProducer.send(message);
        return queryStatementHeaderReplyHandler.getReply(responseKey);
    }

    public void serviceRequestServiceRequest(ServiceRequestResponse serviceRequest) throws ActiveMQException {
        log.debug("serviceRequestServiceRequest: {}", serviceRequest);
        ClientMessage message = clientSession.createMessage(false);
        message.getBodyBuffer().writeBytes(SerializationUtils.serialize(serviceRequest));
        servicerequestProducer.send(message);
    }

    public void accountLoanClosed(StatementHeader statementHeader) throws ActiveMQException {
        log.debug("accountLoanClosed: {}", statementHeader);
        ClientMessage message = clientSession.createMessage(false);
        message.getBodyBuffer().writeBytes(SerializationUtils.serialize(statementHeader));
        accountLoanClosedProducer.send(message);
    }

    public void serviceRequestStatementComplete(StatementCompleteResponse requestResponse) throws ActiveMQException {
        log.debug("serviceRequestStatementComplete: {}", requestResponse);
        ClientMessage message = clientSession.createMessage(false);
        message.getBodyBuffer().writeBytes(SerializationUtils.serialize(requestResponse));
        serviceRequestStatementCompleteProducer.send(message);
    }

}
