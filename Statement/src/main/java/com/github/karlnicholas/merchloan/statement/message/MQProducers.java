package com.github.karlnicholas.merchloan.statement.message;

import com.github.karlnicholas.merchloan.jms.MQConsumerUtils;
import com.github.karlnicholas.merchloan.jms.ReplyWaitingHandler;
import com.github.karlnicholas.merchloan.jmsmessage.BillingCycleCharge;
import com.github.karlnicholas.merchloan.jmsmessage.ServiceRequestResponse;
import com.github.karlnicholas.merchloan.jmsmessage.StatementCompleteResponse;
import com.github.karlnicholas.merchloan.jmsmessage.StatementHeader;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.artemis.api.core.ActiveMQException;
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
    private final ClientSession clientSession;
    private final ClientSession replySession;
    private final ClientProducer servicerequestProducer;
    private final ClientProducer accountLoanClosedProducer;
    private final ClientProducer accountBillingCycleChargeProducer;
    private final ClientProducer accountQueryStatementHeaderProducer;
    private final ClientProducer serviceRequestStatementCompleteProducer;
    private final ReplyWaitingHandler replyWaitingHandler;
    private final String statementReplyQueue;

    @Autowired
    public MQProducers(ServerLocator locator, MQConsumerUtils mqConsumerUtils) throws Exception {
        ClientSessionFactory producerFactory =  locator.createSessionFactory();
        clientSession = producerFactory.createSession();
        clientSession.addMetaData(ClientSession.JMS_SESSION_IDENTIFIER_PROPERTY, "jms-client-id");
        clientSession.addMetaData("jms-client-id", "statement-producers");
        servicerequestProducer = clientSession.createProducer(mqConsumerUtils.getServicerequestQueue());
        accountLoanClosedProducer = clientSession.createProducer(mqConsumerUtils.getAccountLoanClosedQueue());
        accountBillingCycleChargeProducer = clientSession.createProducer(mqConsumerUtils.getAccountBillingCycleChargeQueue());
        accountQueryStatementHeaderProducer = clientSession.createProducer(mqConsumerUtils.getAccountQueryStatementHeaderQueue());
        serviceRequestStatementCompleteProducer = clientSession.createProducer(mqConsumerUtils.getServiceRequestStatementCompleteQueue());
        clientSession.start();

        ClientSessionFactory replyFactory =  locator.createSessionFactory();
        replySession = replyFactory.createSession();
        replySession.addMetaData(ClientSession.JMS_SESSION_IDENTIFIER_PROPERTY, "jms-client-id");
        replySession.addMetaData("jms-client-id", "statement-reply");
        replyWaitingHandler = new ReplyWaitingHandler();
        statementReplyQueue = "statement-reply-"+UUID.randomUUID();
        mqConsumerUtils.bindConsumer(replySession, statementReplyQueue, true, replyWaitingHandler::handleReplies);
        replySession.start();
    }
    @PreDestroy
    public void preDestroy() throws ActiveMQException {
        log.info("producers preDestroy");
        clientSession.close();
        replySession.close();
    }
    public Object accountBillingCycleCharge(BillingCycleCharge billingCycleCharge) throws InterruptedException, ActiveMQException {
        log.debug("accountBillingCycleCharge: {}", billingCycleCharge);
        String responseKey = UUID.randomUUID().toString();
        replyWaitingHandler.put(responseKey);
        ClientMessage message = clientSession.createMessage(false);
        message.setCorrelationID(responseKey);
        message.setReplyTo(SimpleString.toSimpleString(statementReplyQueue));
        message.getBodyBuffer().writeBytes(SerializationUtils.serialize(billingCycleCharge));
        accountBillingCycleChargeProducer.send(message, null);
        return replyWaitingHandler.getReply(responseKey);
    }

    public Object accountQueryStatementHeader(StatementHeader statementHeader) throws InterruptedException, ActiveMQException {
        log.debug("accountQueryStatementHeader: {}", statementHeader);
        String responseKey = UUID.randomUUID().toString();
        replyWaitingHandler.put(responseKey);
        ClientMessage message = clientSession.createMessage(false);
        message.setCorrelationID(responseKey);
        message.setReplyTo(SimpleString.toSimpleString(statementReplyQueue));
        message.getBodyBuffer().writeBytes(SerializationUtils.serialize(statementHeader));
        accountQueryStatementHeaderProducer.send(message, null);
        return replyWaitingHandler.getReply(responseKey);
    }

    public void serviceRequestServiceRequest(ServiceRequestResponse serviceRequest) throws ActiveMQException {
        log.debug("serviceRequestServiceRequest: {}", serviceRequest);
        ClientMessage message = clientSession.createMessage(false);
        message.getBodyBuffer().writeBytes(SerializationUtils.serialize(serviceRequest));
        servicerequestProducer.send(message, null);
    }

    public void accountLoanClosed(StatementHeader statementHeader) throws ActiveMQException {
        log.debug("accountLoanClosed: {}", statementHeader);
        ClientMessage message = clientSession.createMessage(false);
        message.getBodyBuffer().writeBytes(SerializationUtils.serialize(statementHeader));
        accountLoanClosedProducer.send(message, null);
    }

    public void serviceRequestStatementComplete(StatementCompleteResponse requestResponse) throws ActiveMQException {
        log.debug("serviceRequestStatementComplete: {}", requestResponse);
        ClientMessage message = clientSession.createMessage(false);
        message.getBodyBuffer().writeBytes(SerializationUtils.serialize(requestResponse));
        serviceRequestStatementCompleteProducer.send(message, null);
    }

}
