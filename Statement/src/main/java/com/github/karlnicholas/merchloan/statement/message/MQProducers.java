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
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.SerializationUtils;

import java.util.UUID;

@Component
@Slf4j
public class MQProducers {
    private final ClientSession clientSession;
    private final MQConsumerUtils mqConsumerUtils;
    private final ClientProducer statementSendProducer;
    private final ClientProducer servicerequestProducer;
    private final ClientProducer accountLoanClosedProducer;
    private final ClientProducer serviceRequestStatementCompleteProducer;
    private final ReplyWaitingHandler replyWaitingHandler;
    private final String statementReplyQueue;

    @Autowired
    public MQProducers(ClientSession clientSession, MQConsumerUtils mqConsumerUtils) throws ActiveMQException {
        this.clientSession = clientSession;
        this.mqConsumerUtils = mqConsumerUtils;
        replyWaitingHandler = new ReplyWaitingHandler();
        servicerequestProducer = clientSession.createProducer(mqConsumerUtils.getServicerequestQueue());
        accountLoanClosedProducer = clientSession.createProducer(mqConsumerUtils.getAccountLoanClosedQueue());
        serviceRequestStatementCompleteProducer = clientSession.createProducer(mqConsumerUtils.getServiceRequestStatementCompleteQueue());
        statementReplyQueue = "statement-reply-"+UUID.randomUUID();
        statementSendProducer = clientSession.createProducer();
        mqConsumerUtils.bindConsumer(clientSession, statementReplyQueue, replyWaitingHandler::handleReplies);
    }

    public Object accountBillingCycleCharge(BillingCycleCharge billingCycleCharge) throws InterruptedException, ActiveMQException {
        log.debug("accountBillingCycleCharge: {}", billingCycleCharge);
        UUID responseKey = UUID.randomUUID();
        replyWaitingHandler.put(responseKey);
        ClientMessage message = clientSession.createMessage(false);
        message.setCorrelationID(responseKey);
        message.setReplyTo(SimpleString.toSimpleString(statementReplyQueue));
        message.getBodyBuffer().writeBytes(SerializationUtils.serialize(billingCycleCharge));
        statementSendProducer.send(message);
        return replyWaitingHandler.getReply(responseKey);
    }

    public Object accountQueryStatementHeader(StatementHeader statementHeader) throws InterruptedException, ActiveMQException {
        log.debug("accountQueryStatementHeader: {}", statementHeader);
        UUID responseKey = UUID.randomUUID();
        replyWaitingHandler.put(responseKey);
        ClientMessage message = clientSession.createMessage(false);
        message.setCorrelationID(responseKey);
        message.setReplyTo(SimpleString.toSimpleString(statementReplyQueue));
        message.getBodyBuffer().writeBytes(SerializationUtils.serialize(statementHeader));
        statementSendProducer.send(message);
        return replyWaitingHandler.getReply(responseKey);
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
