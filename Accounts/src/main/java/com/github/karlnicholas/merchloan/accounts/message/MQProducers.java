package com.github.karlnicholas.merchloan.accounts.message;

import com.github.karlnicholas.merchloan.jms.MQConsumerUtils;
import com.github.karlnicholas.merchloan.jms.ReplyWaitingHandler;
import com.github.karlnicholas.merchloan.jmsmessage.ServiceRequestResponse;
import com.github.karlnicholas.merchloan.jmsmessage.StatementHeader;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.SerializationUtils;

import java.io.IOException;
import java.util.UUID;

@Service
@Slf4j
public class MQProducers {
    private final ClientSession clientSession;
    private final ClientProducer statementQueryMostRecentStatementProducer ;
    private final ClientProducer statementCloseStatementProducer;
    private final ReplyWaitingHandler replyWaitingHandler;
    private final ClientProducer servicerequestProducer;
    private final String accountsReplyQueue;

    @Autowired
    public MQProducers(ClientSession clientSession, MQConsumerUtils mqConsumerUtils) throws ActiveMQException {
        this.clientSession = clientSession;
        replyWaitingHandler = new ReplyWaitingHandler();
        servicerequestProducer = clientSession.createProducer(mqConsumerUtils.getServicerequestQueue());
        statementCloseStatementProducer = clientSession.createProducer(mqConsumerUtils.getStatementCloseStatementQueue());
        statementQueryMostRecentStatementProducer = clientSession.createProducer(mqConsumerUtils.getStatementQueryMostRecentStatementQueue());
        accountsReplyQueue = "accounts-reply-"+UUID.randomUUID();
        mqConsumerUtils.bindConsumer(clientSession, accountsReplyQueue, true, replyWaitingHandler::handleReplies);
    }

    public void serviceRequestServiceRequest(ServiceRequestResponse serviceRequest) throws ActiveMQException {
        log.debug("serviceRequestServiceRequest: {}", serviceRequest);
        ClientMessage message = clientSession.createMessage(false);
        message.getBodyBuffer().writeBytes(SerializationUtils.serialize(serviceRequest));
        servicerequestProducer.send(message);
    }

    public void statementCloseStatement(StatementHeader statementHeader) throws ActiveMQException {
        log.debug("statementCloseStatement: {}", statementHeader);
        ClientMessage message = clientSession.createMessage(false);
        message.getBodyBuffer().writeBytes(SerializationUtils.serialize(statementHeader));
        statementCloseStatementProducer.send(message);
    }

    public Object queryMostRecentStatement(UUID loanId) throws InterruptedException, ActiveMQException {
        log.debug("queryMostRecentStatement: {}", loanId);
        String responseKey = UUID.randomUUID().toString();
        replyWaitingHandler.put(responseKey);
        ClientMessage message = clientSession.createMessage(false);
        message.setCorrelationID(responseKey);
        message.setReplyTo(SimpleString.toSimpleString(accountsReplyQueue));
        message.getBodyBuffer().writeBytes(SerializationUtils.serialize(loanId));
        statementQueryMostRecentStatementProducer.send(message);
        return replyWaitingHandler.getReply(responseKey);
    }

}
