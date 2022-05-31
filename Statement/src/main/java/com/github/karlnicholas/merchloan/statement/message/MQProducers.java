package com.github.karlnicholas.merchloan.statement.message;

import com.github.karlnicholas.merchloan.jms.MQConsumerUtils;
import com.github.karlnicholas.merchloan.jms.ReplyWaitingHandler;
import com.github.karlnicholas.merchloan.jmsmessage.BillingCycleCharge;
import com.github.karlnicholas.merchloan.jmsmessage.ServiceRequestResponse;
import com.github.karlnicholas.merchloan.jmsmessage.StatementCompleteResponse;
import com.github.karlnicholas.merchloan.jmsmessage.StatementHeader;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.command.ActiveMQQueue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.jms.*;
import java.util.UUID;

@Component
@Slf4j
public class MQProducers {
    private final ConnectionFactory connectionFactory;
    private final ReplyWaitingHandler replyWaitingHandler;
    private final Queue statementReplyQueue;
    private final Queue accountBillingCycleChargeQueue;
    private final Queue accountQueryStatementHeaderQueue;
    private final Queue servicerequestQueue;
    private final Queue accountLoanClosedQueue;
    private final Queue serviceRequestStatementCompleteQueue;

    @Autowired
    public MQProducers(ConnectionFactory connectionFactory, MQConsumerUtils mqConsumerUtils) {
        this.connectionFactory = connectionFactory;

        accountBillingCycleChargeQueue = new ActiveMQQueue(mqConsumerUtils.getAccountBillingCycleChargeQueue());
        accountQueryStatementHeaderQueue = new ActiveMQQueue(mqConsumerUtils.getAccountQueryStatementHeaderQueue());
        servicerequestQueue = new ActiveMQQueue(mqConsumerUtils.getServicerequestQueue());
        accountLoanClosedQueue = new ActiveMQQueue(mqConsumerUtils.getAccountLoanClosedQueue());
        serviceRequestStatementCompleteQueue = new ActiveMQQueue(mqConsumerUtils.getServiceRequestStatementCompleteQueue());

        replyWaitingHandler = new ReplyWaitingHandler();
        Session session = connectionFactory.createConnection().createSession(false, Session.AUTO_ACKNOWLEDGE);
        statementReplyQueue = consumersession.createTemporaryQueue();
        consumersession.createConsumer(statementReplyQueue).setMessageListener(replyWaitingHandler::onMessage);
    }

    public Object accountBillingCycleCharge(BillingCycleCharge billingCycleCharge) throws InterruptedException, JMSException {
        log.debug("accountBillingCycleCharge: {}", billingCycleCharge);
        String responseKey = UUID.randomUUID().toString();
        replyWaitingHandler.put(responseKey);
        try ( Session session = connectionFactory.createConnection().createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            Message message = session.createObjectMessage(billingCycleCharge);
            message.setJMSCorrelationID(responseKey);
            message.setJMSReplyTo(statementReplyQueue);
            session.createProducer(accountBillingCycleChargeQueue).send(message);
            return replyWaitingHandler.getReply(responseKey);
        }
    }

    public Object accountQueryStatementHeader(StatementHeader statementHeader) throws InterruptedException, JMSException {
        log.debug("accountQueryStatementHeader: {}", statementHeader);
        String responseKey = UUID.randomUUID().toString();
        replyWaitingHandler.put(responseKey);
        try ( Session session = connectionFactory.createConnection().createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            Message message = session.createObjectMessage(statementHeader);
            message.setJMSCorrelationID(responseKey);
            message.setJMSReplyTo(statementReplyQueue);
            session.createProducer().send(accountQueryStatementHeaderQueue, message);
            return replyWaitingHandler.getReply(responseKey);
        }
    }

    public void serviceRequestServiceRequest(ServiceRequestResponse serviceRequest) {
        log.debug("serviceRequestServiceRequest: {}", serviceRequest);
        try ( Session session = connectionFactory.createConnection().createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            session.createProducer().send(servicerequestQueue, session.createObjectMessage(serviceRequest));
        }
    }

    public void accountLoanClosed(StatementHeader statementHeader) throws JMSException {
        log.debug("accountLoanClosed: {}", statementHeader);
        try ( Session session = connectionFactory.createConnection().createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            session.createProducer().send(accountLoanClosedQueue, session.createObjectMessage(statementHeader));
        }
    }

    public void serviceRequestStatementComplete(StatementCompleteResponse requestResponse) throws JMSException {
        log.debug("serviceRequestStatementComplete: {}", requestResponse);
        try ( Session session = connectionFactory.createConnection().createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            session.createProducer().send(serviceRequestStatementCompleteQueue, session.createObjectMessage(requestResponse));
        }
    }

}
