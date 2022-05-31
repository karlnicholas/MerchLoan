package com.github.karlnicholas.merchloan.businessdate.businessdate.message;

import com.github.karlnicholas.merchloan.jms.MQConsumerUtils;
import com.github.karlnicholas.merchloan.jms.ReplyWaitingHandler;
import com.github.karlnicholas.merchloan.jmsmessage.BillingCycle;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.command.ActiveMQQueue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.jms.*;
import java.time.LocalDate;
import java.util.UUID;

@Service
@Slf4j
public class MQProducers {
    private final ConnectionFactory connectionFactory;
    private final Queue serviceRequestCheckRequestQueue;
    private final Queue accountQueryLoansToCycleQueue;
    private final Queue serviceRequestBillLoanQueue;

    private final ReplyWaitingHandler replyWaitingHandler;
    private final Queue businessDateReplyQueue;

    @Autowired
    public MQProducers(ConnectionFactory connectionFactory, MQConsumerUtils mqConsumerUtils) throws JMSException {
        this.connectionFactory = connectionFactory;
        serviceRequestCheckRequestQueue = new ActiveMQQueue(mqConsumerUtils.getServiceRequestCheckRequestQueue());
        accountQueryLoansToCycleQueue = new ActiveMQQueue(mqConsumerUtils.getAccountQueryLoansToCycleQueue());
        serviceRequestBillLoanQueue = new ActiveMQQueue(mqConsumerUtils.getServiceRequestBillLoanQueue());

        replyWaitingHandler = new ReplyWaitingHandler();
        Session session = connectionFactory.createConnection().createSession(false, Session.AUTO_ACKNOWLEDGE);
        businessDateReplyQueue = session.createTemporaryQueue();
        session.createConsumer(businessDateReplyQueue).setMessageListener(replyWaitingHandler::onMessage);
    }

    public Object servicerequestCheckRequest() throws InterruptedException, JMSException {
        String responseKey = UUID.randomUUID().toString();
        replyWaitingHandler.put(responseKey);
        try (Session session = connectionFactory.createConnection().createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            Message message = session.createObjectMessage(new byte[0]);
            message.setJMSCorrelationID(responseKey);
            message.setJMSReplyTo(businessDateReplyQueue);
            session.createProducer().send(serviceRequestCheckRequestQueue, message);
            return replyWaitingHandler.getReply(responseKey);
        }
    }

    public Object acccountQueryLoansToCycle(LocalDate businessDate) throws InterruptedException, JMSException {
        String responseKey = UUID.randomUUID().toString();
        replyWaitingHandler.put(responseKey);
        try (Session session = connectionFactory.createConnection().createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            Message message = session.createObjectMessage(businessDate);
            message.setJMSCorrelationID(responseKey);
            message.setJMSReplyTo(businessDateReplyQueue);
            session.createProducer().send(accountQueryLoansToCycleQueue, message);
            return replyWaitingHandler.getReply(responseKey);
        }
    }

    public void serviceRequestBillLoan(BillingCycle billingCycle) {
        try (Session session = connectionFactory.createConnection().createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            session.createProducer().send(serviceRequestBillLoanQueue, session.createObjectMessage(billingCycle));
        }
    }

}
