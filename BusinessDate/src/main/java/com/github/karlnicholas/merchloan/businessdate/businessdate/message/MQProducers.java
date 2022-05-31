package com.github.karlnicholas.merchloan.businessdate.businessdate.message;

import com.github.karlnicholas.merchloan.jms.MQConsumerUtils;
import com.github.karlnicholas.merchloan.jms.ReplyWaitingHandler;
import com.github.karlnicholas.merchloan.jmsmessage.BillingCycle;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.artemis.jms.client.ActiveMQQueue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import jakarta.jms.*;

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
    public MQProducers(ConnectionFactory connectionFactory, MQConsumerUtils mqConsumerUtils) {
        this.connectionFactory = connectionFactory;
        serviceRequestCheckRequestQueue = ActiveMQQueue.createQueue(mqConsumerUtils.getServiceRequestCheckRequestQueue());
        accountQueryLoansToCycleQueue = ActiveMQQueue.createQueue(mqConsumerUtils.getAccountQueryLoansToCycleQueue());
        serviceRequestBillLoanQueue = ActiveMQQueue.createQueue(mqConsumerUtils.getServiceRequestBillLoanQueue());

        replyWaitingHandler = new ReplyWaitingHandler();
        JMSContext jmsContext = connectionFactory.createContext();
        businessDateReplyQueue = jmsContext.createTemporaryQueue();
        jmsContext.createConsumer(businessDateReplyQueue).setMessageListener(replyWaitingHandler::onMessage);
    }

    public Object servicerequestCheckRequest() throws InterruptedException, JMSException {
        String responseKey = UUID.randomUUID().toString();
        replyWaitingHandler.put(responseKey);
        try (JMSContext jmsContext = connectionFactory.createContext()) {
            Message message = jmsContext.createObjectMessage(new byte[0]);
            message.setJMSCorrelationID(responseKey);
            message.setJMSReplyTo(businessDateReplyQueue);
            jmsContext.createProducer().setDeliveryMode(DeliveryMode.NON_PERSISTENT).send(serviceRequestCheckRequestQueue, message);
            return replyWaitingHandler.getReply(responseKey);
        }
    }

    public Object acccountQueryLoansToCycle(LocalDate businessDate) throws InterruptedException, JMSException {
        String responseKey = UUID.randomUUID().toString();
        replyWaitingHandler.put(responseKey);
        try (JMSContext jmsContext = connectionFactory.createContext()) {
            Message message = jmsContext.createObjectMessage(businessDate);
            message.setJMSCorrelationID(responseKey);
            message.setJMSReplyTo(businessDateReplyQueue);
            jmsContext.createProducer().setDeliveryMode(DeliveryMode.NON_PERSISTENT).send(accountQueryLoansToCycleQueue, message);
            return replyWaitingHandler.getReply(responseKey);
        }
    }

    public void serviceRequestBillLoan(BillingCycle billingCycle) {
        try (JMSContext jmsContext = connectionFactory.createContext()) {
            jmsContext.createProducer().setDeliveryMode(DeliveryMode.NON_PERSISTENT).send(serviceRequestBillLoanQueue, jmsContext.createObjectMessage(billingCycle));
        }
    }

}
