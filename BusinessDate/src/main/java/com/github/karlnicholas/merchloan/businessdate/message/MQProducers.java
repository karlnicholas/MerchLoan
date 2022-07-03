package com.github.karlnicholas.merchloan.businessdate.message;

import com.github.karlnicholas.merchloan.jms.MQConsumerUtils;
import com.github.karlnicholas.merchloan.jms.ReplyWaitingHandler;
import com.github.karlnicholas.merchloan.jmsmessage.BillingCycle;
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
import java.time.LocalDate;
import java.util.UUID;

@Service
@Slf4j
public class MQProducers {
    private final ClientSession clientSession;
    private final MQConsumerUtils mqConsumerUtils;
    private final ClientProducer serviceRequestBillLoanProducer;
    private final ClientProducer businessDateSendProducer;
    private final ReplyWaitingHandler replyWaitingHandler;
    private final String businessDateReplyQueue;
    @Autowired
    public MQProducers(ClientSession clientSession, MQConsumerUtils mqConsumerUtils) throws ActiveMQException {
        this.clientSession = clientSession;
        this.mqConsumerUtils = mqConsumerUtils;
        replyWaitingHandler = new ReplyWaitingHandler();
        serviceRequestBillLoanProducer = clientSession.createProducer(mqConsumerUtils.getServiceRequestBillLoanQueue());
        businessDateReplyQueue = "businessdate-reply-"+UUID.randomUUID();
        businessDateSendProducer = clientSession.createProducer();
        mqConsumerUtils.bindConsumer(clientSession, businessDateReplyQueue, true, replyWaitingHandler::handleReplies);
    }

    public Object servicerequestCheckRequest() throws InterruptedException, ActiveMQException {
        log.debug("servicerequestCheckRequest:");
        String responseKey = UUID.randomUUID().toString();
        replyWaitingHandler.put(responseKey);
        ClientMessage message = clientSession.createMessage(false);
        message.setCorrelationID(responseKey);
        message.setReplyTo(SimpleString.toSimpleString(businessDateReplyQueue));
        message.getBodyBuffer().writeBytes(SerializationUtils.serialize(new byte[0]));
        businessDateSendProducer.send(mqConsumerUtils.getServiceRequestCheckRequestQueue(), message);
        return replyWaitingHandler.getReply(responseKey);
    }

    public Object acccountQueryLoansToCycle(LocalDate businessDate) throws InterruptedException, ActiveMQException {
        log.debug("acccountQueryLoansToCycle: {}", businessDate);
        String responseKey = UUID.randomUUID().toString();
        replyWaitingHandler.put(responseKey);
        ClientMessage message = clientSession.createMessage(false);
        message.setCorrelationID(responseKey);
        message.setReplyTo(SimpleString.toSimpleString(businessDateReplyQueue));
        message.getBodyBuffer().writeBytes(SerializationUtils.serialize(businessDate));
        businessDateSendProducer.send(mqConsumerUtils.getAccountQueryLoansToCycleQueue(), message);
        return replyWaitingHandler.getReply(responseKey);
    }

    public void serviceRequestBillLoan(BillingCycle billingCycle) throws ActiveMQException {
        ClientMessage message = clientSession.createMessage(false);
        message.getBodyBuffer().writeBytes(SerializationUtils.serialize(billingCycle));
        serviceRequestBillLoanProducer.send(message);
    }

}
