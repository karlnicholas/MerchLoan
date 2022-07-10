package com.github.karlnicholas.merchloan.businessdate.message;

import com.github.karlnicholas.merchloan.jms.MQConsumerUtils;
import com.github.karlnicholas.merchloan.jms.ReplyWaitingHandler;
import com.github.karlnicholas.merchloan.jmsmessage.BillingCycle;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.SerializationUtils;

import javax.annotation.PreDestroy;
import java.time.LocalDate;
import java.util.UUID;

@Service
@Slf4j
public class MQProducers {
    private final ClientSession clientSession;
    private final ClientSession replySession;
    private final ClientProducer serviceRequestBillLoanProducer;
    private final ClientProducer serviceRequestCheckRequestProducer;
    private final ClientProducer accountQueryLoansToCycleProducer;
    private final ReplyWaitingHandler replyWaitingHandler;
    private final String businessDateReplyQueue;
    @Autowired
    public MQProducers(ServerLocator locator, MQConsumerUtils mqConsumerUtils) throws Exception {
        ClientSessionFactory producerFactory =  locator.createSessionFactory();
        clientSession = producerFactory.createSession();
        clientSession.addMetaData(ClientSession.JMS_SESSION_IDENTIFIER_PROPERTY, "jms-client-id");
        clientSession.addMetaData("jms-client-id", "businessdate-producers");
        serviceRequestBillLoanProducer = clientSession.createProducer(mqConsumerUtils.getServiceRequestBillLoanQueue());
        accountQueryLoansToCycleProducer = clientSession.createProducer(mqConsumerUtils.getAccountQueryLoansToCycleQueue());
        serviceRequestCheckRequestProducer = clientSession.createProducer(mqConsumerUtils.getServiceRequestCheckRequestQueue());
        clientSession.start();

        replyWaitingHandler = new ReplyWaitingHandler();
        ClientSessionFactory replyFactory =  locator.createSessionFactory();
        replySession = replyFactory.createSession();
        replySession.addMetaData(ClientSession.JMS_SESSION_IDENTIFIER_PROPERTY, "jms-client-id");
        replySession.addMetaData("jms-client-id", "businessdate-reply");
        businessDateReplyQueue = "businessdate-reply-"+UUID.randomUUID();
        mqConsumerUtils.bindConsumer(replySession, SimpleString.toSimpleString(businessDateReplyQueue), true, replyWaitingHandler::handleReplies);
        replySession.start();
    }
    @PreDestroy
    public void preDestroy() throws ActiveMQException {
        log.info("producers preDestroy");
        clientSession.close();
        replySession.close();
    }
    public Object servicerequestCheckRequest() throws InterruptedException, ActiveMQException {
        log.debug("servicerequestCheckRequest:");
        String responseKey = UUID.randomUUID().toString();
        replyWaitingHandler.put(responseKey);
        ClientMessage message = clientSession.createMessage(false);
        message.setCorrelationID(responseKey);
        message.setReplyTo(SimpleString.toSimpleString(businessDateReplyQueue));
        message.getBodyBuffer().writeBytes(SerializationUtils.serialize(new byte[0]));
        serviceRequestCheckRequestProducer.send(message, null);
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
        accountQueryLoansToCycleProducer.send(message, null);
        return replyWaitingHandler.getReply(responseKey);
    }

    public void serviceRequestBillLoan(BillingCycle billingCycle) throws ActiveMQException {
        ClientMessage message = clientSession.createMessage(false);
        message.getBodyBuffer().writeBytes(SerializationUtils.serialize(billingCycle));
        serviceRequestBillLoanProducer.send(message, null);
    }

}
