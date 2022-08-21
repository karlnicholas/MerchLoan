package com.github.karlnicholas.merchloan.businessdate.message;

import com.github.karlnicholas.merchloan.jms.MQConsumerUtils;
import com.github.karlnicholas.merchloan.jms.ReplyWaitingHandler;
import com.github.karlnicholas.merchloan.jmsmessage.BillingCycle;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
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
    private final ClientSessionFactory producerFactory;
    private final ClientSession clientSession;
    private final ClientProducer serviceRequestBillLoanProducer;
    private final ClientProducer serviceRequestCheckRequestProducer;
    private final SimpleString checkRequestReplyQueueName;
    private final ClientConsumer checkRequestReplyConsumer;
    private final ReplyWaitingHandler checkRequestReplyHandler;
    private final ClientProducer accountQueryLoansToCycleProducer;
    private final SimpleString loansToCycleQueueName;
    private final ClientConsumer businessDateReply;
    private final ReplyWaitingHandler loansToCycleReplyHandler;
    @Autowired
    public MQProducers(ServerLocator locator, MQConsumerUtils mqConsumerUtils) throws Exception {
        producerFactory =  locator.createSessionFactory();
        clientSession = producerFactory.createSession();
        clientSession.addMetaData(ClientSession.JMS_SESSION_IDENTIFIER_PROPERTY, "jms-client-id");
        clientSession.addMetaData("jms-client-id", "businessdate-producers");
        serviceRequestBillLoanProducer = clientSession.createProducer(mqConsumerUtils.getServiceRequestBillLoanQueue());

        serviceRequestCheckRequestProducer = clientSession.createProducer(mqConsumerUtils.getServiceRequestCheckRequestQueue());
        checkRequestReplyQueueName = SimpleString.toSimpleString("checkRequestReply"+UUID.randomUUID());
        QueueConfiguration queueConfiguration = new QueueConfiguration(checkRequestReplyQueueName);
        queueConfiguration.setDurable(false);
        queueConfiguration.setAutoDelete(true);
        queueConfiguration.setTemporary(true);
        queueConfiguration.setRoutingType(RoutingType.ANYCAST);
        clientSession.createQueue(queueConfiguration);
        checkRequestReplyConsumer = clientSession.createConsumer(checkRequestReplyQueueName);
        checkRequestReplyHandler = new ReplyWaitingHandler();
        checkRequestReplyConsumer.setMessageHandler(message->{
            byte[] mo = new byte[message.getBodyBuffer().readableBytes()];
            message.getBodyBuffer().readBytes(mo);
            checkRequestReplyHandler.handleReply(message.getCorrelationID().toString(), SerializationUtils.deserialize(mo));
        });


        accountQueryLoansToCycleProducer = clientSession.createProducer(mqConsumerUtils.getAccountQueryLoansToCycleQueue());
        loansToCycleQueueName = SimpleString.toSimpleString("businessDateReply"+UUID.randomUUID());
        queueConfiguration = new QueueConfiguration(loansToCycleQueueName);
        queueConfiguration.setDurable(false);
        queueConfiguration.setAutoDelete(true);
        queueConfiguration.setTemporary(true);
        queueConfiguration.setRoutingType(RoutingType.ANYCAST);
        clientSession.createQueue(queueConfiguration);
        businessDateReply = clientSession.createConsumer(loansToCycleQueueName);
        loansToCycleReplyHandler = new ReplyWaitingHandler();
        businessDateReply.setMessageHandler(message -> {
            byte[] mo = new byte[message.getBodyBuffer().readableBytes()];
            message.getBodyBuffer().readBytes(mo);
            loansToCycleReplyHandler.handleReply(message.getCorrelationID().toString(), SerializationUtils.deserialize(mo));
        });

        clientSession.start();

    }
    @PreDestroy
    public void preDestroy() throws ActiveMQException {
        log.info("producers preDestroy");
        clientSession.close();
        producerFactory.close();
    }
    public Object servicerequestCheckRequest() throws ActiveMQException, InterruptedException {
        log.debug("servicerequestCheckRequest:");
        String responseKey = UUID.randomUUID().toString();
        checkRequestReplyHandler.put(responseKey, null);
        ClientMessage message = clientSession.createMessage(false);
        message.setCorrelationID(responseKey);
        message.setReplyTo(checkRequestReplyQueueName);
        message.getBodyBuffer().writeBytes(SerializationUtils.serialize(new byte[0]));
        serviceRequestCheckRequestProducer.send(message);
        return checkRequestReplyHandler.getReply(responseKey);
//        ClientMessage reply = checkRequestReplyConsumer.receive();
//        byte[] mo = new byte[reply.getBodyBuffer().readableBytes()];
//        reply.getBodyBuffer().readBytes(mo);
//        return SerializationUtils.deserialize(mo);
    }

    public Object acccountQueryLoansToCycle(LocalDate businessDate) throws ActiveMQException, InterruptedException {
        log.debug("acccountQueryLoansToCycle: {}", businessDate);
        String responseKey = UUID.randomUUID().toString();
        loansToCycleReplyHandler.put(responseKey, null);
        ClientMessage message = clientSession.createMessage(false);
        message.setCorrelationID(responseKey);
        message.setReplyTo(loansToCycleQueueName);
        message.getBodyBuffer().writeBytes(SerializationUtils.serialize(businessDate));
        accountQueryLoansToCycleProducer.send(message);
        return loansToCycleReplyHandler.getReply(responseKey);
//        ClientMessage reply = loansToCycleConsumer.receive();
//        byte[] mo = new byte[reply.getBodyBuffer().readableBytes()];
//        reply.getBodyBuffer().readBytes(mo);
//        return SerializationUtils.deserialize(mo);
    }

    public void serviceRequestBillLoan(BillingCycle billingCycle) throws ActiveMQException {
        log.debug("serviceRequestBillLoan {}", billingCycle.getLoanId());
        ClientMessage message = clientSession.createMessage(false);
        message.getBodyBuffer().writeBytes(SerializationUtils.serialize(billingCycle));
        serviceRequestBillLoanProducer.send(message);
    }

}
