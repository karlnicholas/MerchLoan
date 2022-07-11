package com.github.karlnicholas.merchloan.businessdate.message;

import com.github.karlnicholas.merchloan.jms.MQConsumerUtils;
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
    private final ClientProducer accountQueryLoansToCycleProducer;
    private final SimpleString loansToCycleQueueName;
    private final ClientConsumer loansToCycleConsumer;
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

        accountQueryLoansToCycleProducer = clientSession.createProducer(mqConsumerUtils.getAccountQueryLoansToCycleQueue());
        loansToCycleQueueName = SimpleString.toSimpleString("loansToCycle"+UUID.randomUUID());
        queueConfiguration = new QueueConfiguration(loansToCycleQueueName);
        queueConfiguration.setDurable(false);
        queueConfiguration.setAutoDelete(true);
        queueConfiguration.setTemporary(true);
        queueConfiguration.setRoutingType(RoutingType.ANYCAST);
        clientSession.createQueue(queueConfiguration);
        loansToCycleConsumer = clientSession.createConsumer(loansToCycleQueueName);

        clientSession.start();

    }
    @PreDestroy
    public void preDestroy() throws ActiveMQException {
        log.info("producers preDestroy");
        clientSession.close();
        producerFactory.close();
    }
    public Object servicerequestCheckRequest() throws ActiveMQException {
        log.debug("servicerequestCheckRequest:");
        ClientMessage message = clientSession.createMessage(false);
        message.setReplyTo(checkRequestReplyQueueName);
        message.getBodyBuffer().writeBytes(SerializationUtils.serialize(new byte[0]));
        serviceRequestCheckRequestProducer.send(message);
        ClientMessage reply = checkRequestReplyConsumer.receive();
        byte[] mo = new byte[reply.getBodyBuffer().readableBytes()];
        reply.getBodyBuffer().readBytes(mo);
        return SerializationUtils.deserialize(mo);
    }

    public Object acccountQueryLoansToCycle(LocalDate businessDate) throws ActiveMQException {
        log.debug("acccountQueryLoansToCycle: {}", businessDate);
        ClientMessage message = clientSession.createMessage(false);
        message.setReplyTo(loansToCycleQueueName);
        message.getBodyBuffer().writeBytes(SerializationUtils.serialize(businessDate));
        accountQueryLoansToCycleProducer.send(message);
        ClientMessage reply = loansToCycleConsumer.receive();
        byte[] mo = new byte[reply.getBodyBuffer().readableBytes()];
        reply.getBodyBuffer().readBytes(mo);
        return SerializationUtils.deserialize(mo);
    }

    public void serviceRequestBillLoan(BillingCycle billingCycle) throws ActiveMQException {
        ClientMessage message = clientSession.createMessage(false);
        message.getBodyBuffer().writeBytes(SerializationUtils.serialize(billingCycle));
        serviceRequestBillLoanProducer.send(message);
    }

}
