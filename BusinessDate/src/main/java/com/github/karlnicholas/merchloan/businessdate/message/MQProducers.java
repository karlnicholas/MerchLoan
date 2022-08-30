package com.github.karlnicholas.merchloan.businessdate.message;

import com.github.karlnicholas.merchloan.jms.MQConsumerUtils;
import com.github.karlnicholas.merchloan.jms.ReplyWaitingHandler;
import com.github.karlnicholas.merchloan.jmsmessage.BillingCycle;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.SerializationUtils;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.time.LocalDate;
import java.util.UUID;

@Service
@Slf4j
public class MQProducers {
    private final Connection producerConnection;
    private final Channel serviceRequestBillLoanProducer;
    private final Channel serviceRequestCheckRequestProducer;
    private final String checkRequestReplyQueueName;
    private final Connection consumerConnection;
    private final MQConsumerUtils mqConsumerUtils;
    private final ReplyWaitingHandler checkRequestReplyHandler;
    private final Channel accountQueryLoansToCycleProducer;
    private final String loansToCycleQueueName;
    private final ReplyWaitingHandler loansToCycleReplyHandler;
    @Autowired
    public MQProducers(ConnectionFactory connectionFactory, MQConsumerUtils mqConsumerUtils) throws Exception {
        producerConnection = connectionFactory.newConnection();
        consumerConnection = connectionFactory.newConnection();
        this.mqConsumerUtils = mqConsumerUtils;
        serviceRequestBillLoanProducer = producerConnection.createChannel(); //.createProducer(mqConsumerUtils.getServiceRequestBillLoanQueue());
        serviceRequestCheckRequestProducer = producerConnection.createChannel();
        checkRequestReplyQueueName = "checkRequestReply"+UUID.randomUUID();
        checkRequestReplyHandler = new ReplyWaitingHandler();
        mqConsumerUtils.bindConsumer(consumerConnection.createChannel(), mqConsumerUtils.getExchange(), checkRequestReplyQueueName, true, (consumerTag, delivery)->{
            checkRequestReplyHandler.handleReply(delivery.getProperties().getCorrelationId(), SerializationUtils.deserialize(delivery.getBody()));
        });

        accountQueryLoansToCycleProducer = producerConnection.createChannel();
        loansToCycleQueueName = "businessDateReply"+UUID.randomUUID();
        loansToCycleReplyHandler = new ReplyWaitingHandler();
        mqConsumerUtils.bindConsumer(consumerConnection.createChannel(), mqConsumerUtils.getExchange(), loansToCycleQueueName, true, (consumerTag, delivery)->{
            loansToCycleReplyHandler.handleReply(delivery.getProperties().getCorrelationId(), SerializationUtils.deserialize(delivery.getBody()));
        });

    }
    @PreDestroy
    public void preDestroy() throws IOException {
        log.info("producers preDestroy");
        producerConnection.close();
        consumerConnection.close();
    }
    public Object servicerequestCheckRequest() throws InterruptedException, IOException {
        log.debug("servicerequestCheckRequest:");
        String responseKey = UUID.randomUUID().toString();
        checkRequestReplyHandler.put(responseKey, null);
        AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder().correlationId(responseKey).replyTo(checkRequestReplyQueueName).build();
        serviceRequestCheckRequestProducer.basicPublish(mqConsumerUtils.getExchange(), mqConsumerUtils.getServiceRequestCheckRequestQueue(), properties, SerializationUtils.serialize(new byte[0]));
        return checkRequestReplyHandler.getReply(responseKey);
    }

    public Object acccountQueryLoansToCycle(LocalDate businessDate) throws InterruptedException, IOException {
        log.debug("acccountQueryLoansToCycle: {}", businessDate);
        String responseKey = UUID.randomUUID().toString();
        loansToCycleReplyHandler.put(responseKey, null);
        AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder().correlationId(responseKey).replyTo(loansToCycleQueueName).build();
        accountQueryLoansToCycleProducer.basicPublish(mqConsumerUtils.getExchange(), mqConsumerUtils.getAccountQueryLoansToCycleQueue(), properties, SerializationUtils.serialize(businessDate));
        return loansToCycleReplyHandler.getReply(responseKey);
    }

    public void serviceRequestBillLoan(BillingCycle billingCycle) throws IOException {
        log.debug("serviceRequestBillLoan {}", billingCycle.getLoanId());
        serviceRequestBillLoanProducer.basicPublish(mqConsumerUtils.getExchange(), mqConsumerUtils.getServiceRequestBillLoanQueue(), new AMQP.BasicProperties.Builder().build(), SerializationUtils.serialize(billingCycle));
    }

}
