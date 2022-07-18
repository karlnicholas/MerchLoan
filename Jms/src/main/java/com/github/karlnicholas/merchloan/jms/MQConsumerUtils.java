package com.github.karlnicholas.merchloan.jms;


import lombok.Data;
import org.apache.activemq.artemis.api.core.*;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.MessageHandler;

import java.io.*;

@Data
public class MQConsumerUtils {

    public ClientConsumer bindConsumer(ClientSession clientSession, SimpleString queueName, boolean temporary, MessageHandler messageHandler) throws ActiveMQException {
        ClientSession.QueueQuery query = clientSession.queueQuery(queueName);
        if (!query.isExists()) {
            QueueConfiguration queueConfiguration = new QueueConfiguration(queueName);
            queueConfiguration.setDurable(false);
            queueConfiguration.setAutoDelete(true);
            queueConfiguration.setTemporary(temporary);
            queueConfiguration.setRoutingType(RoutingType.ANYCAST);
            clientSession.createQueue(queueConfiguration);
        }
        ClientConsumer clientConsumer = clientSession.createConsumer(queueName);
        clientConsumer.setMessageHandler(messageHandler);
        return clientConsumer;
    }

    public ClientConsumer createTemporaryQueue(ClientSession clientSession, SimpleString replyQueueName) throws ActiveMQException {
        QueueConfiguration queueConfiguration = new QueueConfiguration(replyQueueName);
        queueConfiguration.setDurable(false);
        queueConfiguration.setAutoDelete(true);
        queueConfiguration.setTemporary(true);
        queueConfiguration.setRoutingType(RoutingType.ANYCAST);
        clientSession.createQueue(queueConfiguration);
        return clientSession.createConsumer(replyQueueName);
    }

    public void serializeToMessage(ClientMessage message, Object data) throws IOException {
        try ( ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
                oos.writeObject(data);
                message.getBodyBuffer().writeBytes(baos.toByteArray());
            }
        }
    }

    public Object deserialize(ClientMessage reply) throws IOException, ClassNotFoundException {
        try ( ByteArrayInputStream bais = new ByteArrayInputStream(reply.getDataBuffer().toByteBuffer().array())) {
            try ( ObjectInputStream ois = new ObjectInputStream(bais) ) {
                return ois.readObject();
            }
        }
    }
    private String exchange;

    private String accountCreateAccountQueue;
    private String accountFundingQueue;
    private String accountValidateCreditQueue;
    private String accountValidateDebitQueue;
    private String accountCloseLoanQueue;
    private String accountLoanClosedQueue;
    private String accountQueryStatementHeaderQueue;
    private String accountQueryLoansToCycleQueue;
    private String accountQueryAccountIdQueue;
    private String accountQueryLoanIdQueue;
    private String accountBillingCycleChargeQueue;

    private String servicerequestQueue;
    private String servicerequestQueryIdQueue;
    private String serviceRequestCheckRequestQueue;
    private String serviceRequestBillLoanQueue;
    private String serviceRequestStatementCompleteQueue;

    private String statementStatementQueue;
    private String statementCloseStatementQueue;
    private String statementQueryStatementQueue;
    private String statementQueryStatementsQueue;
    private String statementQueryMostRecentStatementQueue;
}
