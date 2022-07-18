package com.github.karlnicholas.merchloan.query.message;

import com.github.karlnicholas.merchloan.jms.MQConsumerUtils;
import com.github.karlnicholas.merchloan.jms.queue.QueueMessageHandlerProducer;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.*;

import java.io.IOException;
import java.util.UUID;

@Slf4j
public class QueryCheckRequestProducer implements QueueMessageHandlerProducer {
    private final MQConsumerUtils mqConsumerUtils;
    private final SimpleString queue;
    private final ClientSessionFactory sessionFactory;
    private final ClientSession clientSession;
    private final SimpleString replyQueueName;
    private final ClientConsumer replyConsumer;

    public QueryCheckRequestProducer(ServerLocator locator, MQConsumerUtils mqConsumerUtils) throws Exception {
        this.mqConsumerUtils = mqConsumerUtils;
        queue = SimpleString.toSimpleString(mqConsumerUtils.getServiceRequestCheckRequestQueue());

        sessionFactory = locator.createSessionFactory();
        clientSession = sessionFactory.createSession();
        replyQueueName = SimpleString.toSimpleString("checkRequestReply" + UUID.randomUUID());
        replyConsumer = mqConsumerUtils.createTemporaryQueue(clientSession, replyQueueName);

        clientSession.start();
    }

    @Override
    public Object sendMessage(ClientSession clientSession, ClientProducer producer, Object data) throws ActiveMQException {
        try {
            log.debug("queryCheckRequest:");
            ClientMessage message = clientSession.createMessage(false);
            message.setReplyTo(replyQueueName);
            mqConsumerUtils.serializeToMessage(message, data);
            producer.send(queue, message);
            return mqConsumerUtils.deserialize(replyConsumer.receive());
        } catch (IOException | ClassNotFoundException e) {
            log.error("QueryCheckRequestProducer ", e);
        }
        return null;
    }

    @Override
    public void close() throws ActiveMQException {
        clientSession.close();
        sessionFactory.close();
    }
}
