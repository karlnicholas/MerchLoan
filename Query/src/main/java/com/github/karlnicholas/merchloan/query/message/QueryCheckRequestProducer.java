package com.github.karlnicholas.merchloan.query.message;

import com.github.karlnicholas.merchloan.jms.MQConsumerUtils;
import com.github.karlnicholas.merchloan.jms.queue.QueueMessageHandlerProducer;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.*;
import org.springframework.util.SerializationUtils;

import java.util.UUID;

@Slf4j
public class QueryCheckRequestProducer implements QueueMessageHandlerProducer {
    private final SimpleString queue;
    private final ClientSessionFactory sessionFactory;
    private final ClientSession clientSession;
    private final SimpleString replyQueueName;
    private final ClientConsumer replyConsumer;

    public QueryCheckRequestProducer(ServerLocator locator, MQConsumerUtils mqConsumerUtils) throws Exception {
        queue = SimpleString.toSimpleString(mqConsumerUtils.getServiceRequestCheckRequestQueue());

        sessionFactory = locator.createSessionFactory();
        clientSession = sessionFactory.createSession();
        replyQueueName = SimpleString.toSimpleString("checkRequestReply" + UUID.randomUUID());
        replyConsumer = MQConsumerUtils.createTemporaryQueue(clientSession, replyQueueName);

        clientSession.start();
    }

    @Override
    public Object sendMessage(ClientSession clientSession, ClientProducer producer, Object data) throws ActiveMQException {
        log.debug("queryCheckRequest:");
        ClientMessage message = clientSession.createMessage(false);
        message.setReplyTo(replyQueueName);
        message.getBodyBuffer().writeBytes(SerializationUtils.serialize(data));
        producer.send(queue, message);
        ClientMessage reply = replyConsumer.receive();
        byte[] mo = new byte[reply.getBodyBuffer().readableBytes()];
        reply.getBodyBuffer().readBytes(mo);
        return SerializationUtils.deserialize(mo);
    }
    @Override
    public void close() throws ActiveMQException {
        clientSession.close();
        sessionFactory.close();
    }
}
