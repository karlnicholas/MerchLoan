package com.github.karlnicholas.merchloan.query.message;

import com.github.karlnicholas.merchloan.jms.MQConsumerUtils;
import com.github.karlnicholas.merchloan.jms.queue.QueueMessageHandlerProducer;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.*;
import org.springframework.util.SerializationUtils;

import java.util.Optional;
import java.util.UUID;

@Slf4j
public class QueryServiceRequestProducer implements QueueMessageHandlerProducer {
    private final SimpleString queue;
    private final ClientSessionFactory sessionFactory;
    private final ClientSession clientSession;
    private final SimpleString replyQueueName;
    private final ClientConsumer replyConsumer;

    public QueryServiceRequestProducer(ServerLocator locator, MQConsumerUtils mqConsumerUtils) throws Exception {
        this.queue = SimpleString.toSimpleString(mqConsumerUtils.getServicerequestQueryIdQueue());

        sessionFactory = locator.createSessionFactory();
        clientSession = sessionFactory.createSession();
        replyQueueName = SimpleString.toSimpleString("queryLoanReply" + UUID.randomUUID());
        QueueConfiguration queueConfiguration = new QueueConfiguration(replyQueueName);
        queueConfiguration.setDurable(false);
        queueConfiguration.setAutoDelete(true);
        queueConfiguration.setTemporary(true);
        queueConfiguration.setRoutingType(RoutingType.ANYCAST);
        clientSession.createQueue(queueConfiguration);
        replyConsumer = clientSession.createConsumer(replyQueueName);

        clientSession.start();
    }

    @Override
    public Object sendMessage(ClientSession clientSession, ClientProducer producer, Object data) throws ActiveMQException {
        UUID id = (UUID) data;
        log.debug("queryServiceRequest: {}", id);
        ClientMessage message = clientSession.createMessage(false);
        message.setReplyTo(replyQueueName);
        message.getBodyBuffer().writeBytes(SerializationUtils.serialize(id));
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
