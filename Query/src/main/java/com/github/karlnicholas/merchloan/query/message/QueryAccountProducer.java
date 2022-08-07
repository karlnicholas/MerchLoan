package com.github.karlnicholas.merchloan.query.message;

import com.github.karlnicholas.merchloan.jms.MQConsumerUtils;
import com.github.karlnicholas.merchloan.jms.ReplyWaitingHandler;
import com.github.karlnicholas.merchloan.jms.queue.QueueMessageHandlerProducer;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.*;
import org.springframework.util.SerializationUtils;

import java.util.UUID;

@Slf4j
public class QueryAccountProducer implements QueueMessageHandlerProducer {
    private final ClientSessionFactory sessionFactory;
    private final ClientSession clientSession;
    private final SimpleString queue;
    private final SimpleString replyQueueName;
    private final ClientConsumer replyConsumer;
    private final ReplyWaitingHandler replyWaitingHandler;

    public QueryAccountProducer(ServerLocator locator, MQConsumerUtils mqConsumerUtils) throws Exception {
        sessionFactory = locator.createSessionFactory();
        clientSession = sessionFactory.createSession();
        this.queue = SimpleString.toSimpleString(mqConsumerUtils.getAccountQueryAccountIdQueue());
        replyQueueName = SimpleString.toSimpleString("queryAccount" + UUID.randomUUID());
        replyConsumer = MQConsumerUtils.createTemporaryQueue(clientSession, replyQueueName);
        replyWaitingHandler = new ReplyWaitingHandler();
        replyConsumer.setMessageHandler(message->{
            byte[] mo = new byte[message.getBodyBuffer().readableBytes()];
            message.getBodyBuffer().readBytes(mo);
            replyWaitingHandler.handleReply(message.getCorrelationID().toString(), SerializationUtils.deserialize(mo));
        });

        clientSession.start();
    }

    @Override
    public Object sendMessage(ClientSession clientSession, ClientProducer producer, Object data) throws ActiveMQException, InterruptedException {
        UUID id = (UUID) data;
        log.debug("queryAccount: {}", id);
        String responseKey = UUID.randomUUID().toString();
        replyWaitingHandler.put(responseKey, id);
        ClientMessage message = clientSession.createMessage(false);
        message.setReplyTo(replyQueueName);
        message.setCorrelationID(responseKey);
        message.getBodyBuffer().writeBytes(SerializationUtils.serialize(id));
        producer.send(queue, message);
        return replyWaitingHandler.getReply(responseKey);
//        ClientMessage reply = replyConsumer.receive();
//        byte[] mo = new byte[reply.getBodyBuffer().readableBytes()];
//        reply.getBodyBuffer().readBytes(mo);
//        return SerializationUtils.deserialize(mo);
    }

    @Override
    public void close() throws ActiveMQException {
        clientSession.close();
        sessionFactory.close();
    }
}
