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
public class QueryLoanProducer implements QueueMessageHandlerProducer {
    private final SimpleString queue;
    private final ReplyWaitingHandler replyWaitingHandler;
    private final SimpleString replyToQueue;

    public QueryLoanProducer(MQConsumerUtils mqConsumerUtils, ReplyWaitingHandler replyWaitingHandler, SimpleString replyToQueue) {
        this.queue = SimpleString.toSimpleString(mqConsumerUtils.getAccountQueryLoanIdQueue());
        this.replyWaitingHandler = replyWaitingHandler;
        this.replyToQueue = replyToQueue;
    }
    @Override
    public Object sendMessage(ClientSession clientSession, ClientProducer producer, Object data) throws ActiveMQException, InterruptedException {
        UUID id = (UUID) data;
        log.debug("queryLoan: {}", id);
        String responseKey = UUID.randomUUID().toString();
        replyWaitingHandler.put(responseKey, id);
        ClientMessage message = clientSession.createMessage(false);
        message.setCorrelationID(responseKey);
        message.setReplyTo(replyToQueue);
        message.getBodyBuffer().writeBytes(SerializationUtils.serialize(id));
        producer.send(queue, message);
        return replyWaitingHandler.getReply(responseKey);
//        ClientMessage reply = replyConsumer.receive();
//        byte[] mo = new byte[reply.getBodyBuffer().readableBytes()];
//        reply.getBodyBuffer().readBytes(mo);
//        return SerializationUtils.deserialize(mo);
    }

}
