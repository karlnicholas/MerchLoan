package com.github.karlnicholas.merchloan.query.message;

import com.github.karlnicholas.merchloan.jms.MQConsumerUtils;
import com.github.karlnicholas.merchloan.jms.ReplyWaitingHandler;
import com.github.karlnicholas.merchloan.jms.queue.QueueMessageHandlerProducer;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.springframework.util.SerializationUtils;

import java.util.UUID;

@Slf4j
public class QueryCheckRequestProducer implements QueueMessageHandlerProducer {
    private final SimpleString queue;
    private final SimpleString replyQueue;
    private final ReplyWaitingHandler replyWaitingHandler;

    public QueryCheckRequestProducer(MQConsumerUtils mqConsumerUtils, ReplyWaitingHandler replyWaitingHandler, SimpleString replyQueue) {
        this.replyWaitingHandler = replyWaitingHandler;
        this.replyQueue = replyQueue;
        this.queue = SimpleString.toSimpleString(mqConsumerUtils.getServiceRequestCheckRequestQueue());
    }

    @Override
    public Object sendMessage(ClientSession clientSession, ClientProducer producer, Object data) {
        log.debug("queryCheckRequest:");
        String responseKey = UUID.randomUUID().toString();
        replyWaitingHandler.put(responseKey);
        ClientMessage message = clientSession.createMessage(false);
        message.setCorrelationID(responseKey);
        message.setReplyTo(replyQueue);
        message.getBodyBuffer().writeBytes(SerializationUtils.serialize(new byte[0]));
        try {
            producer.send(queue, message, null);
            return replyWaitingHandler.getReply(responseKey);
        } catch (InterruptedException | ActiveMQException e) {
            log.error("queryCheckRequest", e);
            Thread.currentThread().interrupt();
            return null;
        }
    }
}
