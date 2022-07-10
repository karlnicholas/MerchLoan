package com.github.karlnicholas.merchloan.query.message;

import com.github.karlnicholas.merchloan.jms.MQConsumerUtils;
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

    public QueryCheckRequestProducer(MQConsumerUtils mqConsumerUtils, SimpleString replyQueue) {
        this.replyQueue = replyQueue;
        this.queue = SimpleString.toSimpleString(mqConsumerUtils.getServiceRequestCheckRequestQueue());
    }

    @Override
    public void sendMessage(ClientSession clientSession, ClientProducer producer, Object data, String responseKey) {
        log.debug("queryCheckRequest:");
        ClientMessage message = clientSession.createMessage(false);
        message.setCorrelationID(responseKey);
        message.setReplyTo(replyQueue);
        message.getBodyBuffer().writeBytes(SerializationUtils.serialize(new byte[0]));
        try {
            producer.send(queue, message, null);
        } catch (ActiveMQException e) {
            log.error("queryCheckRequest", e);
        }
    }
}
