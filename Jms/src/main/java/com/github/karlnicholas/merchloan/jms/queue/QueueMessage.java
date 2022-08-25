package com.github.karlnicholas.merchloan.jms.queue;

import org.apache.activemq.artemis.api.core.client.ClientMessage;

public class QueueMessage {
    private final ClientMessage message;
    private final QueueMessageHandlerProducer producer;

    public QueueMessage(QueueMessageHandlerProducer producer, ClientMessage message) {
        this.message = message;
        this.producer = producer;
    }

    public ClientMessage getMessage() {
        return message;
    }
    public QueueMessageHandlerProducer getProducer() {
        return producer;
    }
}
