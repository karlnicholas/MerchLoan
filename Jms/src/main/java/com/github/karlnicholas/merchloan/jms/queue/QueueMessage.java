package com.github.karlnicholas.merchloan.jms.queue;

import lombok.Data;
import org.apache.activemq.artemis.api.core.client.ClientMessage;

import java.util.Optional;

@Data
public class QueueMessage {
    private final ClientMessage message;
    private final QueueMessageHandlerProducer producer;

    public QueueMessage(QueueMessageHandlerProducer producer, ClientMessage message) {
        this.message = message;
        this.producer = producer;
    }
}
