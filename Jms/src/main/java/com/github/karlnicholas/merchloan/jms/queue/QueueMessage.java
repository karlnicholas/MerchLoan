package com.github.karlnicholas.merchloan.jms.queue;

import lombok.Data;

@Data
public class QueueMessage {
    private final Object message;
    private final QueueMessageHandlerProducer producer;

    public QueueMessage(Object message, QueueMessageHandlerProducer producer) {
        this.message = message;
        this.producer = producer;
    }
}
